#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Based on the tmpfs.py example from pyfuse3: https://github.com/libfuse/pyfuse3/blob/master/examples/tmpfs.py

A mountable filesystem that stores data on Telegram. Maintains a sqlite db on-disk to keep track of stuff
like filename, which Telegram message IDs are associated with a file, etc.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import os
import sys

# If we are running from the pyfuse3 source directory, try
# to load the module from there first.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 'pyfuse3', '__init__.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

import pyfuse3
import errno
import stat
from time import time
import sqlite3
import logging
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio
from io import BytesIO
import gc
import atexit

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger()

class Operations(pyfuse3.Operations):
    enable_writeback_cache = True

    def __init__(self, client):
        super(Operations, self).__init__()
        self.db = sqlite3.connect('telegram.db')
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.inode_open_count = defaultdict(int)
        self.client = client
        # buffer data for writes. BYTEARRAY ONLY. I really wanted this to be a dictionary to support multiple files
        # at once, but I had crazy memory management issues with dictionaries (even cachetools, even though we clear entries)
        # so just a buffer.
        self.write_buffer = bytearray(b'')
        try:
            # Check if inodes table exists
            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("inodes",))
            table_exists = self.cursor.fetchone() is not None
            
            # Create if not
            if not table_exists:
                print("creating tables")
                self.init_tables()

        except Exception as e:
            print(f"Error checking if table exists: {e}")

    # debug. shows contents of tables
    def load_tables(self):
        self.cursor.execute("SELECT * FROM inodes;")
        rows = self.cursor.fetchall()
        print(f"GOT {len(rows)} inodes")
        print(" id   guid    gid mode    mtime_ns    atime_ns    ctime_ns    target  size    rdev    data")
        for row in rows:
            print("Row: ", "    ".join([str(r) for r in row]))

    def init_tables(self):
        '''Initialize file system tables'''

        self.cursor.execute("""
        CREATE TABLE inodes (
            id        INTEGER PRIMARY KEY,
            uid       INT NOT NULL,
            gid       INT NOT NULL,
            mode      INT NOT NULL,
            mtime_ns  INT NOT NULL,
            atime_ns  INT NOT NULL,
            ctime_ns  INT NOT NULL,
            target    BLOB(256) ,
            size      INT NOT NULL DEFAULT 0,
            rdev      INT NOT NULL DEFAULT 0,
            data      BLOB
        )
        """)

        # store telegram msgid associated with this inode
        self.cursor.execute("""
        CREATE TABLE telegram_messages (
            id INTEGER PRIMARY KEY,
            inode INT NOT NULL REFERENCES inodes(id)
        )
        """)

        self.cursor.execute("""
        CREATE TABLE contents (
            rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
            name      BLOB(256) NOT NULL,
            inode     INT NOT NULL REFERENCES inodes(id),
            parent_inode INT NOT NULL REFERENCES inodes(id),

            UNIQUE (name, parent_inode)
        )""")

        # Insert root directory
        now_ns = int(time() * 1e9)
        self.cursor.execute("INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (pyfuse3.ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR
                              | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH
                              | stat.S_IXOTH, os.getuid(), os.getgid(), now_ns, now_ns, now_ns))
        self.cursor.execute("INSERT INTO contents (name, parent_inode, inode) VALUES (?,?,?)",
                            (b'..', pyfuse3.ROOT_INODE, pyfuse3.ROOT_INODE))
        
        self.db.commit()


    def get_row(self, *a, **kw):
        self.cursor.execute(*a, **kw)
        try:
            row = next(self.cursor)
        except StopIteration:
            raise NoSuchRowError()
        try:
            next(self.cursor)
        except StopIteration:
            pass
        else:
            raise NoUniqueValueError()

        return row

    def get_rows(self, *a, **kw):
        self.cursor.execute(*a, **kw)
        rows = self.cursor.fetchall()

        if not rows:
            raise NoSuchRowError()

        return rows

    async def lookup(self, inode_p, name, ctx=None):
        if name == '.':
            inode = inode_p
        elif name == '..':
            inode = self.get_row("SELECT * FROM contents WHERE inode=?",
                                 (inode_p,))['parent_inode']
        else:
            try:
                inode = self.get_row("SELECT * FROM contents WHERE name=? AND parent_inode=?",
                                     (name, inode_p))['inode']
            except NoSuchRowError:
                raise(pyfuse3.FUSEError(errno.ENOENT))

        return await self.getattr(inode, ctx)


    async def getattr(self, inode, ctx=None):
        try:
            row = self.get_row("SELECT * FROM inodes WHERE id=?", (inode,))
        except NoSuchRowError:
            raise(pyfuse3.FUSEError(errno.ENOENT))

        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        entry.st_mode = row['mode']
        entry.st_nlink = self.get_row("SELECT COUNT(inode) FROM contents WHERE inode=?",
                                     (inode,))[0]
        entry.st_uid = row['uid']
        entry.st_gid = row['gid']
        entry.st_rdev = row['rdev']
        entry.st_size = row['size']

        entry.st_blksize = 512
        entry.st_blocks = 1
        entry.st_atime_ns = row['atime_ns']
        entry.st_mtime_ns = row['mtime_ns']
        entry.st_ctime_ns = row['ctime_ns']

        return entry

    async def readlink(self, inode, ctx):
        return self.get_row('SELECT * FROM inodes WHERE id=?', (inode,))['target']

    async def opendir(self, inode, ctx):
        return inode

    async def readdir(self, inode, off, token):
        if off == 0:
            off = -1

        cursor2 = self.db.cursor()
        cursor2.execute("SELECT * FROM contents WHERE parent_inode=? "
                        'AND rowid > ? ORDER BY rowid', (inode, off))

        for row in cursor2:
            pyfuse3.readdir_reply(
                token, row['name'], await self.getattr(row['inode']), row['rowid'])

    async def unlink(self, inode_p, name,ctx):
        entry = await self.lookup(inode_p, name)

        if stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.EISDIR)

        self._remove(inode_p, name, entry)

    async def rmdir(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)

        if not stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        self._remove(inode_p, name, entry)

    def _remove(self, inode_p, name, entry):
        if self.get_row("SELECT COUNT(inode) FROM contents WHERE parent_inode=?",
                        (entry.st_ino,))[0] > 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        if entry.st_nlink == 1 and entry.st_ino not in self.inode_open_count:
            # delete inode from db
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (entry.st_ino,))

            # delete message from Telegram
            self.delete_msgs_for_inode(entry.st_ino)

            # delete from contents
            self.cursor.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                            (name, inode_p))

            # Delete from telegram_messages
            self.cursor.execute("DELETE FROM telegram_messages where inode=?", (entry.st_ino,))

        if entry.st_nlink > 1 and entry.st_ino not in self.inode_open_count:
            # delete from contents
            self.cursor.execute("DELETE FROM contents WHERE name=? AND parent_inode=?",
                            (name, inode_p))

        self.db.commit()

    async def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return await self._create(inode_p, name, mode, ctx, target=target)

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new,
                     flags, ctx):
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        entry_old = await self.lookup(inode_p_old, name_old)

        try:
            entry_new = await self.lookup(inode_p_new, name_new)
        except pyfuse3.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            target_exists = False
        else:
            target_exists = True

        if target_exists:
            self._replace(inode_p_old, name_old, inode_p_new, name_new,
                          entry_old, entry_new)
        else:
            self.cursor.execute("UPDATE contents SET name=?, parent_inode=? WHERE name=? "
                                "AND parent_inode=?", (name_new, inode_p_new,
                                                       name_old, inode_p_old))
            self.db.commit()

    def delete_msgs_for_inode(self, fh):
        # delete from Telegram
        rows = self.cursor.execute("SELECT id FROM telegram_messages WHERE inode = ?", (fh,))
        ids = [r[0] for r in rows] 
        self.client.delete_messages(ids)

    def _replace(self, inode_p_old, name_old, inode_p_new, name_new,
                 entry_old, entry_new):

        if self.get_row("SELECT COUNT(inode) FROM contents WHERE parent_inode=?",
                        (entry_new.st_ino,))[0] > 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        self.cursor.execute("UPDATE contents SET inode=? WHERE name=? AND parent_inode=?",
                            (entry_old.st_ino, name_new, inode_p_new))
        self.db.execute('DELETE FROM contents WHERE name=? AND parent_inode=?',
                        (name_old, inode_p_old))

        if entry_new.st_nlink == 1 and entry_new.st_ino not in self.inode_open_count:
            self.cursor.execute("DELETE FROM inodes WHERE id=?", (entry_new.st_ino,))
            self.delete_msgs_for_inode(entry_new.st_ino)

        self.db.commit()


    async def link(self, inode, new_inode_p, new_name, ctx):
        entry_p = await self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            log.warning('Attempted to create entry %s with unlinked parent %d',
                        new_name, new_inode_p)
            raise FUSEError(errno.EINVAL)

        self.cursor.execute("INSERT INTO contents (name, inode, parent_inode) VALUES(?,?,?)",
                            (new_name, inode, new_inode_p))
        self.db.commit()
        return await self.getattr(inode)

    async def setattr(self, inode, attr, fields, fh, ctx):

        if fields.update_size:
            # get data from telegram
            data = await self.get_telegram_data(fh)
            if data is None:
                data = b''
            if len(data) < attr.st_size:
                data = data + b'\0' * (attr.st_size - len(data))
            else:
                data = data[:attr.st_size]
            self.cursor.execute('UPDATE inodes SET size=? WHERE id=?',
                                (attr.st_size, inode))
        if fields.update_mode:
            self.cursor.execute('UPDATE inodes SET mode=? WHERE id=?',
                                (attr.st_mode, inode))

        if fields.update_uid:
            self.cursor.execute('UPDATE inodes SET uid=? WHERE id=?',
                                (attr.st_uid, inode))

        if fields.update_gid:
            self.cursor.execute('UPDATE inodes SET gid=? WHERE id=?',
                                (attr.st_gid, inode))

        if fields.update_atime:
            self.cursor.execute('UPDATE inodes SET atime_ns=? WHERE id=?',
                                (attr.st_atime_ns, inode))

        if fields.update_mtime:
            self.cursor.execute('UPDATE inodes SET mtime_ns=? WHERE id=?',
                                (attr.st_mtime_ns, inode))

        if fields.update_ctime:
            self.cursor.execute('UPDATE inodes SET ctime_ns=? WHERE id=?',
                                (attr.st_ctime_ns, inode))
        else:
            self.cursor.execute('UPDATE inodes SET ctime_ns=? WHERE id=?',
                                (int(time()*1e9), inode))

        self.db.commit()
        return await self.getattr(inode)

    async def mknod(self, inode_p, name, mode, rdev, ctx):
        return await self._create(inode_p, name, mode, ctx, rdev=rdev)

    async def mkdir(self, inode_p, name, mode, ctx):
        return await self._create(inode_p, name, mode, ctx)

    async def statfs(self, ctx):
        stat_ = pyfuse3.StatvfsData()

        stat_.f_bsize = 512
        stat_.f_frsize = 512

        size = self.get_row('SELECT SUM(size) FROM inodes')[0]
        stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = max(size // stat_.f_frsize, 1024)
        stat_.f_bavail = stat_.f_bfree

        inodes = self.get_row('SELECT COUNT(id) FROM inodes')[0]
        stat_.f_files = inodes
        stat_.f_ffree = max(inodes , 100)
        stat_.f_favail = stat_.f_ffree

        return stat_

    async def open(self, inode, flags, ctx):
        # Yeah, unused arguments
        #pylint: disable=W0613
        self.inode_open_count[inode] += 1

        # Use inodes as a file handles
        return pyfuse3.FileInfo(fh=inode)

    async def access(self, inode, mode, ctx):
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613
        return True

    async def create(self, inode_parent, name, mode, flags, ctx):
        
        #pylint: disable=W0612
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        return (pyfuse3.FileInfo(fh=entry.st_ino), entry)

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if (await self.getattr(inode_p)).st_nlink == 0:
            log.warning('Attempted to create entry %s with unlinked parent %d',
                        name, inode_p)
            raise FUSEError(errno.EINVAL)

        now_ns = int(time() * 1e9)
        self.cursor.execute('INSERT INTO inodes (uid, gid, mode, mtime_ns, atime_ns, '
                            'ctime_ns, target, rdev) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
                            (ctx.uid, ctx.gid, mode, now_ns, now_ns, now_ns, target, rdev))

        inode = self.cursor.lastrowid
        self.db.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
                        (name, inode, inode_p))

        self.db.commit()
        return await self.getattr(inode)

    # helper function to get all data for a file from telegram
    async def get_telegram_data(self, fh):
        filebuf = self.client.get_cached_file(fh)
        if filebuf != None:
            return filebuf
        # CHECK if we have ANY messages for this inode
        try:
            # get telegram messages for inode
            msgIds = self.get_rows('SELECT * FROM telegram_messages WHERE inode=?', (fh,))
            ids = [r[0] for r in msgIds]

            # FOR EACH message, call telegram API and get contents
            filebuf = self.client.download_file(fh, ids)
            return filebuf
        # if no rows, return empty bytes immediately
        except Exception as e:
            print("EXCEPTION: ", e)
            return bytearray(b'')


    async def read(self, fh, offset, length):
        row = self.get_row('SELECT * FROM inodes WHERE id=?', (fh,))

        # if no data, don't query telegram
        if row is None:
            return b''

        telegram_data = await self.get_telegram_data(fh)
        return telegram_data[offset:offset+length]

    # buffer in memory first...
    async def write(self, fh, offset, buf):
        # get data if exists
        result_bytes = self.write_buffer

        # if we are not already writing, try to get data from telegram
        if result_bytes == bytearray(b''):
            row = self.get_row('SELECT * FROM inodes WHERE id=?', (fh,))
            if row != None:
                result_bytes = await self.get_telegram_data(fh)
                self.write_buffer = result_bytes
        if offset == len(result_bytes):
            self.write_buffer += buf
        else:
            self.write_buffer = result_bytes[:offset] + buf + result_bytes[offset+len(buf):] # this is kind of slow

        return len(buf)

    async def close(self, fh):
        pass

    async def fsync(self, fh):
        pass

    async def release(self, fh):
        self.inode_open_count[fh] -= 1
        # THIS is where we write data for real!
        # IF we have un-written data in the buffer for this fh, yeet it to discord/tgram.
        if len(self.write_buffer) > 0:
            data = self.write_buffer
            self.write_buffer = bytearray(b'')
            # self.write_buffer.pop(fh)
            print("CLEANED UP ", gc.collect())
            filename = ""

            fname = self.get_row("SELECT name FROM contents WHERE inode=?", (fh,))
            if fname != None:
                name = fname[0]
                filename = name.decode()
            # write data back to telegram
            fileBytes = BytesIO(data)
            telegram_msgs = self.client.upload_file(fileBytes, fh, filename)

            # clear existing from telegram
            self.delete_msgs_for_inode(fh)

            # clear any existing messages from DB
            self.cursor.execute("DELETE FROM telegram_messages WHERE inode = ?", (fh,))

            # add new message ids back to DB
            for msg in telegram_msgs:
                self.cursor.execute("INSERT INTO telegram_messages (id, inode) VALUES (?, ?)", (msg.id, fh,))

            # update inodes
            self.cursor.execute('UPDATE inodes SET size=? WHERE id=?',
                                (len(data), fh))
            self.db.commit()

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            if (await self.getattr(fh)).st_nlink == 0:
                self.cursor.execute("DELETE FROM inodes WHERE id=?", (fh,))
                self.db.commit()

class NoUniqueValueError(Exception):
    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    def __str__(self):
        return 'Query produced 0 result rows'

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system', default="./telegramfs")
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')

    return parser.parse_args()

def runFs(client):
    try:
        options = parse_args()
        init_logging(options.debug)
        operations = Operations(client)

        fuse_options = set(pyfuse3.default_options)
        fuse_options.add('fsname=telegram_fuse')
        fuse_options.add("allow_other")
        fuse_options.discard('default_permissions')
        if options.debug_fuse:
            fuse_options.add('debug')
        
        pyfuse3.init(operations, options.mountpoint, fuse_options)

        # close db and unmount fs when program is closed
        def cleanup():
            print("RUNNING CLEANUP")
            operations.cursor.close()
            operations.db.close()
            pyfuse3.close(unmount=True)

        atexit.register(cleanup)
        trio.run(pyfuse3.main)

    except:
        raise
    
    
