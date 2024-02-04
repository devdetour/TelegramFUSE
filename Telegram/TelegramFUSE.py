import logging
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
from telethon import TelegramClient, events, sync, utils
from dotenv import load_dotenv
import os
from io import BytesIO
from cryptography.fernet import Fernet
from collections import defaultdict
from cachetools import LRUCache
import gc

load_dotenv()

FILE_MAX_SIZE_BYTES = int(2 * 1e9) # 2GB

# Real LRU cache implementation very cool
CACHE_MAXSIZE = 5e9 # 5GB
def getsizeofelt(val):
    try:
        s = len(val)
        return s
    except:
        return 1

def progress_cb(sent_bytes, total):
    percentTotal = int(sent_bytes/total * 100)
    if percentTotal % 5 == 0:
        print(f"Progress: {percentTotal}%...")

class TelegramFileClient():
    def __init__(self, session_name, api_id, api_hash, channel_link):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.client.start()
        self.channel_entity = self.client.get_entity(channel_link)
        # key to use for encryption, if not set, not encrypted.
        self.encryption_key = os.getenv("ENCRYPTION_KEY")
        self.cached_files = LRUCache(CACHE_MAXSIZE, getsizeof=getsizeofelt)
        self.fname_to_msgs = defaultdict(tuple)

        print("USING ENCRYPTION: ", self.encryption_key != None)

    def upload_file(self, bytesio, fh, file_name=None):
        # invalidate cache as soon as we upload file
        if fh in self.cached_files:
            self.cached_files.pop(fh)
            print("CLEANED UP ", gc.collect())

        # file_bytes is bytesio obj
        file_bytes = bytesio.read()

        if self.encryption_key != None:
            print("ENCRYPTING")
            f = Fernet(bytes(self.encryption_key, 'utf-8'))
            file_bytes = f.encrypt(file_bytes)

        chunks = []
        file_len = len(file_bytes)

        if isinstance(file_len, float):
            print("UH OH FILEBYTES LENGTH IS FLOAT", file_len)
        if isinstance(FILE_MAX_SIZE_BYTES, float):
            print("UH OH FILE_MAX_SIZE_BYTES IS FLOAT", FILE_MAX_SIZE_BYTES)

        if file_len > FILE_MAX_SIZE_BYTES:
            # Calculate the number of chunks needed
            num_chunks = (len(file_bytes) + FILE_MAX_SIZE_BYTES) // FILE_MAX_SIZE_BYTES
            
            if isinstance(num_chunks, float):
                print("UH OH num_chunks IS FLOAT", num_chunks)

            # Split the file into chunks
            for i in range(num_chunks):
                start = i * FILE_MAX_SIZE_BYTES
                end = (i + 1) * FILE_MAX_SIZE_BYTES
                chunk = file_bytes[start:end]
                chunks.append(chunk)
        else:
            # File is within the size limit, no need to split
            chunks = [file_bytes]

        upload_results = []

        fname=file_name

        i = 0
        for c in chunks:
            fname = f"{file_name}_part{i}.txt" # convert everything to text. tgram is weird about some formats
            f = self.client.upload_file(c, file_name=fname, part_size_kb=512, progress_callback=progress_cb)
            result = self.client.send_file(self.channel_entity, f)
            upload_results.append(result)
            i += 1

        self.fname_to_msgs[file_name] = tuple([m.id for m in upload_results])
        print(f"CACHED FILE! NEW SIZE: {self.cached_files.currsize}; maxsize: {self.cached_files.maxsize}")
        return upload_results

    def get_cached_file(self, fh):
        if fh in self.cached_files and self.cached_files[fh] != bytearray(b''):
            print("CACHE HIT")
            return self.cached_files[fh]
        return None

    # download entire file from telegram
    def download_file(self, fh, msgIds):
        if fh in self.cached_files and self.cached_files[fh] != bytearray(b''):
            print("CACHE HIT in download")
            return self.cached_files[fh]
        msgs = self.get_messages(msgIds)
        buf = BytesIO()
        for m in msgs:
            buf.write(self.download_message(m)) # error handling WHO??
        numBytes = buf.getbuffer().nbytes
        print(f"Downloaded file is size {numBytes}")
        buf.seek(0)
        readBytes = buf.read()

        if self.encryption_key != None:
            print("DECRYPTING")
            f = Fernet(bytes(self.encryption_key, 'utf-8'))
            readBytes = f.decrypt(readBytes)

        barr = bytearray(readBytes)
        # add to cache
        self.cached_files[fh] = barr
        return barr

    def get_messages(self, ids):
        result = self.client.get_messages(self.channel_entity, ids=ids)
        return result

    def download_message(self, msg):
        result = msg.download_media(bytes)
        return result
    
    def delete_messages(self, ids):
        return self.client.delete_messages(self.channel_entity, message_ids=ids)