import gzip
import bz2
import shutil
import os
from pathlib import Path
import boto3
import io

from BufferedOutputBase import BufferedOutputBase

s3 = boto3.resource('s3', region_name = 'ca-central-1')

def build_gz(local_file_path):
    with open(local_file_path, 'rb') as source:
        with gzip.open(local_file_path+ '.gz', 'wb') as target:
            shutil.copyfileobj(source, target)

def deflate_gz(local_file_path):
    with gzip.open(local_file_path + '.gz', 'rb') as source:
        with open(local_file_path, 'wb') as target:
            shutil.copyfileobj(source, target)

def rebuild_file(bz2_file_path ,file_path):
    with bz2.open(bz2_file_path, 'rb') as source:
        with open(file_path, 'wb') as target:
           shutil.copyfileobj(source, target)

def convert_gz_to_bz2(gz_file_path, bz2_file_path):
    with gzip.open(gz_file_path, 'rb') as source:
        with bz2.open(bz2_file_path, 'wb', compresslevel=9) as target:
           shutil.copyfileobj(source, target)



def s3_download():
    bucket = s3.Bucket('hbt2')
    obj = bucket.Object('t/3-1.txt')
    file_path = os.path.normcase(cwd + '/py/stream/resource/s3.txt')
    with open(file_path, 'wb') as data:
        obj.download_fileobj(data)

def s3_download_compress():
    
    bucket = s3.Bucket('hbt2')
    obj = bucket.Object('t/3-1.txt')
    file_path = os.path.normcase(cwd + '/py/stream/resource/s3.txt')

    with gzip.open(file_path + '.gz', 'wb') as data:
        obj.download_fileobj(data)
    
    deflate_gz(file_path)

def s3_download_compress_to_file_system():
    bucket = s3.Bucket('hbt2')
    obj = bucket.Object('t/3-1.txt')

    target = bucket.Object('t/s3.txt.gz')

    file_path = os.path.normcase(cwd + '/py/stream/resource/s3.txt')

    client = boto3.client('s3')
    src = client.get_object(Bucket='hbt2', Key='t/3-1.txt')
    # with gzip.open(file_path + '.gz', 'wb') as data:
    #     obj.download_fileobj(data)

    #txt = src['Body'].read()
    src_stream = src['Body']
    with gzip.open(file_path + '.gz', 'wb') as data:
        CHUNK_SIZE= 1*1024
        
        while True: 
            chunk = src_stream.read(CHUNK_SIZE) 
            if not chunk: 
                break 
            data.write(chunk)

    deflate_gz(file_path)
    #
        
def s3_download_compress_upload():
    
    client = boto3.client('s3')
    src = client.get_object(Bucket='hbt2', Key='t/3-1.txt')
   
    #txt = src['Body'].read()
    src_stream = src['Body']
    buff = io.BytesIO()
    
    buff = BufferedOutputBase('hbt2', 't/s3.txt.gz')
    with gzip.GzipFile(fileobj=buff, mode ='wb') as data:
        CHUNK_SIZE= 1*1024
        
        while True: 
            chunk = src_stream.read(CHUNK_SIZE) 
            if not chunk: 
                break 
            data.write(chunk)
            #data.flush()
            #buff.flush()        
            #client.put_object(Bucket='hbt2', Key ='t/s3.txt.gz', Body=data)

    buff.close()

if __name__ == '__main__':
    cwd = os.getcwd()
    gz_file_path = os.path.normcase(cwd + '/py/stream/resource/file.txt.gz')
    bz2_file_path = os.path.normcase(cwd + '/py/stream/resource/file.txt.bz2')

    local_file_path = os.path.normcase(cwd + '/py/stream/resource/file.txt')
    file_path = os.path.normcase(cwd + '/py/stream/resource/recovered.txt')

    # build_gz(local_file_path)
    # convert_gz_to_bz2 (gz_file_path, bz2_file_path)
    # rebuild_file(bz2_file_path,file_path)
    #s3_download_compress()
    s3_download_compress_upload()