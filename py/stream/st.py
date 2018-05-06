import gzip
import bz2
import shutil
import os
from pathlib import Path

def build_gz(local_file_path):
    with open(local_file_path, 'rb') as source:
        with gzip.open(local_file_path+ '.gz', 'wb') as target:
            shutil.copyfileobj(source, target)

def rebuild_file(bz2_file_path ,file_path):
    with bz2.open(bz2_file_path, 'rb') as source:
        with open(file_path, 'wb') as target:
           shutil.copyfileobj(source, target)

def convert_gz_to_bz2(gz_file_path, bz2_file_path):
    with gzip.open(gz_file_path, 'rb') as source:
        with bz2.open(bz2_file_path, 'wb', compresslevel=9) as target:
           shutil.copyfileobj(source, target)


if __name__ == '__main__':
    cwd = os.getcwd()
    gz_file_path = os.path.normcase(cwd + '/py/stream/resource/file.txt.gz')
    bz2_file_path = os.path.normcase(cwd + '/py/stream/resource/file.txt.bz2')

    local_file_path = os.path.normcase(cwd + '/py/stream/resource/file.txt')
    file_path = os.path.normcase(cwd + '/py/stream/resource/recovered.txt')

    # build_gz(local_file_path)

    convert_gz_to_bz2 (gz_file_path, bz2_file_path)
    rebuild_file(bz2_file_path,file_path)
    