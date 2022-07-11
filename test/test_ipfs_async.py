

import fsspec
from ipfsspec.asyn import AsyncIPFSFileSystem
from fsspec import register_implementation
from ipfsspec.utils import dict_equal, dict_hash
import asyncio
import io
import os

# register_implementation(IPFSFileSystem.protocol, IPFSFileSystem)
register_implementation(AsyncIPFSFileSystem.protocol, AsyncIPFSFileSystem)
DEFAULT_LPATH = 'test/data/input/yo.txt'
DEFAULT_LPATH_DIR = 'test/data/input'
DEFAULT_OUTPUT_DIR = 'test/data/output'
DEFAULT_RPATH = '/test'
# with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
#     print(f.read())
fs = fsspec.filesystem("ipfs")
fs_file = fsspec.filesystem('file')
# print(fs_file.cat(path='test/data/*', recursive=True))


def configure_rpath(lpath, rpath):
    if os.path.isfile(lpath):
        rpath = os.path.join(rpath,os.path.basename(lpath))
    else:
        assert os.path.isdir(lpath)
        rpath = os.path.join(rpath, lpath.split('/')[-1])

    return rpath

def test_put(lpath = DEFAULT_LPATH, rpath=DEFAULT_RPATH):
    local_dir_hash = fs_file.cat(path=lpath,  recursive=True)
    cid = fs.put(lpath=lpath, rpath=rpath)

    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    ipfs_dir_hash = fs.cat(path=rpath)
    assert local_dir_hash == ipfs_dir_hash, 'local and ipfs hash do not match'  
    assert fs.exists(path=rpath) 
    fs.rm(path=rpath)
    assert not fs.exists(path=rpath)

def test_info(lpath=DEFAULT_LPATH, rpath=DEFAULT_RPATH):
    cid = fs.put(lpath=lpath, rpath=rpath)
    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    assert fs.info(rpath)['type'] == 'file'
    assert fs.info(os.path.dirname(rpath))['type'] == 'directory'


def test_rm(lpath=DEFAULT_LPATH, rpath = DEFAULT_RPATH):
    cid = fs.put(lpath=lpath, rpath=rpath)
    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    assert fs.exists(path=rpath) 
    fs.rm(path=rpath)
    assert not fs.exists(path=rpath)

def test_get(lpath = DEFAULT_LPATH_DIR, rpath=DEFAULT_RPATH, output_dir = DEFAULT_OUTPUT_DIR):
    # local_dir_hash = fs_file.cat(path=lpath,  recursive=True)
    
    fs.rm(rpath)
    cid = fs.put(lpath=lpath, rpath=rpath, recursive=True)
    fs.get(rpath=rpath, lpath=output_dir, recursive=True)
    
    
    # print(fs_file.cat(path=output_dir, recursive=True))
    if fs_file.exists(output_dir):
        fs_file.rm(output_dir, recursive=True)
    
test_get()
test_info()
test_put()
test_rm()
