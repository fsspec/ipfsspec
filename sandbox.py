

import fsspec
from ipfsspec.asyn import AsyncIPFSFileSystem
from fsspec import register_implementation
from ipfsspec.utils import dict_equal, dict_hash
import asyncio
import io
import os

# register_implementation(IPFSFileSystem.protocol, IPFSFileSystem)
register_implementation(AsyncIPFSFileSystem.protocol, AsyncIPFSFileSystem)
DEFAULT_LPATH = 'test/data/yo.txt'
DEFAULT_LPATH_DIR = 'test/data/'
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

def test_get(lpath = 'test', rpath=DEFAULT_RPATH, local_dir = 'output'):
    # local_dir_hash = fs_file.cat(path=lpath,  recursive=True)
    
    fs.rm(rpath)
    fs_file.rm(local_dir, recursive=True)
    
    cid = fs.put(lpath=lpath, rpath=rpath, recursive=True)
    # get_rpath = configure_rpath(rpath=rpath, lpath=lpath)
    # print(fs.ls(recursive=True, detail=False), cid, 'BRUH')
    # links = fs.api_get(endpoint='dag/get', arg=cid)['links']
    
    fs.get(rpath=rpath, lpath=local_dir, recursive=True)

    # fs.get(rpath=get_rpath, lpath='output2', recursive=True)





test_get()

