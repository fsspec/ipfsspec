
import pytest
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

class fs:
    ipfs = fsspec.filesystem("ipfs")
    local = fsspec.filesystem('file')

# print(fs.local.cat(path='test/data/*', recursive=True))


def configure_rpath(lpath, rpath):
    if os.path.isfile(lpath):
        rpath = os.path.join(rpath,os.path.basename(lpath))
    else:
        assert os.path.isdir(lpath)
        rpath = os.path.join(rpath, lpath.split('/')[-1])

    return rpath

def test_put(lpath = DEFAULT_LPATH, rpath=DEFAULT_RPATH):
    local_dir_hash = fs.local.cat(path=lpath,  recursive=True)
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)

    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    ipfs_dir_hash = fs.ipfs.cat(path=rpath)
    assert local_dir_hash == ipfs_dir_hash, 'local and ipfs hash do not match'  
    assert fs.ipfs.exists(path=rpath) 
    fs.ipfs.rm(path=rpath)
    assert not fs.ipfs.exists(path=rpath)


def test_info(lpath=DEFAULT_LPATH, rpath=DEFAULT_RPATH):
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    assert fs.ipfs.info(rpath)['type'] == 'file'
    assert fs.ipfs.info(os.path.dirname(rpath))['type'] == 'directory'

def test_rm(lpath=DEFAULT_LPATH, rpath = DEFAULT_RPATH):
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    assert fs.ipfs.exists(path=rpath) 
    fs.ipfs.rm(path=rpath)
    assert not fs.ipfs.exists(path=rpath)


def test_get(lpath = DEFAULT_LPATH_DIR, rpath=DEFAULT_RPATH, output_dir = DEFAULT_OUTPUT_DIR):
    # local_dir_hash = fs.local.cat(path=lpath,  recursive=True)
    
    fs.ipfs.rm(rpath)
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath, recursive=True)
    fs.ipfs.get(rpath=rpath, lpath=output_dir, recursive=True)
    
    
    # print(fs.local.cat(path=output_dir, recursive=True))
    if fs.local.exists(output_dir):
        fs.local.rm(output_dir, recursive=True)


def test_cat(lpath = DEFAULT_LPATH, rpath=DEFAULT_RPATH, output_dir = DEFAULT_OUTPUT_DIR):
    # local_dir_hash = fs.local.cat(path=lpath,  recursive=True)
    
    fs.ipfs.change_gateway_type = 'local'
    # fs.ipfs.rm(rpath)
    
    local_bytes = fs.local.cat(lpath)
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    ipfs_bytes= fs.ipfs.cat(cid)
    assert ipfs_bytes==local_bytes, f"ipfs: {ipfs_bytes} != local: {local_bytes}"

def test_get(lpath = 'test/data/input/yo.txt', rpath='/test_get_file',out_lpath = 'test/data/output/yo.txt'):
    if fs.local.exists(out_lpath):
        fs.local.rm(out_lpath, recursive=True)
    

    if fs.local.isdir(out_lpath):
        out_dir =out_lpath
    else:
        out_dir = os.path.dirname(out_lpath)

    print(out_dir)
    fs.ipfs.change_gateway_type = 'local'
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    print('Before: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_dir}/*.txt')])
    print(fs.ipfs.stat(cid))
    fs.ipfs.get(rpath=cid, lpath=out_lpath,recursive=True, return_cid=False)
    print('After: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_dir}/*.txt')])


def test_cat_public(rpath=DEFAULT_RPATH):
    # local_dir_hash = fs.local.cat(path=lpath,  recursive=True)
    # Given a directory CID to cat

    # folder
    fs.ipfs.change_gateway_type = 'public'
    ipfs_bytes = fs.ipfs.cat(path='Qmdgy8KEqoyU9fWdoqPTUDSYWiRsHZVxebkH7fKyJhQnW7')
    print(ipfs_bytes)

    # file
    ipfs_bytes = fs.ipfs.cat(path='QmP8jTG1m9GSDJLCbeWhVSVgEzCPPwXRdCRuJtQ5Tz9Kc9')
    print(ipfs_bytes)
    
# test_get()
# test_info()
# test_put()
# test_rm()
test_get()
# test_cat()
# test_cat_public()
