
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

def test_put():
    fs.ipfs.change_gateway_type = 'local'
    
    lpath = 'test/data/input/yo.txt'
    rpath = '/test'
    
    local_dir_hash = fs.local.cat(path=lpath,  recursive=True)
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)

    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    ipfs_dir_hash = fs.ipfs.cat(path=rpath)
    assert local_dir_hash == ipfs_dir_hash, 'local and ipfs hash do not match'  
    assert fs.ipfs.exists(path=rpath) 
    fs.ipfs.rm(path=rpath)
    assert not fs.ipfs.exists(path=rpath)


def test_info(lpath=DEFAULT_LPATH, rpath=DEFAULT_RPATH):
    fs.ipfs.change_gateway_type = 'local'
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    assert fs.ipfs.info(rpath)['type'] == 'file'
    assert fs.ipfs.info(os.path.dirname(rpath))['type'] == 'directory'

def test_rm(lpath=DEFAULT_LPATH, rpath = DEFAULT_RPATH):
    fs.ipfs.change_gateway_type = 'local'
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    rpath = configure_rpath(rpath=rpath, lpath=lpath)
    assert fs.ipfs.exists(path=rpath) 
    fs.ipfs.rm(path=rpath)
    assert not fs.ipfs.exists(path=rpath)



def test_cat(lpath = DEFAULT_LPATH, rpath=DEFAULT_RPATH, output_dir = DEFAULT_OUTPUT_DIR):
    # local_dir_hash = fs.local.cat(path=lpath,  recursive=True)
    
    fs.ipfs.change_gateway_type = 'local'
    # fs.ipfs.rm(rpath)
    
    local_bytes = fs.local.cat(lpath)
    cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
    ipfs_bytes= fs.ipfs.cat(cid)
    assert ipfs_bytes==local_bytes, f"ipfs: {ipfs_bytes} != local: {local_bytes}"

@pytest.mark.parametrize("mode", [ "file1", 'file2', 'folder' ])

def test_get_rpath(mode, gateway_type='local'):

    fs.ipfs.change_gateway_type = gateway_type

    if mode == 'file1':
        lpath = 'test/data/input/yo.txt'
        rpath='/test_get_file'
        out_lpath = 'test/data/output'

        if fs.local.exists(out_lpath):
            fs.local.rm(out_lpath, recursive=True)

        fs.ipfs.rm(out_lpath, recursive=True)
        # print(out_dir)
        fs.ipfs.rm(rpath, recursive=True)
        cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
        print('Before: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_lpath}')])
        fs.ipfs.get(rpath=rpath, lpath=out_lpath,recursive=True, return_cid=False)
        print('After: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_lpath}')])


    if mode == 'file2':
        lpath = 'test/data/input/yo.txt'
        rpath='/test_get_file'
        out_lpath = 'test/data/output'

        if fs.local.exists(out_lpath):
            fs.local.rm(out_lpath, recursive=True)

        fs.ipfs.rm(out_lpath, recursive=True)
        # print(out_dir)
        fs.ipfs.rm(rpath, recursive=True)
        cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
        print('Before: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_lpath}')])
        fs.ipfs.get(rpath=rpath, lpath=out_lpath,recursive=True, return_cid=False)
        print('After: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_lpath}')])


    if mode == 'folder':
        lpath = 'test/data/input'
        rpath='/test_get_file'
        out_lpath = 'test/data/output'

        if fs.local.exists(out_lpath):
            fs.local.rm(out_lpath, recursive=True)

        fs.ipfs.rm(out_lpath, recursive=True)
        # print(out_dir)
        fs.ipfs.rm(rpath, recursive=True)
        cid = fs.ipfs.put(lpath=lpath, rpath=rpath)
        print('Before: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_lpath}/**')])
        fs.ipfs.get(rpath=rpath, lpath=out_lpath,recursive=True, return_cid=False)
        print('After: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{out_lpath}/**')])

    if fs.local.exists(out_lpath):
        fs.local.rm(out_lpath, recursive=True)


@pytest.mark.parametrize("mode", [ "file", 'folder', ])
@pytest.mark.parametrize("gateway_type", [ "local", 'public', ])
def test_get_cid(mode, gateway_type): 
    
    fs.ipfs.change_gateway_type = gateway_type


    if mode == 'file':


        lpath = 'test/data/output/yo.txt'
        rpath='QmP8jTG1m9GSDJLCbeWhVSVgEzCPPwXRdCRuJtQ5Tz9Kc9'
        ldir = os.path.dirname(lpath)
        if fs.local.exists(lpath):
            fs.local.rm(lpath, recursive=True)
            
        print('Before: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{lpath}')])
        ipfs_bytes = fs.ipfs.get(rpath=rpath, lpath=lpath)
        print('After: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{lpath}')])

    if mode == 'folder':
        lpath = 'test/data/output'
        rpath='QmQwhnitZWNrVQQ1G8rL4FRvvZBUvHcxCtUreskfnBzvD8'
        if fs.local.exists(lpath):
            fs.local.rm(lpath, recursive=True)
        print('Before: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{lpath}/*')])
        ipfs_bytes = fs.ipfs.get(rpath=rpath, lpath=lpath)
        print('After: ', [p.lstrip(os.getcwd()) for p in fs.local.glob(f'{lpath}/*')])


    
# test_get()
# test_info()
# test_put()
# test_rm()
# test_get()
# for m in ['file', 'folder']:
#     for gw in ['local', 'public']:
#         test_get_cid(mode=m, gateway_type=gw)


# for m in ['file1', 'file2', 'folder']:
#     test_get_rpath(mode=m)


# test_get_public('folder')
# test_cat()
# test_cat_public()
