

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
DEFAULT_LPATH_DIR = 'test/data'
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
        rpath = os.path.join(rpath, os.path.join(path.split('/')[1:]))
    
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

def test_get(lpath = DEFAULT_LPATH, rpath=DEFAULT_RPATH):
    local_dir_hash = fs_file.cat(path=lpath,  recursive=True)
    cid = fs.put(lpath=lpath, rpath=rpath)
    fs.get(rpath=configure_rpath(rpath=rpath, lpath=lpath), lpath='output')

test_get()
# test_put_directory()

# test_put()
# test_rm()
# test_info()
# # put in a file
# cid = fs.put(path='test/data', rpath='/y')
# print(cid[-1]['Hash'])
# print(fs.info(f"ipfs://{cid[-1]['Hash']}"))
# with fs.open(f"ipfs://{cid[-1]['Hash']}", mode="rb") as f:
#     print(f.read())
# print(fs.find('', detail=False))  
# print(fs.isdir('/wefer'))
# print(fs.rm('/', recursive=False))
# print(fs.ls('/', recursive=True,  detail=False))



# import fsspec

# from ipfsspec.asyn import AsyncIPFSFileSystem
# from fsspec import register_implementation
# from ipfsspec.syn import IPFSFileSystem
# import asyncio
# import io

# # register_implementation(IPFSFileSystem.protocol, IPFSFileSystem)
# register_implementation(AsyncIPFSFileSystem.protocol, AsyncIPFSFileSystem)

# # with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
# #     print(f.read())
# fs = fsspec.filesystem("ipfs")
# TEST_ROOT = 'QmRys6xQ8XbVzoKriEYo6d3fro3PdGxb8cG1zwS9RZVnTG'
# def test_get_hello_world():
#     # with fsspec.open(f"ipfs://{TEST_ROOT}", mode="rb") as f:
#     #     print(f.read())
#     import json
#     # print(res)
#     # fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
#     print(fs.put(path='./test/data/'))
#     # fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
#     # fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
#     # print(fs.ls(path='/', detail=False,recursive=True))
#     # print(fs.rm_file(path='/whadup/dawg/yo.txt', gc=True))
#     # print(fs.find(path='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB'))
#     print(fs.glob(path='/fam/*o.txt'))

#     # print([i for i in fs.walk(path='/')])
#     # print(fs.info(path='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB'))
#     # print(fs.ls(path='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB'), 'PATH')
#     # print(res)


# # print(fs.ls(path=TEST_ROOT))
# # # @pytest.mark.parametrize("filename", ["default", "multi", "raw", "raw_multi", "write"])
# # def test_different_file_representations(filename):
# #     fs = fsspec.filesystem("ipfs")
# #     # assert fs.size(f"{TEST_ROOT}/{filename}") == len(REF_CONTENT)
# #     with fsspec.open(f"ipfs://{TEST_ROOT}/{filename}") as f:
# #         assert f.read() == REF_CONTENT

# test_get_hello_world()