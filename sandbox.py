import fsspec

from ipfsspec.asyn import AsyncIPFSFileSystem
from fsspec import register_implementation
from ipfsspec.syn import IPFSFileSystem
import asyncio
import io

# register_implementation(IPFSFileSystem.protocol, IPFSFileSystem)
register_implementation(AsyncIPFSFileSystem.protocol, AsyncIPFSFileSystem)

# with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
#     print(f.read())
fs = fsspec.filesystem("ipfs")
TEST_ROOT = 'QmRys6xQ8XbVzoKriEYo6d3fro3PdGxb8cG1zwS9RZVnTG'
def test_get_hello_world():
    # with fsspec.open(f"ipfs://{TEST_ROOT}", mode="rb") as f:
    #     print(f.read())
    import json
    # print(res)
    # fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
    # fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
    # print(fs.ls(path='/', detail=False,recursive=True))
    # print(fs.rm_file(path='/whadup/dawg/yo.txt', gc=True))
    # print(fs.find(path='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB'))
    print(fs.glob(path='/fam/*o.txt'))

    # print([i for i in fs.walk(path='/')])
    # print(fs.info(path='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB'))
    # print(fs.ls(path='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB'), 'PATH')
    # print(res)


# print(fs.ls(path=TEST_ROOT))
# # @pytest.mark.parametrize("filename", ["default", "multi", "raw", "raw_multi", "write"])
# def test_different_file_representations(filename):
#     fs = fsspec.filesystem("ipfs")
#     # assert fs.size(f"{TEST_ROOT}/{filename}") == len(REF_CONTENT)
#     with fsspec.open(f"ipfs://{TEST_ROOT}/{filename}") as f:
#         assert f.read() == REF_CONTENT

test_get_hello_world()