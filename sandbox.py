import fsspec

from ipfsspec.asyn import AsyncIPFSFileSystem
from fsspec import register_implementation
from ipfsspec.syn import IPFSFileSystem


# register_implementation(IPFSFileSystem.protocol, IPFSFileSystem)
register_implementation(AsyncIPFSFileSystem.protocol, AsyncIPFSFileSystem)

# with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
#     print(f.read())
fs = fsspec.filesystem("ipfs")
TEST_ROOT = 'QmRys6xQ8XbVzoKriEYo6d3fro3PdGxb8cG1zwS9RZVnTG'
def test_get_hello_world():
    # with fsspec.open(f"ipfs://{TEST_ROOT}", "rb") as f:
    #     f.read()
    fs.put_file('setup.py')

print(fs.ls(path=TEST_ROOT))
# # @pytest.mark.parametrize("filename", ["default", "multi", "raw", "raw_multi", "write"])
# def test_different_file_representations(filename):
#     fs = fsspec.filesystem("ipfs")
#     # assert fs.size(f"{TEST_ROOT}/{filename}") == len(REF_CONTENT)
#     with fsspec.open(f"ipfs://{TEST_ROOT}/{filename}") as f:
#         assert f.read() == REF_CONTENT

test_get_hello_world()