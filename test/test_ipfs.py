import fsspec
import pytest


def test_get_hello_world():
    with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx") as f:
        assert f.read() == b'hello worlds\n'


TEST_ROOT = "QmW3CrGFuFyF3VH1wvrap4Jend5NRTgtESDjuQ7QhHD5dd"
REF_CONTENT = b'ipfsspec test data'


@pytest.mark.parametrize("filename", ["default", "multi", "raw", "raw_multi", "write"])
def test_different_file_representations(filename):
    fs = fsspec.filesystem("ipfs")
    assert fs.size(f"{TEST_ROOT}/{filename}") == len(REF_CONTENT)
    with fsspec.open(f"ipfs://{TEST_ROOT}/{filename}") as f:
        assert f.read() == REF_CONTENT
