import ipfsspec  # noqa: F401
import fsspec


def test_get_hello_world():
    with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx") as f:
        assert f.read() == b'hello worlds\n'
