# ipfsspec

A readonly implementation of fsspec for IPFS.

## Installation

You can install `ipfsspec` directly from git with the following command:

```bash
pip install ipfsspec
```

## Usage

This project is currently very rudimentaty. It is not yet optimized for efficiency and is not yet feature complete. However it should be enough to list directory contents and to retrieve files from `ipfs://` resources via fsspec. A simple hello worlds would look like:

```python
import fsspec

with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
    print(f.read())
```

The current implementation uses a HTTP gateway to access the data. It uses [IPIP-280](https://github.com/ipfs/specs/pull/280) to determine which gateway to use. If you have a current installation of an IPFS node (e.g. kubo, IPFS Desktop etc...), you should be fine. In case you want to use a different gateway, you can use any of the methods specified in IPIP-280, e.g.:

- create the file `~/.ipfs/gateway` with the gateway address as first line
- define the environment variable `IPFS_GATEWAY` to the gateway address
- create the file `/etc/ipfs/gateway` with the gateway address as first line

No matter which option you use, the gateway has to be specified as an HTTP(S) url, e.g.: `http://127.0.0.1:8080`.

## Implementation details

ipfsspec supports retrieval and verification of [UnixFS](https://specs.ipfs.tech/unixfs/) encoded files and directories. UnixFS HAMTs have not been implemented yet.

fsspec uses entry points to discover filesystem implementations. When you install ipfsspec, it registers itself via an entry point in its [pyproject.toml](./pyproject.toml):

```toml
[project.entry-points."fsspec.specs"]
ipfs = "ipfsspec.AsyncIPFSFileSystem"
```

When you call `fsspec.open("ipfs://...")`:

1. fsspec scans entry points in the `fsspec.specs` group using Python's package metadata
2. Finds the ipfs entry point that points to `ipfsspec.AsyncIPFSFileSystem`
3. Dynamically loads and instantiates that class to handle the request

ipfsspec just needs to be installed and have the right entry point declared, and fsspec automatically discovers it.

The actual filesystem class (`AsyncIPFSFileSystem`) in [async_ipfs.py](./ipfsspec/async_ipfs.py) inherits from `fsspec.asyn.AsyncFileSystem` and implements the required methods like `_cat_file()`, `_ls()`, etc. to fetch and verify data from [IPFS HTTP trustless gateways](https://specs.ipfs.tech/http-gateways/trustless-gateway/).

All data fetched is verified to match the CID by fetching [CAR files](https://ipld.io/specs/transport/car/carv1/) which contain the merkle proofs. This means you don't need to trust the gateway.
