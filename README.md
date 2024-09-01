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

* create the file `~/.ipfs/gateway` with the gateway address as first line
* define the environment variable `IPFS_GATEWAY` to the gateway address
* create the file `/etc/ipfs/gateway` with the gateway address as first line

No matter which option you use, the gateway has to be specified as an HTTP(S) url, e.g.: `http://127.0.0.1:8080`.
