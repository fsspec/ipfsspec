# ipfsspec

A readonly implementation of fsspec for IPFS.

This Project supports the following functions in ffsspec. Because ipfs uses content hashes we treat the Mutable File System as a proxy for saving files in a unix format while also storing the ipfs object as a cid. 


### open
- r

### put/put_file
- puts local file (lpath) in ipfs mutible file system (rpath)

### rm
- removes path in the MFS store

### ls
- list all of the links in an ipfs object cid or a MFS file store

### cp

- copies path1 into path2 via Mutable File System

```python
fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
```

### cat/cat_file

### put/put_file

### get/get_file

### expand_path

### du

### size
- get the size of the ipfs path/ MFS path within


```python
import fsspec

with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
    print(f.read())
```

The current implementation uses a HTTP gateway to access the data. It tries to use a local one (which is expected to be found at `http://127.0.0.1:8080`) and falls back to `ipfs.io` if the local gateway is not available.

You can modify the list of gateways using the space separated environment variable `IPFSSPEC_GATEWAYS`.
