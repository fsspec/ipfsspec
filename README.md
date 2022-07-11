# ipfsspec

A readonly implementation of fsspec for IPFS.

This Project supports the following functions in ffsspec. Because ipfs uses content hashes we treat the Mutable File System as a proxy for saving files in a unix format while also storing the ipfs object as a cid. This allows for ipfs to interoperate with other 


**Note**: only tested for local node via test/test_ipfs_async.py. 


## Playing around with IPFS in 3 Easy Steps

### Dependencies
- docker
- docker-compose

### Steps
1. spin up docker-compose with ```make up ```
    - this starts a python container and an ipfs node. The localhost is routed through the docker-compose default network. This results in the ipfs host url being ipfs:8080 for when docker-compose is setup, and 127.0.0.1 in any other setting. This is 
2. run ``` docker logs backend``` and get the jupyter link in the format of ```http://127.0.0.1:8888/lab?token={TOKEN}```

3. YOUR ALL SET, play with ```demo.ipynb```




## Supported Functions

### put
- puts local file (lpath) in ipfs mutible file system (rpath)
- recursive is turned on if the lpath is a directory (is this safe? you tell me lol)


```python

fs.ipfs.put(path='test/data/input/yo.txt', rpath='/test_put_file')
```

### rm
- removes path in the MFS store
- this is followed by garbage collection
- the relationship between MFS and pins are essentially the same, with MFS having unix like abstraction

```python

fs.ipfs.put(path='test/data', rpath='/test_rm_folder' ,recursive=True)
fs.ipfs.rm('/test_rm_folder')


```


### ls/find/glob/walk
- list all of the links in an ipfs object cid or a MFS file store


### get

- Gets the mfs path or ipfs content and stores it to a local path


```python
fs.ipfs.get(rpath='QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',lpath='/fam')
# or
fs.ipfs.get(rpath='/mfs/path',lpath='/fam')
```

### cp

- copies path1 into path2 via Mutable File System

```python
fs.cp(path1='/ipfs/QmdgxwYid1bmPjN92GdAEZtUNCWonYK9Jz5chbob5BdphB',path2='/fam')
```

### open
- **read** and write are operational

```python
import fsspec

with fs.ipfs.open("QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
    print(f.read())
```
- for **write**, a temprary file is saved and is written to ipfs when closed

```python
with fs.ipfs.open("/hello/mommy.txt", "w") as f:
    print(f.write(b'bro whadup'))
```




## Multiple Gateways

The current implementation uses a HTTP gateway to access the data. It tries to use a local one (which is expected to be found at `http://127.0.0.1:8080`) and falls back to `ipfs.io` if the local gateway is not available.

You can modify the list of gateways using the space separated environment variable `IPFSSPEC_GATEWAYS`.
