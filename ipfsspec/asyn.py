import io
import time
import weakref

import asyncio
import aiohttp
from .buffered_file import IPFSBufferedFile
import json
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from ipfshttpclient.multipart import stream_directory, stream_files #needed to prepare files/directory to be sent through http
import os
from fsspec.exceptions import FSTimeoutError
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractBufferedFile
from .gateway import MultiGateway
from .utils import dict_get, dict_put

import logging

logger = logging.getLogger("ipfsspec")


class RequestsTooQuick(OSError):
    def __init__(self, retry_after=None):
        self.retry_after = retry_after




DEFAULT_GATEWAY = None


import requests
from requests.exceptions import HTTPError
    
class AsyncRequestSession:
    def __init__(self, loop=None, 
                adapter=dict(pool_connections=100, pool_maxsize=100), 
                **kwargs):
                
        self.session = requests.Session()
        self.loop = loop
        adapter = requests.adapters.HTTPAdapter(**adapter)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    async def get(self, *args, **kwargs):
        return await self.loop.run_in_executor(None, lambda x: self.session.get(*args,**kwargs), None)
    
    async def post(self, *args, **kwargs):
        return await self.loop.run_in_executor(None, lambda x: self.session.post(*args, **kwargs), None)

    async def close(self):
        pass
class AsyncIPFSFileSystem(AsyncFileSystem):
    sep = "/"
    protocol = "ipfs"
    root = '/tmp/fspec/ipfs'

    def __init__(self, asynchronous=False,
                 gateway_type='local',
                loop=None, 
                root = None,
                client_kwargs={},
                 **storage_options):
        super().__init__(self, asynchronous=asynchronous, loop=loop, **storage_options,)
        self._session = None
        self.client_kwargs=client_kwargs
        
        if root:
            self.root = root

        self.gateway_type = gateway_type
        self.fs_local = LocalFileSystem(auto_mkdir=True)

        if not asynchronous:
            sync(self.loop, self.set_session)

    @property
    def gateway(self, gateway_type = None):
        if gateway_type is None:
            gateway_type = self.gateway_type
        return MultiGateway.get_gateway(gateway_type=self.gateway_type)

    @staticmethod
    async def get_client(**kwargs):
        timeout = aiohttp.ClientTimeout(sock_connect=1, sock_read=5)
        kwargs = {"timeout": timeout, **kwargs}
        return aiohttp.ClientSession(**kwargs)


    @staticmethod
    def close_session(loop, session):
        if loop is not None and loop.is_running():
            try:
                sync(loop, session.close, timeout=0.1)
                return
            except (TimeoutError, FSTimeoutError):
                pass
        if session._connector is not None:
            # close after loop is dead
            session._connector._close()

    async def set_session(self, refresh=False):
        if (not self._session) or (refresh==True):
            self._session = await self.get_client(loop=self.loop, **self.client_kwargs)
            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)
        return self._session

    async def _ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        res = await self.gateway.ls(path, session)
        if detail:
            return res
        else:
            return [r["name"] for r in res]

    ls = sync_wrapper(_ls)


    async def _put_dir(self,
        path=None,
        pin=True,
        chunker=262144, 
        **kwargs
    ):
        session = await self.set_session()
        if not os.path.isfile(path): raise TypeError ('Use `put` to upload a directory')        
        # if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        params = {}
        params['wrap-with-directory'] = 'true' if wrap_with_directory else 'false'
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        params.update(kwargs)
        data, headers = stream_directory(path, chunk_size=chunker)
        data = self.data_gen_wrapper(data=data)                                  
        res = await self.gateway.api_post('add', session,  params=params, data=data, headers=headers)
        
        return res
    


    def store_pin(self, path):
        return self.fs_local.put_file(path1, path2)
        


    # def _store_path(self, path, hash):

    def pin(self,cid):
        return self.client.pin.add(cid)


    async def _is_pinned(self, cid):
        session = await self.set_session()
        res = await self.gateway.api_post('pin/ls', session, params={'arg':cid})
        pinned_cid_list = list(json.loads(res.decode()).get('Keys').keys())
        return bool(cid in pinned_cid_list)

    is_pinned = sync_wrapper(_is_pinned)


    async def pin(self, cid, recursive=False, progress=False):
        session = await self.set_session()
        res = await self.gateway.api_post('pin/add', session, params={'arg':cid, 
                                                                     'recursive': recursive,
                                                                      'progress': progress})
        return bool(cid in pinned_cid_list)

    pin = sync_wrapper(_is_pinned)



    async def copy(self):
        res = await self.gateway.api_post('cp', session,  params=params)


    async def _put_file(self,
        lpath=None,
        rpath=None,
        pin=True,
        chunker=262144, 
        wrap_with_directory=False,
        **kwargs
    ):
        session = await self.set_session()
        if not os.path.isfile(lpath): raise TypeError ('Use `put` to upload a directory')        
        if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        params = {}
        params['wrap-with-directory'] = 'true' if wrap_with_directory else 'false'
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        
        data, headers = stream_files(lpath, chunk_size=chunker)
        data = self.data_gen_wrapper(data=data)                                        
        res = await self.gateway.api_post('add', session,  params=params, data=data, headers=headers)


        return json.loads(res.decode())
        # return res
    
    @staticmethod
    async def data_gen_wrapper(data):
        for d in data:
            yield d


    async def _put(self,
        path=None, 
        recursive=True,
        pin=True,
        chunker=262144, 
        **kwargs
    ):
        
        # if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        params = {}
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        params.update(kwargs)
        data, headers = stream_directory(path, chunk_size=chunker, recursive=recursive) 
        data = self.data_gen_wrapper(data=data)                             
        res = await self.gateway.api_post('add', params=params, data=data)
        return res

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return (await self.gateway.cat(path, session))[start:end]

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return await self.gateway.file_info(path, session)

    def open(self, path, mode="rb",  block_size="default",autocommit=True,
                cache_type="readahead", cache_options=None, size=None, **kwargs):
        
        return IPFSBufferedFile(
                            fs=self,
                            path=path,
                            mode=mode,
                            block_size=block_size,
                            autocommit=autocommit,
                            cache_type=cache_type,
                            cache_options=cache_options,
                            size=size
                        )

        # if mode == 'rb':
        #     data = self.cat_file(path)  # load whole chunk into memory
        #     return io.BytesIO(data)
        # elif mode == 'wb':
        #     self.put_file(path)
        # else:
        #     raise NotImplementedError

    def ukey(self, path):
        """returns the CID, which is by definition an unchanging identitifer"""
        return self.info(path)["CID"]



class IPFSBufferedFile(AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        super(IPFSBufferedFile, self).__init__(*args, **kwargs)
        self.__content = None

    def _fetch_range(self, start, end):
        if self.__content is None:
            self.__content = self.fs.cat_file(self.path)
        content = self.__content[start:end]
        if "b" not in self.mode:
            return content.decode("utf-8")
        else:
            return content


