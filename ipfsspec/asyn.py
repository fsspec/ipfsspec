import io
import time
import weakref

import asyncio
import aiohttp

from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from ipfshttpclient.multipart import stream_directory, stream_files #needed to prepare files/directory to be sent through http
import os
from fsspec.exceptions import FSTimeoutError

from .utils import get_default_gateways
from .gateway import MultiGateway

import logging

logger = logging.getLogger("ipfsspec")


class RequestsTooQuick(OSError):
    def __init__(self, retry_after=None):
        self.retry_after = retry_after




DEFAULT_GATEWAY = None



    


class AsyncIPFSFileSystem(AsyncFileSystem):
    sep = "/"
    protocol = "ipfs"

    def __init__(self, asynchronous=False, loop=None, client_kwargs=None, **storage_options):
        super().__init__(self, asynchronous=asynchronous, loop=loop, **storage_options)
        self._session = None

        self.client_kwargs = client_kwargs or {}

        if not asynchronous:
            sync(self.loop, self.set_session)

    @property
    def gateway(self):
        return MultiGateway.get_gateway()

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

    async def set_session(self):
        if self._session is None:
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


    async def _put_file(self,
        path=None,
        pin=True,
        chunker=262144, 
        wrap_with_directory=False,
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
        
        data, headers = stream_files(path, chunk_size=chunker)
                                                      
        res = await self.gateway.api_post('add', session,  params=params, data=data, headers=headers)
        
        return res
    
    async def _put(self,
        rpath=None,
        lpath=None, 
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
        res = await self.gateway.api_post('add', params=params, data=io.BytesIO(data))
        
        return res

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return (await self.gateway.cat(path, session))[start:end]

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return await self.gateway.file_info(path, session)

    def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        
        if mode == 'rb':
            data = self.cat_file(path)  # load whole chunk into memory
            return io.BytesIO(data)
        elif mode == 'wb':
            self.put_file(path)
        else:
            raise NotImplementedError

    def ukey(self, path):
        """returns the CID, which is by definition an unchanging identitifer"""
        return self.info(path)["CID"]

