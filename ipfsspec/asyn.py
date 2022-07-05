import io
import time
import weakref
import copy
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

    async def _ls(self, path='', detail=True, recursive=True, **kwargs):
        # path = self._strip_protocol(path)
        session = await self.set_session()
        
        res = await self.gateway.ls(session=session, path=path)

        
        if recursive:
            res_list = []
            cor_list = []
            for r in res:
                if r['type'] == 'directory':
                    cor_list.append(self._ls(path=r['name'], detail=True, recursive=recursive, **kwargs))
                elif r['type'] == 'file':
                    res_list += [r]
                    
            if len(cor_list) > 0:
                for r in (await asyncio.gather(*cor_list)):
                    res_list += r
            res = res_list


        if detail:
            return res
        else:
            print(res)
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


    async def _api_post(self, endpoint, **kwargs):
        session = await self.set_session()
        return await self.gateway.api_post(endpoint=endpoint, session=session, **kwargs)
    api_post = sync_wrapper(_api_post)

    async def _cp(self,path1, path2):
        session = await self.set_session()
        res = await self.gateway.cp(session=session, arg=[path1, path2])
        return res
    cp = sync_wrapper(_cp)

    async def _put_file(self,
        lpath=None,
        rpath=None,
        pin=True,
        chunker=262144, 
        wrap_with_directory=False,
        **kwargs
    ):

        if 'path' in kwargs:
            lpath = kwargs.pop('path')

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

        res =  await res.content.read()
        return json.loads(res.decode())
        # return res
    
    @staticmethod
    async def data_gen_wrapper(data):
        for d in data:
            yield d


    async def _put(self,
        lpath=None, 
        rpath=None,
        recursive=True,
        pin=True,
        chunker=262144, 
        return_json=True,
        **kwargs
    ):
        
        # if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        session = await self.set_session()

        if 'path' in kwargs:
            lpath = kwargs.pop('path')

        params = {}
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        params.update(kwargs)
        params['wrap-with-directory'] = 'true'


        # print(os.path.isdir(lpath))
        if os.path.isdir(lpath):
            data, headers = stream_directory(lpath, chunk_size=chunker, recursive=recursive)
        else:
            data, headers = stream_files(lpath, chunk_size=chunker)


        data = self.data_gen_wrapper(data=data)                             
        res = await self.gateway.api_post('add', session=session, params=params, data=data, headers=headers)
        res =  await res.content.read()
        if return_json:
            res = list(map(lambda x: json.loads(x), filter(lambda x: bool(x),  res.decode().split('\n'))))
            res = list(filter(lambda x: isinstance(x, dict) and x.get('Name'), res))
        if pin and not rpath:
            rpath='/'
        if rpath:
            await self._cp(path1=f'/ipfs/{res[-1]["Hash"]}', path2=rpath)
        
        return res

    put = sync_wrapper(_put)

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


    async def _get_links(self,
        path,
        fol
    ):
        root_struct = {}
        struct = {}
        
        if self.gateway_type == 'local':
            res = self._gw_apipost('ls', arg=path)
            links = parse_response(res)[0]['Objects'][0]['Links']            
            for link in links:
                name = f'{fol}/{link["Name"]}'
                hash_ = link['Hash']
                if link['Type'] == 1:
                    details = await self._get_links(hash_, name)
                else:
                    details = {'Hash': hash_}
                struct[name] = details
        elif self.gateway_type == 'infura':
            res = await self.gateway.api_post('dag/get', arg=path)
            links = (await res.json())['Links']
            for link in links:
                name = f'{fol}/{link["Name"]}'
                hash_ = link['Hash']['/']
                if len(name.split('.')) == 1:
                    details = self._get_links(hash_, name)
                else:
                    details = {'Hash': hash_}
                struct[name] = details
        else:
            raise TypeError ('`get` not supported on public gateways')
        root_struct[fol] = struct
        return root_struct

    async def _save_links(self,links):
        return asyncio.gather([self._save_link(k=k,v=v)for k, v in links.items()])

    async def _save_link(self, k,v):
        if len(k.split('.')) < 2:
            if not os.path.exists(k): 
                os.mkdir(k)
            await self._save_links(v)
        else:
            data = await self.cat_file(links[k]['Hash'])
            with open(k, 'wb') as f:
                f.write(data.encode('utf-8'))    

    async def _get(self,
        rpath,
        lpath=None,
        **kwargs
    ):
        if lpath is None: lpath = os.getcwd()
        self.full_structure = await self._get_links(rpath, lpath)
        await self._save_links(self.full_structure)

    get=sync_wrapper(_get)
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





