import io
import time
import weakref
import copy
import asyncio
import aiohttp
from fsspec.asyn import _run_coros_in_chunks
from fsspec.utils import is_exception
from fsspec.callbacks import _DEFAULT_CALLBACK
from glob import has_magic
from .buffered_file import IPFSBufferedFile
import json
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from ipfshttpclient.multipart import stream_directory, stream_files #needed to prepare files/directory to be sent through http
import os
from fsspec.exceptions import FSTimeoutError
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import is_exception, other_paths
from .gateway import MultiGateway
from .utils import dict_get, dict_put, dict_hash,dict_equal

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
    
    async def close(self):
        """Close file

        Finalizes writes, discards cache
        """
        if getattr(self, "_unclosable", False):
            return
        if self.closed:
            return
        if self.mode == "rb":
            self.cache = None
        else:
            if not self.forced:
                await self.flush(force=True)

            if self.fs is not None:
                self.fs.invalidate_cache(self.path)
                self.fs.invalidate_cache(self.fs._parent(self.path))

        self.closed = True

    
    def __del__(self):
        self.close_session(loop=self.loop, session=self._session)
    async def _expand_path(self, path, recursive=False, maxdepth=None):
        if isinstance(path, str):
            out = await self._expand_path([path], recursive, maxdepth)
        else:
            # reduce depth on each recursion level unless None or 0
            maxdepth = maxdepth if not maxdepth else maxdepth - 1
            out = set()
            path = [self._strip_protocol(p) for p in path]
            for p in path:  # can gather here
                if has_magic(p):
                    bit = set(await self._glob(p))
                    out |= bit
                    if recursive:
                        out |= set(
                            await self._expand_path(
                                list(bit), recursive=recursive, maxdepth=maxdepth
                            )
                        )
                    continue
                elif recursive:
                    
                    rec = set(await self._find(p, maxdepth=maxdepth, withdirs=True))
                    out |= rec
                if p not in out and (recursive is False or (await self._exists(p))):
                    # should only check once, for the root
                    out.add(p)
        if not out:
            raise FileNotFoundError(path)
        return list(sorted(out))
    async def _rm_file(self ,path, gc=True, **kwargs):
        session = await self.set_session()
        response = await self.gateway.api_post(session=session, endpoint='files/rm', recursive='true', arg=path)
        if gc:
            await self.gateway.api_post(session=session, endpoint='repo/gc')

    # async def _rm(self, path, recursion=True , gc=True,**kwargs):
    #     recursion='true' if recursion else 'false'
    #     session = await self.set_session()
    #     await self.gateway.api_post(session=session, endpoint='files/rm', recursion=recursion, arg=path)
    #     if gc:
    #         await self.gateway.api_post(session=session, endpoint='repo/gc')

    @staticmethod
    def ensure_path(path):
        assert isinstance(path, str), f'path must be string, but got {path}'
        if len(path) == 0:
            path = '/'
        elif len(path) > 0:
            if path[0] != '/':
                path = '/' + path
        
        return path

    
    async def _cat(
        self, path, recursive=False, on_error="raise", batch_size=None, **kwargs
    ):
        if await self._isdir(path=path):
            recursive = True


        paths = await self._expand_path(path, recursive=recursive)
        
        async def _file_filter(p):
            # FIX: returns path if file, else returns False
            if await self._isfile(p):
                return p
            else:
                return False

        paths = [_ for _ in await asyncio.gather(*[ _file_filter(p) for p in paths]) if bool(_) ]
        coros = [self._cat_file(path, **kwargs) for path in paths]
        batch_size = batch_size or self.batch_size
        out = await _run_coros_in_chunks(
            coros, batch_size=batch_size, nofiles=True, return_exceptions=True
        )
        if on_error == "raise":
            ex = next(filter(is_exception, out), False)
            if ex:
                raise ex

        assert len(paths) == len(out)
        if len(paths) == 1:
            return out[0]
        elif len(paths) > 1:
            return {
                k: v
                for k, v in zip(paths, out)
                if on_error != "omit" or not is_exception(v)
            }


    async def  _stat(self, path):
        session = await self.set_session()

        res = await self.gateway.api_get(endpoint='files/stat', session=session, path=path)
        return res
    stat = sync_wrapper(_stat)

    
    async def _ls(self, path='/', detail=True, recursive=False, **kwargs):
        # path = self._strip_protocol(path)

        path = self.ensure_path(path=path)
        session = await self.set_session()
        res = await self.gateway.ls(session=session, path=path)

        if recursive:
            # this is prob not needed with self.find
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
            return [r["name"] for r in res]




    ls = sync_wrapper(_ls)

    # def _store_path(self, path, hash):

    async def pin(self,cid):
        res = await self.gateway.api_post(endpoint='pin/add', session=session, params={'arg':cid})
        return self.client.pin.add(cid)


    async def _is_pinned(self, cid):
        session = await self.set_session()
        res = await self.gateway.api_post(endpoint='pin/ls', session=session, params={'arg':cid})
        pinned_cid_list = list(json.loads(res.decode()).get('Keys').keys())
        return bool(cid in pinned_cid_list)

    is_pinned = sync_wrapper(_is_pinned)


    async def pin(self, cid, recursive=False, progress=False):
        session = await self.set_session()
        res = await self.gateway.api_post(endpoint='pin/add', session=session, params={'arg':cid, 
                                                                     'recursive': recursive,
                                                                      'progress': progress})
        return bool(cid in pinned_cid_list)

    pin = sync_wrapper(_is_pinned)


    async def _api_post(self, endpoint, **kwargs):
        session = await self.set_session()
        return await self.gateway.api_post(endpoint=endpoint, session=session, **kwargs)
    api_post = sync_wrapper(_api_post)

    async def _api_get(self, endpoint, **kwargs):
        session = await self.set_session()
        res =  await self.gateway.api_get(endpoint=endpoint, session=session, **kwargs)
        if res.headers['Content-Type'] == 'application/json':
            res = await res.json()
        elif res.headers['Content-Type'] == 'text/plain':
            res = json.loads((await res.content.read()).decode())
        return res
    api_get = sync_wrapper(_api_get)

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
        res = await self.gateway.api_post(endpoint='add', session=session,  params=params, data=data, headers=headers)

        res =  await res.json()

        return res
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
        return_cid = True,
        **kwargs
    ):
        
        # if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        session = await self.set_session()

        if 'path' in kwargs:
            lpath = kwargs.pop('path')

        if rpath[0] != '/' and len(rpath) > 1:
            rpath = '/' + rpath 

        params = {}
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        params.update(kwargs)
        params['wrap-with-directory'] = 'true'

        local_isdir = os.path.isdir(lpath)
        local_isfile = os.path.isfile(lpath)

        assert bool(local_isfile) != bool(local_isdir), \
                f'WTF, local_isfile: {local_isfile} && local_isdir: {local_isdir}'
        
        if local_isdir:
            assert bool(local_isfile) == False
            data, headers = stream_directory(lpath, chunk_size=chunker, recursive=recursive)
        else:
            assert bool(local_isfile) == True
            data, headers = stream_files(lpath, chunk_size=chunker)
        
        
        data = self.data_gen_wrapper(data=data)                             
        res = await self.gateway.api_post(endpoint='add', session=session, params=params, data=data, headers=headers)
        
        res =  await res.content.read()
        res = list(map(lambda x: json.loads(x), filter(lambda x: bool(x),  res.decode().split('\n'))))
        res = list(filter(lambda x: isinstance(x, dict) and x.get('Name'), res))
        res_hash = res[-1]["Hash"]
        
        if pin and not rpath:
            rpath='/'
        if rpath:
            
            if  local_isdir:
                await self._cp(path1=f'/ipfs/{res[-1]["Hash"]}', path2=rpath )
            else:

                cid_hash = res[-1]["Hash"]
                ipfs_path = f'/ipfs/{cid_hash}'
                tmp_path = f'{rpath}/{cid_hash}'
                rdir = os.path.dirname(tmp_path)
                final_path = f'{rpath}/{os.path.basename(lpath)}'
                if not (await self._isdir(rdir)):
                    await self._mkdir(path=rdir)


                if rdir[-1] != '/' and len(rdir) > 1:
                    rdir = rdir + '/'
                await self._cp(path1=ipfs_path, path2=rdir  )
                ipfs_path = f'/{rpath}/{cid_hash}'
                await self._cp(path1=tmp_path, path2=final_path )
                await self._rm_file(ipfs_path)
        
        
        if return_cid:
            return res_hash
        return res

    put = sync_wrapper(_put)


    async def _mkdir(self, path):
        session = await self.set_session()
        return await self.gateway.api_post(session=session, endpoint='files/mkdir', arg=path)
    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        # TODO: implement on_error
        batch_size = batch_size or self.batch_size
        path = await self._expand_path(path, recursive=recursive)
        return await _run_coros_in_chunks(
            [self._rm_file(p, **kwargs) for p in path],
            batch_size=batch_size,
            nofiles=True,
        )

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        
        
        session = await self.set_session()
        return (await self.gateway.cat(session=session, path=path))[start:end]

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        info = await self.gateway.file_info(session=session, path=path)
        return info

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

    # async def _get(self,
    #     rpath,
    #     lpath=None,
    #     **kwargs
    # ):
    #     if 'path' in kwargs:
    #         lpath = kwargs.pop('path')
        
    #     session = await self.set_session()
    #     if lpath is None: lpath = os.getcwd()
    #     self.full_structure = await self.gateway.get_links(session=session, rpath=rpath, lpath=lpath)
    #     await self.gateway.save_links(session=session, links=self.full_structure)
    # get=sync_wrapper(_get)

    async def _get_file(self, rpath, lpath, **kwargs):
        import shutil
        session = self._session
        # shutil.rmtree(lpath)
        data = await self._cat(path=rpath)
        
        f = open(lpath, mode='wb')
        f.write(data)
        f.close()

    async def _get(
        self, rpath, lpath, recursive=True, callback=_DEFAULT_CALLBACK, **kwargs
    ):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        The get_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit .
        """
        from fsspec.implementations.local import make_path_posix

        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)

        root_dir_lpath = lpath if os.path.isdir(lpath) else os.path.dirname(lpath)

        rpaths = await self._expand_path(rpath, recursive=recursive)

        lpaths = other_paths(rpaths, lpath)
        [os.makedirs(os.path.dirname(lp), exist_ok=True) for lp in lpaths]
        batch_size = kwargs.pop("batch_size", self.batch_size)

        #TODO: not good for hidden files
        lpaths = list(filter(lambda p: len(p.split('.')) == 2, lpaths))
        rpaths = list(filter(lambda p: len(p.split('.')) == 2, rpaths))



        coros = []
        callback.set_size(len(lpaths))
        for lpath, rpath in zip(lpaths, rpaths):
            callback.branch(rpath, lpath, kwargs)
            coros.append(self._get_file(rpath, lpath, **kwargs))
        return await _run_coros_in_chunks(
            coros, batch_size=batch_size, callback=callback
        )