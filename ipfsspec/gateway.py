import io
import time
import weakref
import typing as ty

import asyncio
import aiohttp
import logging
import ipfshttpclient 
import copy
import os

logger = logging.getLogger("ipfsspec")

class RequestsTooQuick(OSError):
    def __init__(self, retry_after=None):
        self.retry_after = retry_after

def get_default_gateways():
    try:
        return os.environ["IPFSSPEC_GATEWAYS"].split()
    except KeyError:
        return GATEWAYS


import os

# IPFSHTTP_LOCAL_HOST = os.getenv('IPFSHTTP_LOCAL_HOST', '127.0.0.1')

class AsyncIPFSGatewayBase:

    # DEFAULT_GATEWAY_MAP = {
    # 'local': f"http://{IPFSHTTP_LOCAL_HOST}:8080",
    # 'public': "https://ipfs.io",
    # 'pinata': "https://gateway.pinata.cloud",
    # 'cloudflare': "https://cloudflare-ipfs.com",
    #  'dweb': "https://dweb.link",
    # }

    # DEFAULT_GATEWAYS = list(DEFAULT_GATEWAY_MAP.keys())
    # DEFAULT_GATEWAY_TYPES = list(DEFAULT_GATEWAY_MAP.keys())

    async def stat(self,session, path, **kwargs):
        res = await self.api_get(endpoint="files/stat", session=session, arg=path, **kwargs)
        self._raise_not_found_for_status(res, path)
        return await res.json()


    async def dag_get(self, session, path, **kwargs):
        path = await self.resolve_mfs_path(session=session, path=path)
        res =  await self.api_get(endpoint="dag/get", session=session, arg=path, **kwargs)
        return res

    async def file_info(self,session, path):
        '''
        Resolves the file info for hash or mfs path:
        
        params:
            session: aiohttp session
            path: cid or mfs path. Ifs mfs path then resolves to cid. 

        
        '''
        info = {"name": path}
        path = await self.resolve_mfs_path(session=session, path=path)

        headers = {"Accept-Encoding": "identity"}  # this ensures correct file size
        res = await self.cid_head(session=session, path=path, headers=headers)
        async with res:
            self._raise_not_found_for_status(res, path)
            if res.status != 200:
                # TODO: maybe handle 301 here
                raise FileNotFoundError(path)
            if "Content-Length" in res.headers:
                info["size"] = int(res.headers["Content-Length"])
            elif "Content-Range" in res.headers:
                info["size"] = int(res.headers["Content-Range"].split("/")[1])
            if "ETag" in res.headers:
                etag = res.headers["ETag"].strip("\"")
                info["ETag"] = etag
                if etag.startswith("DirIndex"):
                    info["type"] = "directory"
                    info["CID"] = etag.split("-")[-1]
                else:
                    info["type"] = "file"
                    info["CID"] = etag

        return info

    async def resolve_mfs_path(self, session, path):
        # converts mfs to cid if it exists. If it doesnt, then it returns the original cid
        
        res = await self.api_post(endpoint='files/stat', session=session, arg=path)
        mfs_hash = (await res.json()).get('Hash')
        if mfs_hash:
            path = mfs_hash
        
        return path
    
    async def cat(self, session, path):
        
        path = await self.resolve_mfs_path(session=session, path=path)        
        res = await self.api_get(endpoint='cat', session=session, arg=path)

        async with res:
            self._raise_not_found_for_status(res, path)
            if res.status != 200:
                raise FileNotFoundError(path)
            return await res.read()

    async def add(self, session, path, **kwargs):
        res = await self.api_get(endpoint='add', session=session, path=path, **kwargs)
        async with res:
            self._raise_not_found_for_status(res, path)
            if res.status != 200:
                raise FileNotFoundError(path)
            return await res.read()

    
    async def pin(self, session, cid, recursive=False, progress=False, **kwargs):
        kwargs['params'] = kwargs.get('params', {})
        kwargs['params'] = dict(arg=cid, recursive= recursive,progress= progress)
        res = await self.api_post(endpoint='pin/add', session=session ,
                                          arg=cid, recursive= recursive,progress= progress, **kwargs)
        return bool(cid in pinned_cid_list)


    async def dag_get(self, session,  **kwargs):
        kwargs['params'] = kwargs.get('params', {})
        kwargs['params'] = dict(arg=cid, recursive= recursive,progress= progress)
        res = await self.api_post(endpoint='dag/get', session=session , **kwargs)
        return bool(cid in pinned_cid_list)



    async def cp(self, session,  **kwargs):
        
        res = await self.api_post(endpoint="files/cp", session=session, arg=kwargs['arg'])
        
        res= await res.json()
        return res

    async def ls(self,session, path, **kwargs):

        # if await self._in_mfs(session=session, path=path):

        res = await self.api_post(endpoint="files/ls", session=session, arg=path, long='true')
        
   
        resdata = await res.json()
        # self._raise_not_found_for_status(res, path)

     

        if resdata.get('Entries'):
            links = resdata["Entries"]
        else:
            if path[0] == '/':
                path = path[1:]
            res = await self.api_get(endpoint="ls", session=session, arg=path)
            

            # self._raise_not_found_for_status(res, path)
            resdata = await res.json()

            if resdata.get('Type') == 'error':
                return []
            links = resdata['Objects'][0]['Links']

        types = {1: "directory", 2: "file", 0: 'file'}
        if path[-1] != '/':
            path += '/'


        return [{
                    "name": path  + link["Name"],
                    "CID": link["Hash"],
                    "type": types[link["Type"]],
                    "size": link["Size"],
                } for link in links]

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError for 404s, otherwise uses raise_for_status.
        """
        
        if response.status == 404:
            raise FileNotFoundError(url)
        elif response.status == 400:
            raise FileNotFoundError(url)
        elif response.status == 500:

            raise FileNotFoundError( f'{url} ERROR ({response.status})')
        response.raise_for_status()


class AsyncIPFSGateway(AsyncIPFSGatewayBase):

    resolution = "path"

    def __init__(self, url=None, gateway_type='local'):

        if url == None:
            url = self.DEFAULT_GATEWAY_MAP[gateway_type]

        self.gateway_type = gateway_type
        self.url = url


    async def api_get(self,session, endpoint, **kwargs):
        headers = kwargs.pop('headers') if 'headers' in kwargs else {}
        params = kwargs['params'] if 'params' in kwargs else kwargs

        res = await session.get(self.url + "/api/v0/" + endpoint, params=params,headers=headers, trace_request_ctx={'gateway': self.url})
        self._raise_requests_too_quick(res)
        return res


    async def api_post(self, session,endpoint, **kwargs):


        data = kwargs.pop('data') if 'data'  in kwargs else {}
        headers = kwargs.pop('headers') if 'headers' in kwargs else {}
        params = kwargs['params'] if 'params' in kwargs else kwargs

        url = copy.copy(self.url)
        if self.gateway_type == 'local':
            url = url.replace('8080', '5001') 

        url = url + f"/api/v0/{endpoint}"
        res = await session.post(url, params=params, data= data , headers=headers , trace_request_ctx={'gateway': self.url})
        
        return res


    async def _cid_req(self, method, path, **kwargs):

        headers = kwargs.get('headers', {})
        if self.resolution == "path":
            res = await method(self.url + "/ipfs/" + path, trace_request_ctx={'gateway': self.url}, headers=headers)
        elif self.resolution == "subdomain":
            raise NotImplementedError("subdomain resolution is not yet implemented")
        else:
            raise NotImplementedError(f"'{self.resolution}' resolution is not known")
        # TODO: maybe handle 301 here
        self._raise_requests_too_quick(res)
        return res

    async def cid_head(self, session, path, headers, **kwargs):
        return await self._cid_req(session.head, path, headers=headers, **kwargs)

    async def cid_get(self, session, path,  **kwargs):
        return await self._cid_req(session.get, path, headers=headers, **kwargs)

    async def version(self, session):
        res = await self.api_get(session=session, endpoint="version", )
        res.raise_for_status()
        return await res.json()

    @staticmethod
    def _raise_requests_too_quick(response):
        if response.status == 429:
            if "retry-after" in response.headers:
                retry_after = int(response.headers["retry-after"])
            else:
                retry_after = None
            raise RequestsTooQuick(retry_after)


    
    async def get_links(self,session, lpath,rpath):
        root_struct = {}
        struct = {}
        
        rpath = await self.resolve_mfs_path(session=session, path=rpath)

        if self.gateway_type in ['infura', 'local']:
            res = await self.api_post(endpoint='dag/get',session=session, arg=rpath)
            
            links = (await res.json())

            res = await self.file_info(session=session, path=rpath)
            # print(links, 'links', lpath, rpath)
            for link in links:
                name = f'{lpath}/{link["Name"]}'
                hash_ = link['Hash']['/']
                if len(name.split('.')) == 1:
                    details = await self.get_links(session=session, rpath=hash_, lpath=name)
                else:
                    details = {'Hash': hash_}
                struct[name] = details
        else:
            raise TypeError ('`get` not supported on public gateways')
        root_struct[rpath] = struct
        return root_struct


    async def save_links(self, session, links):
        return await asyncio.gather(*[self.save_link(session=session, lpath=k,rpath=v)for k, v in links.items()])

    async def save_link(self, session, lpath,rpath):
        lpath_dir = os.path.dirname(lpath)
        
        if len(lpath.split('.')) < 2:
            if not os.path.isdir(lpath_dir): 
                os.mkdir(lpath_dir)
            await self.save_links(lpath=lpath, rpath= rpath)
        else:
            data = await self.cat_file(links[lpath]['Hash'])
            with open(k, 'wb') as f:
                f.write(data.encode('utf-8'))    



    def __str__(self):
        return f"GW({self.url})"


class GatewayState:
    def __init__(self):
        self.reachable = True
        self.next_request_time = 0
        self.backoff_time = 0
        self.start_backoff = 1e-5
        self.max_backoff = 5

    def schedule_next(self):
        self.next_request_time = time.monotonic() + self.backoff_time

    def backoff(self):
        if self.backoff_time < self.start_backoff:
            self.backoff_time = self.start_backoff
        else:
            self.backoff_time *= 2
        self.reachable = True
        self.schedule_next()

    def speedup(self, not_below=0):
        did_speed_up = False
        if self.backoff_time > not_below:
            self.backoff_time *= 0.9
            did_speed_up = True
        self.reachable = True
        self.schedule_next()
        return did_speed_up

    def broken(self):
        self.backoff_time = self.max_backoff
        self.reachable = False
        self.schedule_next()

    def trying_to_reach(self):
        self.next_request_time = time.monotonic() + 1


class MultiGateway(AsyncIPFSGatewayBase):
    def __init__(self, gws, max_backoff_rounds=40):
        self.gws = [(GatewayState(), gw) for gw in gws]
        self.max_backoff_rounds = max_backoff_rounds

    @property
    def _gws_in_priority_order(self):
        now = time.monotonic()
        return sorted(self.gws, key=lambda x: max(now, x[0].next_request_time))

    async def _gw_op(self, op):
        for _ in range(self.max_backoff_rounds):
            for state, gw in self._gws_in_priority_order:
                not_before = state.next_request_time
                if not state.reachable:
                    state.trying_to_reach()
                else:
                    state.schedule_next()
                now = time.monotonic()
                if not_before > now:
                    await asyncio.sleep(not_before - now)
                logger.debug("tring %s", gw)
                try:
                    res = await op(gw)
                    if state.speedup(time.monotonic() - now):
                        logger.debug("%s speedup", gw)
                    return res
                except FileNotFoundError:  # early exit if object doesn't exist
                    raise
                except (RequestsTooQuick, aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                    state.backoff()
                    logger.debug("%s backoff %s", gw, e)
                    break
                except IOError as e:
                    exception = e
                    state.broken()
                    logger.debug("%s broken", gw)
                    continue
            else:
                raise exception
        raise RequestsTooQuick()

    async def api_get(self,session, endpoint, **kwargs):
        return await self._gw_op(lambda gw: gw.api_get(session=session,endpoint=endpoint, **kwargs))

    async def api_post(self, session,endpoint, **kwargs):
        return await self._gw_op(lambda gw: gw.api_post(session=session,endpoint=endpoint, **kwargs))

    async def cid_head(self, session,  **kwargs):
        return await self._gw_op(lambda gw: gw.cid_head(session=session, **kwargs))

    async def cid_get(self, session,  **kwargs):
        return await self._gw_op(lambda gw: gw.cid_get(session=session, path=path, **kwargs))

    async def cat(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.cat(session=session, **kwargs))

    async def ls(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.ls(session=session, **kwargs))

    async def add(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.add(session=session, **kwargs))

    async def dag_get(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.dag_get(session=session, **kwargs))


    async def cp(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.cp(session=session,**kwargs))
    async def get_links(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.get_links(session=session,**kwargs))

    async def save_links(self, session, **kwargs):
        return await self._gw_op(lambda gw: gw.save_links(session=session,**kwargs))


    def state_report(self):
        return "\n".join(f"{s.next_request_time}, {gw}" for s, gw in self.gws)

    def __str__(self):
        return "Multi-GW(" + ", ".join(str(gw) for _, gw in self.gws) + ")"

    @classmethod
    def get_gateway(cls, gateway_type=['local']):
        if isinstance(gateway_type, str):
            gateway_type = [gateway_type]
        return cls([AsyncIPFSGateway(gateway_type=gwt) for gwt in gateway_type])
