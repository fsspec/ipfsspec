from .utils import  parse_error_message, parse_response
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
import requests
from requests.exceptions import HTTPError
import hashlib
import functools
import time
import os 

import logging

from ipfshttpclient.multipart import stream_directory, stream_files #needed to prepare files/directory to be sent through http

logger = logging.getLogger("ipfsspec")

MAX_RETRIES = 2

class IPFSGateway:
    def __init__(self, url):
        self.url = url
        if self.url in ['http://127.0.0.1:5001', 'https://ipfs.infura.io:5001']:
            self.reqtype = 'post'
        else:
            self.reqtype = 'get'

        self.state = "unknown"
        self.min_backoff = 1e-9
        self.max_backoff = 5
        self.backoff_time = 0
        self.next_request_time = time.monotonic()
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)


    def get(self, path):
        logger.debug("get %s via %s", path, self.url)
        try:
            res = self.session.get(self.url + "/ipfs/" + path)
        except requests.ConnectionError as e:
            logger.debug("Connection Error: %s", e)
            self._backoff()
            return None
        # this is from https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
        expected_length = res.headers.get('Content-Length')
        if expected_length is not None:
            actual_length = res.raw.tell()
            expected_length = int(expected_length)
            if actual_length < expected_length:
                # if less than the expected amount of data is delivered, just backoff which will will eiter trigger a
                # retry on the same server or will fall back to another server later on.
                logger.debug("received size of resource %s is %d, but %d was expected", path, actual_length, expected_length)
                self._backoff()
                return None

        if res.status_code == 429:  # too many requests
            self._backoff()
            return None
        elif res.status_code == 200:
            self._speedup()
        # res.raise_for_status() # moving the exception to filesystem level 
        return res

    def head(self, path, headers=None):
        logger.debug("head %s via %s", path, self.url, headers=headers or {})
        try:
            res = self.session.get(self.url + "/ipfs/" + path)
        except requests.ConnectionError as e:
            logger.debug("Connection Error: %s", e)
            self._backoff()
            return None
        if res.status_code == 429:  # too many requests
            self._backoff()
            return None
        elif res.status_code == 200:
            self._speedup()
        res.raise_for_status()
        return res.headers

    def apipost(self, call, **kwargs):
        logger.debug("post %s via %s", call, self.url)
        if 'data' in kwargs.keys():
            data = kwargs.pop('data')
        else: data = None

        if 'headers' in kwargs.keys():
            headers = kwargs.pop('headers')
        else: headers = None
            
        try:
            if data is not None:
                res = self.session.post(self.url + "/api/v0/" + call, params=kwargs, data=data, headers=headers)
            else:
                res = self.session.post(self.url + "/api/v0/" + call, params=kwargs)
        
        except requests.ConnectionError:
            self._backoff()
            return None
        
        if res.status_code == 429:  # too many requests
            self._backoff()
            return None
        
        elif res.status_code == 200:
            self._speedup()
        
#         res.raise_for_status()

        return res
    def _schedule_next(self):
        self.next_request_time = time.monotonic() + self.backoff_time

    def _backoff(self):
        self.backoff_time = min(max(self.min_backoff, self.backoff_time) * 2,
                                self.max_backoff)
        logger.debug("%s: backing off -> %f sec", self.url, self.backoff_time)
        self._schedule_next()

    def _speedup(self):
        self.backoff_time = max(self.min_backoff, self.backoff_time * 0.9)
        logger.debug("%s: speeding up -> %f sec", self.url, self.backoff_time)
        self._schedule_next()

    def _init_state(self):
        try:
            if self.reqtype == 'get':
                res = self.session.get(self.url + "/api/v0/version")
            else:
                res = self.session.post(self.url + "/api/v0/version")      
            if res.ok:
                self.state = "online"
            else:
                self.state = "offline"
        except requests.ConnectionError:
            self.state = "offline"
    def get_state(self):
        if self.state == "unknown":
            self._init_state()
        now = time.monotonic()
        if self.next_request_time > now:
            return ("backoff", self.next_request_time - now)
        else:
            return (self.state, None)


class IPFSFileSystem(AbstractFileSystem):
    protocol = "ipfs"

    def __init__(self, 
        *args, 
        gateway_type:str='public', # Can be 'local', 'infura', 'public' 
        gateways=None, 
        timeout=10, 
        **kwargs
    ):
        super(IPFSFileSystem, self).__init__(*args, **kwargs)
        
        self.gateway_type = gateway_type
        
        if gateway_type == 'local':
            self._gateways = [IPFSGateway('http://127.0.0.1:5001')]
        
        elif gateway_type == 'infura':
            self._gateways = [IPFSGateway('https://ipfs.infura.io:5001')]
        
        else:
            gateways = gateways or get_default_gateways()
            self._gateways = [IPFSGateway(g) for g in gateways]
        
        self.timeout = timeout

    def _find_gateway(self):
        backoff_list = []
        for gw in self._gateways:
            state, wait_time = gw.get_state()
            if state == "online":
                return gw, 0
            if state == "backoff":
                backoff_list.append((wait_time, gw))
        if len(backoff_list) > 0:
            return sorted(backoff_list)[0][::-1]
        else:
            raise RuntimeError("no working gateways could be found")

    def _run_on_any_gateway(self, f):
        timeout = time.monotonic() + self.timeout
        while time.monotonic() <= timeout:
            gw, wait_time = self._find_gateway()
            if wait_time > 0:
                time.sleep(wait_time)
            res = f(gw)
            if res is not None:
                break
        return res

    def _gw_get(self, path):
        return self._run_on_any_gateway(lambda gw: gw.get(path))

    def _gw_head(self, path, headers=None):
        return self._run_on_any_gateway(lambda gw: gw.head(path, headers))

    def _gw_apipost(self, call, **kwargs):
        return self._run_on_any_gateway(lambda gw: gw.apipost(call, **kwargs))

    def ls(self, path, detail=True, **kwargs):
        logger.debug("ls on %s", path)
        res = self._gw_apipost("ls", arg=path)
        if res.status_code == 200:
            links = parse_response(res)[0]["Objects"][0]["Links"]
            types = {1: "directory", 2: "file"}
            if detail:
                return [{"name": path + "/" + link["Name"],
                         "size": link["Size"],
                         "type": types[link["Type"]]}
                        for link in links]
            else:
                return [path + "/" + link["Name"]
                        for link in links]
        else:
            raise HTTPError (parse_error_message(res)) 

    @functools.lru_cache()
    def cat_file(self, path):
        logger.debug("cat on %s", path)
        if self.gateway_type == 'public':
            res = self._gw_get(path)        
            if res.status_code == 200:
                return parse_response(res)
            else:
                raise HTTPError (parse_error_message(res))
        else:
            res = self._gw_apipost(call='cat', arg=path)         
            if res.status_code == 200:
                return parse_response(res)
            else:
                raise HTTPError (parse_error_message(res))                
        return parse_response(data)

    def cat(self, path, detail=True):        
        res = self._gw_apipost('cat', arg=path)
        if res.status_code == 200:
            return parse_response(res)       
        elif res.status_code == 500:
            res = self._gw_apipost("get", arg=path)
            return parse_response(res)
        else:
            raise HTTPError (parse_error_message(res))

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return IPFSBufferedFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs
        )

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        logger.debug("info on %s", path)

        headers = {"Accept-Encoding": "identity"}  # this ensures correct file size
        response_headers = self._gw_head(path, headers)

        info = {"name": path}
        if "Content-Length" in response_headers:
            info["size"] = int(response_headers["Content-Length"])
        elif "Content-Range" in response_headers:
            info["size"] = int(response_headers["Content-Range"].split("/")[1])

        if "ETag" in response_headers:
            etag = response_headers["ETag"].strip("\"")
            info["ETag"] = etag
            if etag.startswith("DirIndex"):
                info["type"] = "directory"
                info["CID"] = etag.split("-")[-1]
            else:
                info["type"] = "file"
                info["CID"] = etag
        return info

    def _get_links(self,
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
                    details = self._get_links(hash_, name)
                else:
                    details = {'Hash': hash_}
                struct[name] = details
        elif self.gateway_type == 'infura':
            res = self._gw_apipost('dag/get', arg=path)
            links = res.json()['Links']
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

    def _save_links(self,
        links
    ):
        for k, v in links.items():
            if len(k.split('.')) < 2:
                if not os.path.exists(k): os.mkdir(k)
                self._save_links(v)

            else:
                data = self.cat_file(links[k]['Hash'])

                with open(k, 'wb') as f:
                    f.write(data.encode('utf-8'))    

    def get(self,
        rpath,
        lpath=None,
        **kwargs
    ):
        
        if lpath is None: lpath = os.getcwd()
            
        self.full_structure = self._get_links(rpath, lpath)
        self._save_links(self.full_structure)
            
    def put_file(self,
        path, 
        pin=True,
        chunker=262144, 
        wrap_with_directory=False,
        **kwargs
    ):
        if not os.path.isfile(path): raise TypeError ('Use `put` to upload a directory')        
        if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        params = {}
        params['wrap-with-directory'] = 'true' if wrap_with_directory else 'false'
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        params.update(kwargs)
        
        data, headers = stream_files(path, chunk_size=chunker)
                                                      
        res = self._gw_apipost('add', params=params, data=data, headers=headers)
        
        return parse_response(res) 
    
    def put(self,
        path, 
        recursive=True,
        pin=True,
        chunker=262144, 
        **kwargs
    ):
        
        if self.gateway_type == 'public': raise TypeError ('`put_file` and `put` functions require local/infura `gateway_type`')
        
        params = {}
        params['chunker'] = f'size-{chunker}'
        params['pin'] = 'true' if pin else 'false'
        params.update(kwargs)
        
        data, headers = stream_directory(path, chunk_size=chunker, recursive=recursive)                                              
        res = self._gw_apipost('add', params=params, data=data, headers=headers)
        
        return parse_response(res)

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