from .utils import get_default_gateways

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
import requests
from requests.exceptions import HTTPError
import hashlib
import functools
import time

import logging

logger = logging.getLogger("ipfsspec")

MAX_RETRIES = 2


class IPFSGateway:
    """
    Abstrats the IPFS gateway REST API.

    Documentation for the REST API can be found here: [Kubo_RPC_API]_.

    References:
        .. [Kubo_RPC_API] http://docs.ipfs.tech.ipns.localhost:8080/reference/kubo/rpc/
    """
    def __init__(self, url):
        self.url = url
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
        except IOError as e:
            logger.debug("IOError: %s", e)
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
        res.raise_for_status()
        return res.content

    def head(self, path, headers=None):
        logger.debug("head %s via %s", path, self.url, headers=headers or {})
        try:
            res = self.session.get(self.url + "/ipfs/" + path)
        except IOError as e:
            logger.debug("IOError: %s", e)
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
        try:
            res = self.session.post(self.url + "/api/v0/" + call, params=kwargs)
        except IOError:
            self._backoff()
            return None
        if res.status_code == 429:  # too many requests
            self._backoff()
            return None
        elif res.status_code == 200:
            self._speedup()
        res.raise_for_status()
        return res.json()

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
            res = self.session.get(self.url + "/api/v0/version")
            if res.ok:
                self.state = "online"
            else:
                self.state = "offline"
        except IOError:
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

    def __init__(self, *args, gateways=None, timeout=10, **kwargs):
        super(IPFSFileSystem, self).__init__(*args, **kwargs)
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
        links = res["Objects"][0]["Links"]

        # Documentation for the type codes doesn't seem to have a great location.
        # Currently 2023-10-17, the ls API docs specify the ls return
        # structure, but not what the type integers mean:
        # http://docs.ipfs.tech/reference/kubo/rpc/#api-v0-ls
        # Some information about type codes can be found here:
        # https://ipfs-search.readthedocs.io/en/latest/ipfs_datatypes.html
        # https://github.com/ipfs/go-unixfs/blob/master/pb/unixfs.proto

        types = {
            0: "raw",
            1: "directory",
            2: "file",
            3: "metadata",
            4: "symlink",
            5: "shard",
        }
        if detail:
            return [{"name": path + "/" + link["Name"],
                     "size": link["Size"],
                     "type": types[link["Type"]]}
                    for link in links]
        else:
            return [path + "/" + link["Name"]
                    for link in links]

    @functools.lru_cache()
    def cat_file(self, path):
        logger.debug("cat on %s", path)
        try:
            data = self._gw_get(path)
        except HTTPError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(e.response.text)
            else:
                raise e
        if logger.isEnabledFor(logging.DEBUG):
            h = hashlib.sha256(data).hexdigest()
            logger.debug("sha256 of received resouce at %s: %s", path, h)
        return data

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
