from .unixfs_pb2 import Data as UnixFSData
import cid

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
import requests
from requests.exceptions import HTTPError
import hashlib
import base64
import functools
import time
import json
import os

import logging

logger = logging.getLogger("ipfsspec")

MAX_RETRIES = 2


class IPFSGateway:
    """
    CommandLine:
        xdoctest -m ipfsspec.core IPFSGateway

    Example:
        >>> # Load content from an IPFS gateway
        >>> from ipfsspec.core import *  # NOQA
        >>> import ubelt as ub
        >>> self = IPFSGateway('https://ipfs.io', protocol='ipfs')
        >>> content = self.get('QmWt2CjtbvSv7UbKAJ8QhxJkB3vydxWGL47G8cJ7kgLycP')
        >>> print('content = {!r}'.format(content))
        >>> res_json = self.apipost('ls', arg='QmWt2CjtbvSv7UbKAJ8QhxJkB3vydxWGL47G8cJ7kgLycP')
        >>> print('res_json = {}'.format(ub.repr2(res_json, nl=4)))
        >>> res_json = self.apipost('ls', arg='QmUgbNSRLuTDackyeNuT7T2DERpcaFzSKExgmuqXWhByoa')
        >>> print('res_json = {}'.format(ub.repr2(res_json, nl=4)))

    Example:
        >>> # Load content from an IPNS gateway
        >>> from ipfsspec.core import *  # NOQA
        >>> import ubelt as ub
        >>> self = IPFSGateway('https://ipfs.io', protocol='ipns')
        >>> content = self.get('dist.ipfs.io/go-ipfs/versions')
        >>> print('content = {!r}'.format(content))
        >>> res_json = self.apipost('ls', arg='/ipns/dist.ipfs.io/go-ipfs')
        >>> print('res_json = {}'.format(ub.repr2(res_json, nl=4)))
        >>> res_json = self.apipost('ls', arg='/ipns/dist.ipfs.io/go-ipfs/versions')
        >>> print('res_json = {}'.format(ub.repr2(res_json, nl=4)))
    """
    def __init__(self, url, protocol='ipfs'):
        self.url = url
        self.protocol = protocol
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
            res = self.session.get(f"{self.url}/{self.protocol}/{path}")
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
        res.raise_for_status()
        return res.content

    def apipost(self, call, **kwargs):
        logger.debug("post %s via %s", call, self.url)
        try:
            res = self.session.post(self.url + "/api/v0/" + call, params=kwargs)
        except requests.ConnectionError:
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


GATEWAYS = [
    "http://127.0.0.1:8080",
    "https://ipfs.io",
    "https://gateway.pinata.cloud",
    "https://cloudflare-ipfs.com",
    "https://dweb.link",
]


def get_default_gateways():
    try:
        return os.environ["IPFSSPEC_GATEWAYS"].split()
    except KeyError:
        return GATEWAYS


class IPFSFileSystem(AbstractFileSystem):
    """
    Core IPFS read-only implementation for addressing immutable IPFS CIDs

    CommandLine:
        xdoctest -m ipfsspec.core IPFSFileSystem

    Example:
        >>> import fsspec
        >>> with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
        >>>     print(f.read())
        >>> with fsspec.open("ipfs://QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx", "r") as f:
        >>>     print(f.read())
        >>> import fsspec
        >>> self_ipfs = fsspec.filesystem('ipfs')
        >>> print(self_ipfs.ls('QmZ4tDuvesekSs4qM5ZBKpXiZGun7S2CYtEZRB3DYXkjGx'))
        >>> print(self_ipfs.ls('QmUgbNSRLuTDackyeNuT7T2DERpcaFzSKExgmuqXWhByoa'))  # /ipns/dist.ipfs.io/go-ipfs
        >>> self_ipns = fsspec.filesystem('ipns')
        >>> print(self_ipns.ls('/ipns/dist.ipfs.io/go-ipfs'))  # /ipns/dist.ipfs.io/go-ipfs
    """
    protocol = "ipfs"

    def __init__(self, *args, gateways=None, timeout=10, **kwargs):
        super().__init__(*args, **kwargs)
        gateways = gateways or get_default_gateways()
        self._gateways = [IPFSGateway(g, protocol=self.protocol) for g in gateways]
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

    def _gw_apipost(self, call, **kwargs):
        return self._run_on_any_gateway(lambda gw: gw.apipost(call, **kwargs))

    def ls(self, path, detail=True, **kwargs):
        logger.debug("ls on %s", path)
        ipfs_ref = f'/{self.protocol}/{path}'
        res = self._gw_apipost("ls", arg=ipfs_ref)
        links = res["Objects"][0]["Links"]
        types = {1: "directory", 2: "file"}
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
        data = self._gw_get(path)
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
        logger.debug("info on %s", path)
        ipfs_ref = f'/{self.protocol}/{path}'

        def req(endpoint):
            try:
                return self._gw_apipost(endpoint, arg=ipfs_ref)
            except HTTPError as e:
                try:
                    msg = e.response.json()
                except json.JSONDecodeError:
                    raise IOError("unknown error") from e
                else:
                    if "Message" in msg:
                        raise FileNotFoundError(msg["Message"]) from e
                    else:
                        raise IOError(msg) from e

        stat = req("object/stat")
        c = cid.from_string(stat["Hash"])
        if c.codec == "raw":
            size = stat["DataSize"]
            ftype = "file"
        else:
            dag = req("dag/get")
            data = UnixFSData()
            if "data" in dag:
                data.ParseFromString(base64.b64decode(dag["data"]))
            else:
                rawdata = dag["Data"]["/"]["bytes"]
                data.ParseFromString(base64.b64decode(rawdata + "=" * (-len(rawdata) % 4)))

            size = data.filesize
            if data.Type == data.File:
                ftype = "file"
            else:
                ftype = "directory"

        return {"name": path, "size": size, "type": ftype}


class IPNSFileSystem(IPFSFileSystem):
    """
    Read (and maybe write) POC implementation for IPNS

    CommandLine:
        xdoctest -m ipfsspec.core IPNSFileSystem

    Example:
        >>> import ipfsspec
        >>> import fsspec
        >>> ipns_fs = fsspec.filesystem('ipns')
        >>> print(ipns_fs.resolve('dist.ipfs.io/go-ipfs/versions'))
        >>> print(ipns_fs.resolve('dist.ipfs.io/go-ipfs'))
        >>> print(ipns_fs.resolve('dist.ipfs.io'))
        >>> print(ipns_fs.ls('dist.ipfs.io/go-ipfs'))
        >>> print(ipns_fs.ls('dist.ipfs.io'))
        >>> with fsspec.open("ipns://dist.ipfs.io/go-ipfs/versions", "r") as f:
        >>>     print(f.read())
    """
    protocol = 'ipns'

    def resolve(self, path):
        """ Get the current CID for the ipns address """
        logger.debug("resolve on %s", path)
        ipfs_ref = f'/{self.protocol}/{path}'
        try:
            resolved = self._gw_apipost('dag/resolve', arg=ipfs_ref)
        except HTTPError as e:
            try:
                msg = e.response.json()
            except json.JSONDecodeError:
                raise IOError("unknown error") from e
            else:
                if "Message" in msg:
                    raise FileNotFoundError(msg["Message"]) from e
                else:
                    raise IOError(msg) from e
        cid = resolved['Cid']['/']
        return cid


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
