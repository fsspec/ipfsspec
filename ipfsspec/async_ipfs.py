import io
import os
import platform
import weakref
from functools import lru_cache
from pathlib import Path
import warnings

import asyncio
import aiohttp
import aiohttp_retry

from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.exceptions import FSTimeoutError

from multiformats import CID, multicodec
from . import unixfsv1

import logging

logger = logging.getLogger("ipfsspec")

DagPbCodec = multicodec.get("dag-pb")
RawCodec = multicodec.get("raw")

class RequestsTooQuick(OSError):
    def __init__(self, retry_after=None):
        self.retry_after = retry_after


class AsyncIPFSGateway:
    resolution = "path"

    def __init__(self, url, protocol="ipfs"):
        self.url = url
        self.protocol = protocol

    async def _cid_req(self, method, path, headers=None, **kwargs):
        headers = headers or {}
        if self.resolution == "path":
            res = await method("/".join((self.url, self.protocol,  path)), trace_request_ctx={'gateway': self.url}, headers=headers, **kwargs)
        elif self.resolution == "subdomain":
            raise NotImplementedError("subdomain resolution is not yet implemented")
        else:
            raise NotImplementedError(f"'{self.resolution}' resolution is not known")
        # TODO: maybe handle 301 here
        self._raise_requests_too_quick(res)
        return res

    async def head(self, path, session, headers=None, **kwargs):
        return await self._cid_req(session.head, path, headers=headers, **kwargs)

    async def get(self, path, session, headers=None, **kwargs):
        return await self._cid_req(session.get, path, headers=headers, **kwargs)

    @staticmethod
    def _raise_requests_too_quick(response):
        if response.status == 429:
            if "retry-after" in response.headers:
                retry_after = int(response.headers["retry-after"])
            else:
                retry_after = None
            raise RequestsTooQuick(retry_after)

    def __str__(self):
        return f"GW({self.url})"

    async def info(self, path, session):
        res = await self.get(path, session, headers={"Accept": "application/vnd.ipld.raw"}, params={"format": "raw"})
        self._raise_not_found_for_status(res, path)
        cid = CID.decode(res.headers["X-Ipfs-Roots"].split(",")[-1])
        resdata = await res.read()

        if cid.codec == RawCodec:
            return {
                "name": path,
                "CID": str(cid),
                "type": "file",
                "size": len(resdata),
            }
        elif cid.codec == DagPbCodec:
            node = unixfsv1.PBNode.loads(resdata)
            data = unixfsv1.Data.loads(node.Data)
            if data.Type == unixfsv1.DataType.Raw:
                raise FileNotFoundError(path)  # this is not a file, it's only a part of it
            elif data.Type == unixfsv1.DataType.Directory:
                return {
                    "name": path,
                    "CID": str(cid),
                    "type": "directory",
                    "islink": False,
                }
            elif data.Type == unixfsv1.DataType.File:
                return {
                    "name": path,
                    "CID": str(cid),
                    "type": "file",
                    "size": data.filesize,
                    "islink": False,
                }
            elif data.Type == unixfsv1.DataType.Metadata:
                raise NotImplementedError(f"The path '{path}' contains a Metadata node, this is currently not implemented")
            elif data.Type == unixfsv1.DataType.Symlink:
                return {
                    "name": path,
                    "CID": str(cid),
                    "type": "other",  # TODO: maybe we should have directory or file as returning type, but that probably would require resolving at least another level of blocks
                    "islink": True,
                }
            elif data.Type == unixfsv1.DataType.HAMTShard:
                raise NotImplementedError(f"The path '{path}' contains a HAMTSharded directory, this is currently not implemented")
        else:
            raise FileNotFoundError(path)  # it exists, but is not a UNIXFSv1 object, so it's not a file

    async def cat(self, path, session):
        res = await self.get(path, session)
        async with res:
            self._raise_not_found_for_status(res, path)
            return await res.read()

    async def ls(self, path, session, detail=False):
        res = await self.get(path, session, headers={"Accept": "application/vnd.ipld.raw"}, params={"format": "raw"})
        self._raise_not_found_for_status(res, path)
        resdata = await res.read()
        cid = CID.decode(res.headers["X-Ipfs-Roots"].split(",")[-1])
        assert cid.codec == DagPbCodec, "this is not a directory"
        node = unixfsv1.PBNode.loads(resdata)
        data = unixfsv1.Data.loads(node.Data)
        if data.Type != unixfsv1.DataType.Directory:
            # TODO: we might need support for HAMTShard here (for large directories)
            raise NotADirectoryError(path)

        if detail:
            return await asyncio.gather(*(
                self.info(path + "/" + link.Name, session)
                for link in node.Links))
        else:
            return [path + "/" + link.Name for link in node.Links]

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError for 404s, otherwise uses raise_for_status.
        """
        if response.status == 404:  # returned for known missing files
            raise FileNotFoundError(url)
        elif response.status == 400:  # return for invalid requests, so it's also certainly not there
            raise FileNotFoundError(url)
        response.raise_for_status()


async def get_client(**kwargs):
    retry_options = aiohttp_retry.ExponentialRetry(
            attempts=5,
            exceptions={OSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError})
    retry_client = aiohttp_retry.RetryClient(raise_for_status=False, retry_options=retry_options)
    return retry_client


def gateway_from_file(gateway_path, protocol="ipfs"):
    if gateway_path.exists():
        with open(gateway_path) as gw_file:
            ipfs_gateway = gw_file.readline().strip()
            logger.debug("using IPFS gateway from %s: %s", gateway_path, ipfs_gateway)
            return AsyncIPFSGateway(ipfs_gateway, protocol=protocol)
    return None


@lru_cache
def get_gateway(protocol="ipfs"):
    """
    Get IPFS gateway according to IPIP-280

    see: https://github.com/ipfs/specs/pull/280
    """

    # IPFS_GATEWAY environment variable should override everything
    ipfs_gateway = os.environ.get("IPFS_GATEWAY", "")
    if ipfs_gateway:
        logger.debug("using IPFS gateway from IPFS_GATEWAY environment variable: %s", ipfs_gateway)
        return AsyncIPFSGateway(ipfs_gateway, protocol)

    # internal configuration: accept IPFSSPEC_GATEWAYS for backwards compatibility
    if ipfsspec_gateways := os.environ.get("IPFSSPEC_GATEWAYS", ""):
        ipfs_gateway = ipfsspec_gateways.split()[0]
        logger.debug("using IPFS gateway from IPFSSPEC_GATEWAYS environment variable: %s", ipfs_gateway)
        warnings.warn("The IPFSSPEC_GATEWAYS environment variable is deprecated, please configure your IPFS Gateway according to IPIP-280, e.g. by using the IPFS_GATEWAY environment variable or using the ~/.ipfs/gateway file.", DeprecationWarning)
        return AsyncIPFSGateway(ipfs_gateway, protocol)

    # check various well-known files for possible gateway configurations
    if ipfs_path := os.environ.get("IPFS_PATH", ""):
        if ipfs_gateway := gateway_from_file(Path(ipfs_path) / "gateway", protocol):
            return ipfs_gateway

    if home := os.environ.get("HOME", ""):
        if ipfs_gateway := gateway_from_file(Path(home) / ".ipfs" / "gateway", protocol):
            return ipfs_gateway

    if config_home := os.environ.get("XDG_CONFIG_HOME", ""):
        if ipfs_gateway := gateway_from_file(Path(config_home) / "ipfs" / "gateway", protocol):
            return ipfs_gateway

    if ipfs_gateway := gateway_from_file(Path("/etc") / "ipfs" / "gateway", protocol):
        return ipfs_gateway

    system = platform.system()

    if system == "Windows":
        candidates = [
            Path(os.environ.get("LOCALAPPDATA")) / "ipfs" / "gateway",
            Path(os.environ.get("APPDATA")) / "ipfs" / "gateway",
            Path(os.environ.get("PROGRAMDATA")) / "ipfs" / "gateway",
        ]
    elif system == "Darwin":
        candidates = [
            Path(os.environ.get("HOME")) / "Library" / "Application Support" / "ipfs" / "gateway",
            Path("/Library") / "Application Support" / "ipfs" / "gateway",
        ]
    elif system == "Linux":
        candidates = [
            Path(os.environ.get("HOME")) / ".config" / "ipfs" / "gateway",
            Path("/etc") / "ipfs" / "gateway",
        ]
    else:
        candidates = []

    for candidate in candidates:
        if ipfs_gateway := gateway_from_file(candidate, protocol):
            return ipfs_gateway

    # if we reach this point, no gateway is configured
    raise RuntimeError("IPFS Gateway could not be found!\n"
                       "In order to access IPFS, you must configure an "
                       "IPFS Gateway using a IPIP-280 configuration method. "
                       "Possible options are: \n"
                       "  * set the environment variable IPFS_GATEWAY\n"
                       "  * write a gateway in the first line of the file ~/.ipfs/gateway\n"
                       "\n"
                       "It's always best to run your own IPFS gateway, e.g. by using "
                       "IPFS Desktop (https://docs.ipfs.tech/install/ipfs-desktop/) or "
                       "the command line version Kubo (https://docs.ipfs.tech/install/command-line/). "
                       "If you can't run your own gateway, you may also try using the "
                       "public IPFS gateway at https://ipfs.io or https://dweb.link . "
                       "However, this is not recommended for productive use and you may experience "
                       "severe performance issues.")


class AsyncIPFSFileSystem(AsyncFileSystem):
    sep = "/"
    protocol = "ipfs"

    def __init__(self, asynchronous=False, loop=None, client_kwargs=None, **storage_options):
        super().__init__(self, asynchronous=asynchronous, loop=loop, **storage_options)
        self._session = None

        self.client_kwargs = client_kwargs or {}
        self.get_client = get_client

        if not asynchronous:
            sync(self.loop, self.set_session)

    @property
    def gateway(self):
        return get_gateway(self.protocol)

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

    async def _ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return await self.gateway.ls(path, session, detail=detail)

    ls = sync_wrapper(_ls)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return (await self.gateway.cat(path, session))[start:end]

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return await self.gateway.info(path, session)

    def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        if mode != "rb":
            raise NotImplementedError("opening modes other than read binary are not implemented")
        data = self.cat_file(path)  # load whole chunk into memory
        return io.BytesIO(data)

    def ukey(self, path):
        """returns the CID, which is by definition an unchanging identitifer"""
        return self.info(path)["CID"]


class AsyncIPNSFileSystem(AsyncIPFSFileSystem):
    protocol = "ipns"
