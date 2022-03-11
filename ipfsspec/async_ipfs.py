import io
import time
import weakref

import asyncio
import aiohttp

from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.exceptions import FSTimeoutError

from .core import get_default_gateways

import logging

logger = logging.getLogger("ipfsspec")


class RequestsTooQuick(OSError):
    def __init__(self, retry_after=None):
        self.retry_after = retry_after


class AsyncIPFSGatewayBase:
    async def stat(self, path, session):
        res = await self.api_get("files/stat", session, arg=path)
        self._raise_not_found_for_status(res, path)
        return await res.json()

    async def file_info(self, path, session):
        info = {"name": path}

        headers = {"Accept-Encoding": "identity"}  # this ensures correct file size
        res = await self.cid_head(path, session, headers=headers)

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

    async def cat(self, path, session):
        res = await self.cid_get(path, session)
        async with res:
            self._raise_not_found_for_status(res, path)
            if res.status != 200:
                raise FileNotFoundError(path)
            return await res.read()

    async def ls(self, path, session):
        res = await self.api_get("ls", session, arg=path)
        self._raise_not_found_for_status(res, path)
        resdata = await res.json()
        types = {1: "directory", 2: "file"}
        return [{
                    "name": path + "/" + link["Name"],
                    "CID": link["Hash"],
                    "type": types[link["Type"]],
                    "size": link["Size"],
                }
                for link in resdata["Objects"][0]["Links"]]

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError for 404s, otherwise uses raise_for_status.
        """
        if response.status == 404:
            raise FileNotFoundError(url)
        elif response.status == 400:
            raise FileNotFoundError(url)
        response.raise_for_status()


class AsyncIPFSGateway(AsyncIPFSGatewayBase):
    resolution = "path"

    def __init__(self, url):
        self.url = url

    async def api_get(self, endpoint, session, **kwargs):
        res = await session.get(self.url + "/api/v0/" + endpoint, params=kwargs, trace_request_ctx={'gateway': self.url})
        self._raise_requests_too_quick(res)
        return res

    async def api_post(self, endpoint, session, **kwargs):
        res = await session.post(self.url + "/api/v0/" + endpoint, params=kwargs, trace_request_ctx={'gateway': self.url})
        self._raise_requests_too_quick(res)
        return res

    async def _cid_req(self, method, path, headers=None, **kwargs):
        headers = headers or {}
        if self.resolution == "path":
            res = await method(self.url + "/ipfs/" + path, trace_request_ctx={'gateway': self.url}, headers=headers)
        elif self.resolution == "subdomain":
            raise NotImplementedError("subdomain resolution is not yet implemented")
        else:
            raise NotImplementedError(f"'{self.resolution}' resolution is not known")
        # TODO: maybe handle 301 here
        self._raise_requests_too_quick(res)
        return res

    async def cid_head(self, path, session, headers=None, **kwargs):
        return await self._cid_req(session.head, path, headers=headers, **kwargs)

    async def cid_get(self, path, session, headers=None, **kwargs):
        return await self._cid_req(session.get, path, headers=headers, **kwargs)

    async def version(self, session):
        res = await self.api_get("version", session)
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
    def __init__(self, gws, max_backoff_rounds=50):
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

    async def api_get(self, endpoint, session, **kwargs):
        return await self._gw_op(lambda gw: gw.api_get(endpoint, session, **kwargs))

    async def api_post(self, endpoint, session, **kwargs):
        return await self._gw_op(lambda gw: gw.api_post(endpoint, session, **kwargs))

    async def cid_head(self, path, session, headers=None, **kwargs):
        return await self._gw_op(lambda gw: gw.cid_head(path, session, headers=headers, **kwargs))

    async def cid_get(self, path, session, headers=None, **kwargs):
        return await self._gw_op(lambda gw: gw.cid_get(path, session, headers=headers, **kwargs))

    async def cat(self, path, session):
        return await self._gw_op(lambda gw: gw.cat(path, session))

    async def ls(self, path, session):
        return await self._gw_op(lambda gw: gw.ls(path, session))

    def state_report(self):
        return "\n".join(f"{s.next_request_time}, {gw}" for s, gw in self.gws)

    def __str__(self):
        return "Multi-GW(" + ", ".join(str(gw) for _, gw in self.gws) + ")"


async def get_client(**kwargs):
    timeout = aiohttp.ClientTimeout(sock_connect=1, sock_read=5)
    kwargs = {"timeout": timeout, **kwargs}
    return aiohttp.ClientSession(**kwargs)


DEFAULT_GATEWAY = None


def get_gateway():
    global DEFAULT_GATEWAY
    if DEFAULT_GATEWAY is None:
        use_gateway(*get_default_gateways())
    return DEFAULT_GATEWAY


def use_gateway(*urls):
    global DEFAULT_GATEWAY
    if len(urls) == 1:
        DEFAULT_GATEWAY = AsyncIPFSGateway(urls[0])
    else:
        DEFAULT_GATEWAY = MultiGateway([AsyncIPFSGateway(url) for url in urls])


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
        return get_gateway()

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
        res = await self.gateway.ls(path, session)
        if detail:
            return res
        else:
            return [r["name"] for r in res]

    ls = sync_wrapper(_ls)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return (await self.gateway.cat(path, session))[start:end]

    async def _info(self, path, **kwargs):
        path = self._strip_protocol(path)
        session = await self.set_session()
        return await self.gateway.file_info(path, session)

    def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        if mode != "rb":
            raise NotImplementedError
        data = self.cat_file(path)  # load whole chunk into memory
        return io.BytesIO(data)

    def ukey(self, path):
        """returns the CID, which is by definition an unchanging identitifer"""
        return self.info(path)["CID"]
