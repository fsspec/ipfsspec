from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
import requests
import hashlib

import logging

logger = logging.getLogger("ipfsspec")

MAX_RETRIES = 2


class IPFSGateway:
    def __init__(self, url):
        self.url = url

    def get(self, path):
        res = requests.get(self.url + "/ipfs/" + path)
        res.raise_for_status()
        return res.content

    def apipost(self, call, **kwargs):
        res = requests.post(self.url + "/api/v0/" + call, params=kwargs)
        res.raise_for_status()
        return res.json()

    def is_available(self):
        try:
            res = requests.get(self.url + "/api/v0/version")
            return res.ok
        except requests.exceptions.ConnectionError:
            return False


GATEWAYS = [
    IPFSGateway("http://localhost:8080"),
    IPFSGateway("https://ipfs.io"),
    IPFSGateway("https://gateway.pinata.cloud"),
    IPFSGateway("https://dweb.link"),
]


class IPFSFileSystem(AbstractFileSystem):
    protocol = "ipfs"

    def __init__(self, *args, **kwargs):
        super(IPFSFileSystem, self).__init__(*args, **kwargs)
        self._bad_gateways = []
        self._select_gateway()

    def _select_gateway(self):
        for gw in GATEWAYS:
            if gw.is_available():
                self._gateway = gw
                break
            else:
                self._bad_gateways.append(gw)
        else:
            raise RuntimeError("no available gateway found")
        logger.debug("using IPFS gateway at %s", self._gateway.url)

    def _switch_gateway(self):
        logger.debug("switching gateway")
        self._bad_gateways.append(self._gateway)
        for gw in GATEWAYS:
            if gw not in self._bad_gateways:
                if gw.is_available():
                    self._gateway = gw
                    break
                else:
                    self._bad_gateways.append(gw)
        else:
            raise RuntimeError("no available gateway found")
        logger.debug("using IPFS gateway at %s", self._gateway.url)

    def _run_on_any_gateway(self, f):
        i = 0
        while True:
            try:
                res = f()
                break
            except requests.ConnectionError:
                if i < MAX_RETRIES:
                    i += 1
                    self._switch_gateway()
                else:
                    raise
        self._bad_gateways = []
        return res

    def _gw_get(self, path):
        return self._run_on_any_gateway(lambda: self._gateway.get(path))

    def _gw_apipost(self, call, **kwargs):
        return self._run_on_any_gateway(lambda: self._gateway.apipost(call, **kwargs))

    def ls(self, path, detail=True, **kwargs):
        res = self._gw_apipost("ls", arg=path)
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

    def cat_file(self, path):
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
        res = self._gw_apipost("object/stat", arg=path)
        if res["NumLinks"] == 0:
            ftype = "file"
        else:
            ftype = "directory"
        return {"name": path, "size": res["DataSize"], "type": ftype}


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
