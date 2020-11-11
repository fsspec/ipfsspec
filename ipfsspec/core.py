from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
import requests

import logging

logger = logging.getLogger("ipfsspec")


class IPFSGateway:
    def __init__(self, url):
        self.url = url

    def get(self, path):
        return requests.get(self.url + "/ipfs/" + path).content

    def apipost(self, call, **kwargs):
        res = requests.post(self.url + "/api/v0/" + call, params=kwargs)
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
]


class IPFSFileSystem(AbstractFileSystem):
    protocol = "ipfs"

    def __init__(self, *args, **kwargs):
        super(IPFSFileSystem, self).__init__(*args, **kwargs)
        for gw in GATEWAYS:
            if gw.is_available():
                self._gateway = gw
                break
        else:
            raise RuntimeError("no available gateway found")
        logger.debug("using IPFS gateway at %s", self._gateway.url)

    def ls(self, path, detail=True, **kwargs):
        res = self._gateway.apipost("ls", arg=path)
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
        return self._gateway.get(path)

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
        res = self._gateway.apipost("object/stat", arg=path)
        if res["NumLinks"] == 0:
            ftype = "file"
        else:
            ftype = "directory"
        return {"name": path, "size": res["DataSize"], "type": ftype}


class IPFSBufferedFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        content = self.fs.cat_file(self.path)[start:end]
        if "b" not in self.mode:
            return content.decode("utf-8")
        else:
            return content
