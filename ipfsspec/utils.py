import os

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
