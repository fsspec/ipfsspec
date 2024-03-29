from .core import IPFSFileSystem
from .async_ipfs import AsyncIPFSFileSystem

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

__all__ = ["__version__", "IPFSFileSystem", "AsyncIPFSFileSystem"]
