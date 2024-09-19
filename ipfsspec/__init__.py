from .async_ipfs import AsyncIPFSFileSystem, AsyncIPNSFileSystem
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("package-name")
except PackageNotFoundError:
    # package is not installed
    pass

__all__ = ["__version__", "AsyncIPFSFileSystem", "AsyncIPNSFileSystem"]
