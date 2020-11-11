from .core import IPFSFileSystem
from fsspec import register_implementation

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

register_implementation(IPFSFileSystem.protocol, IPFSFileSystem)

__all__ = ["__version__", "IPFSFileSystem"]
