from fsspec.spec import  AbstractBufferedFile
import io
from fsspec.core import get_compression


class IPFSBufferedFile(AbstractBufferedFile):
    def __init__(self, 
                fs,
                path,
                mode="rb",
                block_size="default",
                autocommit=True,
                cache_type="readahead",
                cache_options=None,
                size=None,
                pin=False,
                rpath=None,
                **kwargs):
        super(IPFSBufferedFile, self).__init__(
                fs,
                path,
                mode="rb",
                block_size="default",
                autocommit=True,
                cache_type="readahead",
                cache_options=None,
                size=None, **kwargs)

        self.pin = pin
        self.


        self.__content = None

        if 'w' in mode:
            i, name = tempfile.mkstemp()
            self.local_f = LocalFileOpener(path=name, mode=mode, fs=f)

    def _fetch_range(self, start, end):
        if self.__content is None:
            self.__content = self.fs.cat_file(self.path)
        content = self.__content[start:end]
        if "b" not in self.mode:
            return content.decode("utf-8")
        else:
            return content

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        # may not yet have been initialized, may need to call _initialize_upload


    def _upload_chunk(self, final=False):
         self.local_tmp_f.write(self.buffer.read())
        if force:
            self.commit()
        return not final
    def commit(self):

        self.local_tmp_f.close()
        # put the local file into ipfs
        self.fs.put(path=self.local_tmp_path)
        # remove temp file
        os.remove(self.temp)


    def _initiate_upload(self):
        # TODO: check if path is writable?
        i, local_tmp_path = tempfile.mkstemp()
        os.close(i)  # we want normal open and normal buffered file
        self.local_tmp_path = local_tmp_path
        self.local_tmp_f = open(local_tmp_path, mode=self.mode)


class LocalFileOpener(io.IOBase):
    def __init__(
        self, path, mode, autocommit=True, fs=None, compression=None, **kwargs
    ):
        self.path = path
        self.mode = mode
        self.fs = fs
        self.f = None
        self.autocommit = autocommit
        self.compression = get_compression(path, compression)
        self.blocksize = io.DEFAULT_BUFFER_SIZE
        self._open()

    def _open(self):
        if self.f is None or self.f.closed:
            if self.autocommit or "w" not in self.mode:
                self.f = open(self.path, mode=self.mode)
                if self.compression:
                    compress = compr[self.compression]
                    self.f = compress(self.f, mode=self.mode)
            else:
                # TODO: check if path is writable?
                i, name = tempfile.mkstemp()
                os.close(i)  # we want normal open and normal buffered file
                self.temp = name
                self.f = open(name, mode=self.mode)
            if "w" not in self.mode:
                self.size = self.f.seek(0, 2)
                self.f.seek(0)
                self.f.size = self.size

    def _fetch_range(self, start, end):
        # probably only used by cached FS
        if "r" not in self.mode:
            raise ValueError
        self._open()
        self.f.seek(start)
        return self.f.read(end - start)

    def __setstate__(self, state):
        self.f = None
        loc = state.pop("loc", None)
        self.__dict__.update(state)
        if "r" in state["mode"]:
            self.f = None
            self._open()
            self.f.seek(loc)

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop("f")
        if "r" in self.mode:
            d["loc"] = self.f.tell()
        else:
            if not self.f.closed:
                raise ValueError("Cannot serialise open write-mode local file")
        return d

    def commit(self):
        if self.autocommit:
            raise RuntimeError("Can only commit if not already set to autocommit")
        shutil.move(self.temp, self.path)

    def discard(self):
        if self.autocommit:
            raise RuntimeError("Cannot discard if set to autocommit")
        os.remove(self.temp)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return "r" not in self.mode

    def read(self, *args, **kwargs):
        return self.f.read(*args, **kwargs)

    def write(self, *args, **kwargs):
        return self.f.write(*args, **kwargs)

    def tell(self, *args, **kwargs):
        return self.f.tell(*args, **kwargs)

    def seek(self, *args, **kwargs):
        return self.f.seek(*args, **kwargs)

    def seekable(self, *args, **kwargs):
        return self.f.seekable(*args, **kwargs)

    def readline(self, *args, **kwargs):
        return self.f.readline(*args, **kwargs)

    def readlines(self, *args, **kwargs):
        return self.f.readlines(*args, **kwargs)

    def close(self):
        return self.f.close()

    @property
    def closed(self):
        return self.f.closed

    def __fspath__(self):
        # uniquely among fsspec implementations, this is a real, local path
        return self.path

    def __iter__(self):
        return self.f.__iter__()

    def __getattr__(self, item):
        return getattr(self.f, item)

    def __enter__(self):
        self._incontext = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._incontext = False
        self.f.__exit__(exc_type, exc_value, traceback)


