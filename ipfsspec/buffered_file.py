from fsspec.spec import  AbstractBufferedFile

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
                **kwargs):
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
