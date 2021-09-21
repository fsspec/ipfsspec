import pytest
from ipfsspec.async_ipfs import AsyncIPFSGateway, AsyncIPFSFileSystem
import aiohttp

TEST_ROOT = "QmW3CrGFuFyF3VH1wvrap4Jend5NRTgtESDjuQ7QhHD5dd"
REF_CONTENT = b'ipfsspec test data'
TEST_FILENAMES = ["default", "multi", "raw", "raw_multi", "write"]


@pytest.mark.parametrize("gw_host", ["http://127.0.0.1:8080"])
@pytest.mark.parametrize("filename", TEST_FILENAMES)
@pytest.mark.asyncio
async def test_different_file_representations(filename, gw_host):
    gw = AsyncIPFSGateway(gw_host)
    async with aiohttp.ClientSession() as session:
        path = TEST_ROOT + "/" + filename
        info = await gw.file_info(path, session)
        assert info["size"] == len(REF_CONTENT)
        assert info["type"] == "file"
        content = await gw.cat(path, session)
        assert content == REF_CONTENT


@pytest.mark.parametrize("gw_host", ["http://127.0.0.1:8080"])
@pytest.mark.asyncio
async def test_get_cid_of_folder(gw_host):
    gw = AsyncIPFSGateway(gw_host)
    async with aiohttp.ClientSession() as session:
        info = await gw.file_info(TEST_ROOT, session)
        assert info["CID"] == TEST_ROOT


@pytest.mark.asyncio
async def test_ls(event_loop):
    AsyncIPFSFileSystem.clear_instance_cache()  # avoid reusing old event loop
    fs = AsyncIPFSFileSystem(asynchronous=True, loop=event_loop)
    res = await fs._ls(TEST_ROOT, detail=False)
    assert res == [TEST_ROOT + fs.sep + fn for fn in TEST_FILENAMES]
    res = await fs._ls(TEST_ROOT, detail=True)
    assert [r["name"] for r in res] == [TEST_ROOT + fs.sep + fn for fn in TEST_FILENAMES]
    assert all([r["size"] == len(REF_CONTENT) for r in res])


@pytest.mark.asyncio
async def test_cat_file(event_loop):
    AsyncIPFSFileSystem.clear_instance_cache()  # avoid reusing old event loop
    fs = AsyncIPFSFileSystem(asynchronous=True, loop=event_loop)
    res = await fs._cat_file(TEST_ROOT + "/default")
    assert res == REF_CONTENT
    res = await fs._cat_file(TEST_ROOT + "/default", start=3, end=7)
    assert res == REF_CONTENT[3:7]


@pytest.mark.asyncio
async def test_exists(event_loop):
    AsyncIPFSFileSystem.clear_instance_cache()  # avoid reusing old event loop
    fs = AsyncIPFSFileSystem(asynchronous=True, loop=event_loop)
    res = await fs._exists(TEST_ROOT + "/default")
    assert res is True
    res = await fs._exists(TEST_ROOT + "/missing")
    assert res is False
    res = await fs._exists("/missing")
    assert res is False


@pytest.mark.asyncio
async def test_isfile(event_loop):
    AsyncIPFSFileSystem.clear_instance_cache()  # avoid reusing old event loop
    fs = AsyncIPFSFileSystem(asynchronous=True, loop=event_loop)
    res = await fs._isfile(TEST_ROOT + "/default")
    assert res is True
    res = await fs._isfile(TEST_ROOT)
    assert res is False
