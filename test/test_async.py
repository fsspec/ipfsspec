import pytest
import pytest_asyncio
from ipfsspec.async_ipfs import AsyncIPFSGateway, AsyncIPFSFileSystem
import asyncio
import aiohttp

TEST_ROOT = "QmW3CrGFuFyF3VH1wvrap4Jend5NRTgtESDjuQ7QhHD5dd"
REF_CONTENT = b'ipfsspec test data'
TEST_FILENAMES = ["default", "multi", "raw", "raw_multi", "write"]


@pytest_asyncio.fixture
async def session():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest_asyncio.fixture
async def get_client(session):
    async def get_client(**kwargs):
        return session


@pytest_asyncio.fixture
async def fs(get_client):
    AsyncIPFSFileSystem.clear_instance_cache()  # avoid reusing old event loop
    return AsyncIPFSFileSystem(asynchronous=True, loop=asyncio.get_running_loop(), get_client=get_client)


@pytest.mark.parametrize("gw_host", ["http://127.0.0.1:8080"])
@pytest.mark.parametrize("filename", TEST_FILENAMES)
@pytest.mark.asyncio
async def test_different_file_representations(filename, gw_host, session):
    gw = AsyncIPFSGateway(gw_host)

    path = TEST_ROOT + "/" + filename
    info = await gw.info(path, session)
    assert info["size"] == len(REF_CONTENT)
    assert info["type"] == "file"
    content = await gw.cat(path, session)
    assert content == REF_CONTENT


@pytest.mark.parametrize("gw_host", ["http://127.0.0.1:8080"])
@pytest.mark.asyncio
async def test_get_cid_of_folder(gw_host, session):
    gw = AsyncIPFSGateway(gw_host)

    info = await gw.info(TEST_ROOT, session)
    assert info["CID"] == TEST_ROOT


@pytest.mark.asyncio
async def test_ls(fs):
    res = await fs._ls(TEST_ROOT, detail=False)
    assert res == [TEST_ROOT + fs.sep + fn for fn in TEST_FILENAMES]
    res = await fs._ls(TEST_ROOT, detail=True)
    assert [r["name"] for r in res] == [TEST_ROOT + fs.sep + fn for fn in TEST_FILENAMES]
    assert all([r["size"] == len(REF_CONTENT) for r in res])


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.asyncio
async def test_ls_missing(fs, detail):
    with pytest.raises(FileNotFoundError):
        await fs._ls(TEST_ROOT + "/missing", detail=detail)


@pytest.mark.asyncio
async def test_glob(fs):
    res = await fs._glob(TEST_ROOT + "/w*")
    assert res == [TEST_ROOT + fs.sep + fn for fn in TEST_FILENAMES if fn.startswith("w")]


@pytest.mark.asyncio
async def test_cat_file(fs):
    res = await fs._cat_file(TEST_ROOT + "/default")
    assert res == REF_CONTENT
    res = await fs._cat_file(TEST_ROOT + "/default", start=3, end=7)
    assert res == REF_CONTENT[3:7]


@pytest.mark.asyncio
async def test_exists(fs):
    res = await fs._exists(TEST_ROOT + "/default")
    assert res is True
    res = await fs._exists(TEST_ROOT + "/missing")
    assert res is False


@pytest.mark.asyncio
async def test_isfile(fs):
    res = await fs._isfile(TEST_ROOT + "/default")
    assert res is True
    res = await fs._isfile(TEST_ROOT)
    assert res is False
