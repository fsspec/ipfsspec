import pytest
import time

from ipfsspec.async_ipfs import MultiGateway, AsyncIPFSGatewayBase, RequestsTooQuick


class MockGateway(AsyncIPFSGatewayBase):
    def __init__(self, objects):
        self.objects = objects

    async def cid_get(self, path, session, headers=None, **kwargs):
        try:
            return self.objects[path]
        except KeyError:
            raise FileNotFoundError(path)


class RateLimitedMockGateway(AsyncIPFSGatewayBase):
    def __init__(self, max_rate, base, report_time=True):
        self.request_count = 0
        self.next_allowed_request = time.monotonic()

        self.max_rate = max_rate
        self.base = base
        self.report_time = report_time

    def _rate_limit(self):
        self.request_count += 1
        now = time.monotonic()
        if now <= self.next_allowed_request:
            raise RequestsTooQuick(self.next_allowed_request - now if self.report_time else None)
        else:
            self.next_allowed_request = now + self.max_rate

    async def cid_get(self, path, session, headers=None, **kwargs):
        self._rate_limit()
        return await self.base.cid_get(path, session, headers=headers, **kwargs)


@pytest.fixture
def session():
    return None


@pytest.mark.asyncio
async def test_backoff(session):
    base = MockGateway({
        "QmTz3oc4gdpRMKP2sdGUPZTAGRngqjsi99BPoztyP53JMM": "bar",
    })
    gws = [RateLimitedMockGateway(0.01, base)]
    gw = MultiGateway(gws)

    for _ in range(50):
        obj = await gw.cid_get("QmTz3oc4gdpRMKP2sdGUPZTAGRngqjsi99BPoztyP53JMM", session)
        assert obj == "bar"
    assert 50 <= gws[0].request_count < 240


@pytest.mark.asyncio
async def test_backoff_use_faster_server(session):
    base = MockGateway({
        "QmTz3oc4gdpRMKP2sdGUPZTAGRngqjsi99BPoztyP53JMM": "zapp",
    })
    gws = [
        RateLimitedMockGateway(0.1, base),
        RateLimitedMockGateway(0.01, base)
    ]
    gw = MultiGateway(gws)

    for _ in range(100):
        obj = await gw.cid_get("QmTz3oc4gdpRMKP2sdGUPZTAGRngqjsi99BPoztyP53JMM", session)
        assert obj == "zapp"
    assert gws[0].request_count < gws[1].request_count
