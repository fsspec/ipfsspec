import datetime
import json
from mockserver import mock_server

from flask import abort, request

import ipfsspec
import fsspec


class RateLimitedServer:
    def __init__(self, max_rate, objects):
        self.max_rate = max_rate
        self.next_allowed_request = datetime.datetime.now()
        self.objects = objects
        self.request_count = 0

    def configure(self, mock_server):
        mock_server.add_callback_response("/ipfs/<oid>",
                lambda oid: self.get_backoff(oid))
        mock_server.add_callback_response("/api/v0/object/stat",
                lambda: self.stat_backoff(),
                ("POST",))
        mock_server.add_json_response("/api/v0/version", {"version": "0.1_test"})

    def stat_backoff(self):
        self.request_count += 1
        now = datetime.datetime.now()
        if now <= self.next_allowed_request:
            abort(429)
        else:
            self.next_allowed_request = now + self.max_rate

            oid = request.args.get("arg")
            res = {"Hash": oid, "NumLinks": 0, "DataSize": len(self.objects[oid])}
            return json.dumps(res)

    def get_backoff(self, oid):
        self.request_count += 1
        now = datetime.datetime.now()
        if now <= self.next_allowed_request:
            abort(429)
        else:
            self.next_allowed_request = now + self.max_rate
            return self.objects[oid]

def test_backoff(mock_server):
    s = RateLimitedServer(datetime.timedelta(seconds=0.01),
            {"foo": "bar"})
    s.configure(mock_server)

    fs = fsspec.filesystem("ipfs", gateways=[mock_server.url])
    for _ in range(100):
        with fs.open("foo") as f:
            assert f.read().decode("utf-8") == "bar"
    assert s.request_count < 240
