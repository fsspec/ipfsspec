import requests
import pytest

from flask import Flask, jsonify
from threading import Thread
from uuid import uuid4

if False:
    import click
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    log.disabled = True

    def secho(text, file=None, nl=None, err=None, color=None, **styles):
        pass

    def echo(text, file=None, nl=None, err=None, color=None, **styles):
        pass

    click.echo = echo
    click.secho = secho


class MockServer(Thread):
    def __init__(self, port=5000):
        super().__init__()
        self.port = port
        self.app = Flask(__name__)
        self.app.logger.disabled = True
        self.url = "http://localhost:%s" % self.port

        self.app.add_url_rule("/shutdown", view_func=self._shutdown_server)

    def _shutdown_server(self):
        from flask import request
        if 'werkzeug.server.shutdown' not in request.environ:
            raise RuntimeError('Not running the development server')
        request.environ['werkzeug.server.shutdown']()
        return 'Server shutting down...'

    def shutdown_server(self):
        requests.get("http://localhost:%s/shutdown" % self.port)
        self.join()

    def add_callback_response(self, url, callback, methods=('GET',)):
        callback.__name__ = str(uuid4())  # change name of method to mitigate flask exception
        self.app.add_url_rule(url, view_func=callback, methods=methods)

    def add_json_response(self, url, serializable, methods=('GET',)):
        def callback():
            return jsonify(serializable)

        self.add_callback_response(url, callback, methods=methods)

    def run(self):
        self.app.run(port=self.port)


@pytest.fixture
def mock_server():
    server = MockServer()
    server.start()
    yield server
    server.shutdown_server()


@pytest.fixture
def dual_mock_server():
    server1 = MockServer(5000)
    server2 = MockServer(5001)
    server1.start()
    server2.start()
    yield server1, server2
    server1.shutdown_server()
    server2.shutdown_server()
