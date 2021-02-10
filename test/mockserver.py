import http.server
from contextlib import contextmanager

import threading
import random


def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def any_free_port():
    port = random.randint(4001, 7999)
    while is_port_in_use(port):
        port = random.randint(4001, 7999)
    return port


@contextmanager
def mock_servers(handlers):
    servers = []
    urls = []
    threads = []
    for handler in handlers:
        port = any_free_port()
        server = http.server.HTTPServer(("localhost", port), handler)
        url = "http://localhost:%s" % port
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append(server)
        urls.append(url)
        threads.append(thread)
    yield urls
    for server in servers:
        server.shutdown()
    for thread in threads:
        thread.join()
