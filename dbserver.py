import json
import os
import socket
from datetime import datetime as dt
from importlib import import_module
from socketserver import BaseRequestHandler, UnixStreamServer
from typing import Callable

from persistence import get_conn

SOCKET = "dbserver.socket"

"""
SQLite is advertised to work even with multiple processes accessing the db file at once.
Supposedly writes can only go one at a time but that's about the only drawback; if one
write doesn't block for too long other writes should eventually get in. In practice, I
got a bunch of "database locked" errors.

So _naturally_, I'm now running a separate "dbserver" process that is the only one
allowed to touch the db. Others talk to it via a UNIX socket when they need to do
db-touching stuff.

Not sure how ridiculous this is but hey I wanted an excuse to try writing socket code.
"""


## CLIENT CODE


def via_dbserver(func):
    """
    Wrap a normal function with this and we get a function that, when called,
    automagically gets sent to dbserver, executes there, and give us back the return
    value.

    What's the catch?
    - Wrapped function can only take JSON-serializable args/kwargs.
    - Wrapped function must anticipate an extra `conn` arg prepended to *args.
    """

    def wrapper(*args, **kwargs):
        return call_via_dbserver(func, *args, **kwargs)

    return wrapper


def call_via_dbserver(func: Callable, *args, **kwargs):
    start = dt.now()

    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(SOCKET)

    payload = json.dumps(
        {
            "func_module": func.__module__,
            "func_name": func.__name__,
            "args": args,
            "kwargs": kwargs,
        }
    ).encode()

    size = str(len(payload)).encode()
    client.sendall(size + b"\n")
    client.sendall(payload)
    resp = client.recv(1048576)
    client.close()
    result = json.loads(resp.decode())

    duration = (dt.now() - start).total_seconds()
    print(f"IPC call took {duration}s")
    return result


## SERVER CODE


class DbServerRequestHandler(BaseRequestHandler):
    def handle(self):
        header = self.request.recv(4096)

        size_end_index = header.find(b"\n")
        size = int(header[:size_end_index])
        bytedata = header[size_end_index + 1 :]

        while len(bytedata) < size:
            bytedata += self.request.recv(1048576)

        assert len(bytedata) == size

        data = json.loads(bytedata)

        func_module = data["func_module"]
        func_name = data["func_name"]
        func = getattr(import_module(func_module), func_name)
        args = data["args"]
        kwargs = data["kwargs"]

        resp = func(self.server.conn, *args, **kwargs)
        result = json.dumps(resp)

        self.request.sendall(result.encode())

        print(f"{size} {func_name}")


class DbServer(UnixStreamServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = get_conn()
        print(f"{self.__class__.__name__} up")


def runserver():
    if os.path.exists(SOCKET):
        os.remove(SOCKET)

    with DbServer(SOCKET, DbServerRequestHandler) as server:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()


if __name__ == "__main__":
    runserver()
