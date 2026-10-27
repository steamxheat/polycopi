"""Локальный «поддельный Polymarket» для проверки бота без интернета.

HTTP: /activity, /book, /markets (те же форматы, что у настоящих API).
WebSocket: RTDS-подобная лента activity/trades.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class FakeState:
    def __init__(self):
        self.activity: dict[str, list] = {}   # wallet -> [trade dicts], новые первыми
        self.books: dict[str, dict] = {}      # token -> book
        self.markets: dict[str, dict] = {}    # slug -> gamma market
        self.requests: list[str] = []
        self.connections = 0                  # новых TCP-соединений к HTTP


def start_http(state: FakeState):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            state.connections += 1

        def log_message(self, *a):
            pass

        def do_GET(self):
            u = urlsplit(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            state.requests.append(u.path)
            if u.path == "/activity":
                body = state.activity.get(q.get("user", "").lower(), [])[: int(q.get("limit", 50))]
            elif u.path == "/book":
                body = state.books.get(q.get("token_id"))
            elif u.path == "/markets":
                # как настоящая gamma: закрытые рынки видны только с closed=true
                m = state.markets.get(q.get("slug"))
                want_closed = q.get("closed") == "true"
                body = [m] if m and bool(m.get("closed")) == want_closed else []
            else:
                body = None
            if body is None:
                data, code = b'{"error":"not found"}', 404
            else:
                data, code = json.dumps(body).encode(), 200
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


# ------------------------------------------------------------------ websocket server

async def ws_read(reader):
    b1, b2 = await reader.readexactly(2)
    n = b2 & 0x7F
    if n == 126:
        n = struct.unpack("!H", await reader.readexactly(2))[0]
    elif n == 127:
        n = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if b2 & 0x80 else None
    data = await reader.readexactly(n)
    if mask:
        data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return b1 & 0x0F, bool(b2 & 0x80), data


def ws_frame(opcode: int, payload: bytes, fin: bool = True) -> bytes:
    head = bytearray([(0x80 if fin else 0) | opcode])
    n = len(payload)
    if n < 126:
        head.append(n)
    elif n < 65536:
        head.append(126)
        head += struct.pack("!H", n)
    else:
        head.append(127)
        head += struct.pack("!Q", n)
    return bytes(head) + payload


async def start_ws(handler):
    """handler(reader, writer) вызывается после успешного рукопожатия."""

    async def conn(reader, writer):
        raw = await reader.readuntil(b"\r\n\r\n")
        headers = {l.split(":", 1)[0].strip().lower(): l.split(":", 1)[1].strip()
                   for l in raw.decode().split("\r\n")[1:] if ":" in l}
        acc = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] + GUID).encode())
                               .digest()).decode()
        writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                      f"Connection: Upgrade\r\nSec-WebSocket-Accept: {acc}\r\n\r\n").encode())
        await writer.drain()
        try:
            await handler(reader, writer)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    srv = await asyncio.start_server(conn, "127.0.0.1", 0)
    return srv, f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}"
