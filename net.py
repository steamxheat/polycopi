"""Сеть только на стандартной библиотеке Python: ничего ставить не нужно.

- Http: пул постоянных HTTPS-соединений (без повторного TLS-рукопожатия на каждый запрос).
- WebSocket: минимальный клиент RFC 6455 на asyncio (текстовые кадры, ping/pong, фрагменты).
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import http.client
import json
import os
import ssl
import struct
import threading
import time
from urllib.parse import urlsplit, urlencode

UA = "polycopy-sim/1.0"


class HttpError(Exception):
    pass


class Http:
    """Синхронный пул соединений; вызывается из asyncio через to_thread."""

    def __init__(self, timeout: float = 5.0, keepalive: bool = True):
        self.timeout = timeout
        self.keepalive = keepalive      # False: новое соединение на каждый запрос
        self._idle: dict[str, list] = {}
        self._lock = threading.Lock()
        self._ctx = ssl.create_default_context()

    def _get_conn(self, scheme: str, host: str):
        key = f"{scheme}://{host}"
        with self._lock:
            pool = self._idle.setdefault(key, [])
            if pool:
                return key, pool.pop()
        if scheme == "https":
            return key, http.client.HTTPSConnection(host, timeout=self.timeout, context=self._ctx)
        return key, http.client.HTTPConnection(host, timeout=self.timeout)

    def _put_conn(self, key: str, conn) -> None:
        with self._lock:
            self._idle.setdefault(key, []).append(conn)

    def get_json(self, url: str, params: dict | None = None):
        u = urlsplit(url)
        path = (u.path or "/") + ("?" + u.query if u.query else "")
        if params:
            path += ("&" if "?" in path else "?") + urlencode(params)
        last = None
        for attempt in range(2):            # одно повторение, если соединение протухло
            key, conn = self._get_conn(u.scheme, u.netloc)
            try:
                conn.request("GET", path, headers={"User-Agent": UA, "Accept": "application/json"})
                r = conn.getresponse()
                body = r.read()
                if r.status != 200:
                    conn.close()
                    raise HttpError(f"{r.status} {url} {body[:200]!r}")
                if self.keepalive:
                    self._put_conn(key, conn)
                else:
                    conn.close()
                return json.loads(body)
            except HttpError:
                raise
            except Exception as e:          # noqa: BLE001 — сеть может упасть как угодно
                conn.close()
                last = e
        raise HttpError(f"{url}: {last}")

    async def aget(self, url: str, params: dict | None = None):
        return await asyncio.to_thread(self.get_json, url, params)


# ---------------------------------------------------------------- WebSocket

class WsClosed(Exception):
    pass


_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocket:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.r = reader
        self.w = writer
        self._wlock = asyncio.Lock()

    @classmethod
    async def connect(cls, url: str, timeout: float = 10.0) -> "WebSocket":
        u = urlsplit(url)
        secure = u.scheme == "wss"
        host = u.hostname
        port = u.port or (443 if secure else 80)
        ctx = ssl.create_default_context() if secure else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ctx, server_hostname=host if secure else None),
            timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        path = (u.path or "/") + ("?" + u.query if u.query else "")
        hdr = (f"GET {path} HTTP/1.1\r\nHost: {u.netloc}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
               f"Sec-WebSocket-Version: 13\r\nUser-Agent: {UA}\r\n\r\n")
        writer.write(hdr.encode())
        await writer.drain()
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout)
        lines = raw.decode("latin-1").split("\r\n")
        if " 101 " not in lines[0] + " ":
            writer.close()
            raise WsClosed(f"handshake failed: {lines[0]}")
        headers = {l.split(":", 1)[0].strip().lower(): l.split(":", 1)[1].strip()
                   for l in lines[1:] if ":" in l}
        expect = base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
        if headers.get("sec-websocket-accept") != expect:
            writer.close()
            raise WsClosed("bad Sec-WebSocket-Accept")
        return cls(reader, writer)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        head = bytearray([0x80 | opcode])
        n = len(payload)
        if n < 126:
            head.append(0x80 | n)
        elif n < 65536:
            head.append(0x80 | 126)
            head += struct.pack("!H", n)
        else:
            head.append(0x80 | 127)
            head += struct.pack("!Q", n)
        mask = os.urandom(4)
        head += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        async with self._wlock:
            self.w.write(bytes(head) + masked)
            await self.w.drain()

    async def send(self, text: str) -> None:
        await self._send_frame(0x1, text.encode())

    async def _read_frame(self):
        try:
            b1, b2 = await self.r.readexactly(2)
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack("!H", await self.r.readexactly(2))[0]
            elif n == 127:
                n = struct.unpack("!Q", await self.r.readexactly(8))[0]
            mask = await self.r.readexactly(4) if b2 & 0x80 else None
            data = await self.r.readexactly(n) if n else b""
        except (asyncio.IncompleteReadError, ConnectionError) as e:
            raise WsClosed(str(e)) from e
        if mask:
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        return bool(b1 & 0x80), b1 & 0x0F, data

    async def recv(self) -> str:
        parts: list[bytes] = []
        while True:
            fin, op, data = await self._read_frame()
            if op == 0x9:                      # ping -> pong
                await self._send_frame(0xA, data)
                continue
            if op == 0xA:
                continue
            if op == 0x8:
                raise WsClosed("closed by server")
            parts.append(data)
            if fin:
                return b"".join(parts).decode("utf-8", "replace")

    async def close(self) -> None:
        try:
            await self._send_frame(0x8, b"")
        except Exception:  # noqa: BLE001
            pass
        self.w.close()


def now() -> float:
    return time.time()
