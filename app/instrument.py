"""Instrument TCP client.

One background task maintains exactly one TCP connection to the instrument.
Every complete line (``\n``-terminated) is handed to :class:`CaptureService`.
The connection is re-established with backoff; each reconnect is reported as a
discontinuity so in-flight recordings fail as required.
"""

from __future__ import annotations

import asyncio
import logging

from .capture import CaptureService

log = logging.getLogger("instrument")

MAX_LINE_BYTES = 64 * 1024
RECONNECT_MIN = 0.5
RECONNECT_MAX = 5.0


class InstrumentLink:
    def __init__(
        self,
        capture: CaptureService,
        host: str,
        port: int,
        reconnect_min: float = RECONNECT_MIN,
        reconnect_max: float = RECONNECT_MAX,
    ) -> None:
        self._capture = capture
        self._host = host
        self._port = port
        self._reconnect_min = reconnect_min
        self._reconnect_max = reconnect_max
        self._task: asyncio.Task | None = None
        self._closing = False

    async def start(self) -> None:
        self._closing = False
        self._task = asyncio.create_task(self._run(), name="instrument-link")

    async def stop(self) -> None:
        self._closing = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        delay = self._reconnect_min
        while not self._closing:
            try:
                reader, writer = await asyncio.open_connection(self._host, self._port)
            except (OSError, asyncio.TimeoutError) as exc:
                self._capture.link_connect_failed(
                    f"连接仪器 {self._host}:{self._port} 失败"
                    f"（{type(exc).__name__}: {exc}）"
                )
                log.warning("connect failed: %s", exc)
                await asyncio.sleep(delay)
                delay = min(self._reconnect_max, delay * 1.6)
                continue

            delay = self._reconnect_min
            self._capture.link_connected()
            reason = "仪器关闭了连接"
            try:
                while True:
                    raw = await reader.readline()
                    if not raw:
                        break
                    if len(raw) > MAX_LINE_BYTES:
                        # Feed a deliberately invalid line so the stream is
                        # marked broken instead of silently merging fragments.
                        self._capture.ingest_line("<超长行无法解析,,>")
                        continue
                    line = raw.decode("utf-8", errors="replace").strip()
                    if line:
                        self._capture.ingest_line(line)
            except (ConnectionError, OSError, ValueError) as exc:
                reason = f"{type(exc).__name__}: {exc}"
                log.warning("read error: %s", exc)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
            if self._closing:
                break
            self._capture.link_disconnected(reason)
            await asyncio.sleep(delay)
