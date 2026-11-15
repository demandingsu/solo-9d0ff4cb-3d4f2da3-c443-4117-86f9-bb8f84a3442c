"""Instrument simulator.

A single process exposes two things:

* a TCP server (default port 5000) that emits newline-separated CSV samples
  ``seq,sample_time,acceleration`` to whichever client is connected;
* an HTTP control panel (default port 8001) to send samples in batches, skip
  sequence numbers, and drop the TCP connection on demand.

The simulator is deliberately independent of the recorder: it never blocks a
batch just because no client is attached, it simply drops the emitted lines and
says so in the UI.
"""

from __future__ import annotations

import asyncio
import math
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

HOST = os.environ.get("SIM_HOST", "0.0.0.0")
TCP_PORT = int(os.environ.get("SIM_TCP_PORT", "5000"))
HTTP_PORT = int(os.environ.get("SIM_HTTP_PORT", "8001"))
SAMPLE_INTERVAL_US = int(os.environ.get("SIM_SAMPLE_INTERVAL_US", "1000"))


class Emitter:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._writer: asyncio.StreamWriter | None = None
        self._sample_index = 0
        self._seq = 1
        self._virtual_ts = datetime.now(timezone.utc).replace(microsecond=0)
        self._rng = random.Random(20261113)
        self._sent_total = 0
        self._dropped_total = 0
        self._constant_total = 0
        self._skips = 0
        self._disconnects = 0
        self._last_batch_at: str | None = None
        self._auto_task: asyncio.Task | None = None
        self._auto_rate = 0.0
        self._connected_at: str | None = None
        self._connections_served = 0

    def reset(self) -> None:
        """Restore generator state (used between isolated test sessions)."""
        self._sample_index = 0
        self._seq = 1  # first emitted sample is sequence 1
        self._virtual_ts = datetime.now(timezone.utc).replace(microsecond=0)
        self._rng = random.Random(20261113)
        self._sent_total = 0
        self._dropped_total = 0
        self._constant_total = 0
        self._skips = 0
        self._disconnects = 0
        self._last_batch_at = None
        self._auto_rate = 0.0
        self._connected_at = None
        self._connections_served = 0
        # Do not touch _writer: TestClient reuses the same running event loop,
        # and clearing it here would strand a live connection (and lose its
        # header/sample ordering).
        self._auto_task = None

    # -- connection handling ---------------------------------------------
    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        async with self._lock:
            if self._writer is not None:
                # Only one instrument link is meaningful; accept the newest.
                old = self._writer
                self._writer = writer
                self._connected_at = _now()
                self._connections_served += 1
                try:
                    old.close()
                except Exception:  # pragma: no cover - defensive
                    pass
            else:
                self._writer = writer
                self._connected_at = _now()
                self._connections_served += 1
            await self._write_header_locked()
        try:
            # Drain anything the client sends; the instrument has nothing to
            # read, but consuming input detects disconnects promptly.
            while await reader.read(4096):
                pass
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            async with self._lock:
                if self._writer is writer:
                    self._writer = None
                    self._connected_at = None
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _write_header_locked(self) -> None:
        assert self._writer is not None
        self._writer.write("序号,采样时间,加速度\n".encode("utf-8"))
        try:
            await self._writer.drain()
        except (ConnectionError, OSError):
            self._writer = None
            self._connected_at = None

    # -- waveform ---------------------------------------------------------
    def _next_line(
        self, *, jump: int = 0, accel: float | None = None
    ) -> tuple[int, bytes, float]:
        # Normal call keeps the current sequence; a jump skips extra numbers.
        self._seq += max(0, jump)
        self._sample_index += 1
        self._virtual_ts += timedelta(microseconds=SAMPLE_INTERVAL_US)
        if accel is None:
            # Slow structural vibration + a little noise; occasional shock so
            # the trace is interesting on screen.
            t = self._sample_index / 1000.0
            accel = (
                0.62 * math.sin(2 * math.pi * 6.5 * t)
                + 0.18 * math.sin(2 * math.pi * 23.0 * t + 0.7)
                + self._rng.gauss(0.0, 0.06)
            )
            if self._rng.random() < 0.002:
                accel += self._rng.choice((-1, 1)) * self._rng.uniform(1.5, 3.5)
        ts = self._virtual_ts.isoformat(timespec="microseconds").replace("+00:00", "Z")
        line = f"{self._seq},{ts},{accel:.6f}\n"
        result = (self._seq, line.encode("utf-8"), accel)
        self._seq += 1
        return result

    # -- control API ------------------------------------------------------
    async def send_batch(self, count: int) -> dict:
        async with self._lock:
            seqs = []
            if self._writer is None:
                self._dropped_total += count
                # Advance the generator anyway so the timeline keeps moving.
                for _ in range(count):
                    seq, _, _ = self._next_line()
                    seqs.append(seq)
                self._last_batch_at = _now()
                return {
                    "sent": 0,
                    "dropped": count,
                    "first_seq": seqs[0] if seqs else None,
                    "last_seq": seqs[-1] if seqs else None,
                    "connected": False,
                }
            chunks: list[bytes] = []
            for _ in range(count):
                seq, line, _ = self._next_line()
                seqs.append(seq)
                chunks.append(line)
            try:
                self._writer.write(b"".join(chunks))
                await self._writer.drain()
            except (ConnectionError, OSError):
                self._writer = None
                self._connected_at = None
                self._dropped_total += count
                self._last_batch_at = _now()
                return {
                    "sent": 0,
                    "dropped": count,
                    "first_seq": seqs[0] if seqs else None,
                    "last_seq": seqs[-1] if seqs else None,
                    "connected": False,
                }
            self._sent_total += count
            self._last_batch_at = _now()
            return {
                "sent": count,
                "dropped": 0,
                "first_seq": seqs[0] if seqs else None,
                "last_seq": seqs[-1] if seqs else None,
                "connected": True,
            }

    async def send_constant(self, count: int, value: float) -> dict:
        """Emit ``count`` samples that all carry the same acceleration.

        Simulates a stuck sensor: the recorder raises a constant-value alarm
        once 64 identical samples arrive inside one contiguous segment.
        """
        async with self._lock:
            seqs: list[int] = []
            chunks: list[bytes] = []
            for _ in range(count):
                seq, line, _ = self._next_line(accel=value)
                seqs.append(seq)
                chunks.append(line)
            if self._writer is None:
                self._dropped_total += count
                self._last_batch_at = _now()
                return {
                    "sent": 0,
                    "dropped": count,
                    "value": value,
                    "first_seq": seqs[0] if seqs else None,
                    "last_seq": seqs[-1] if seqs else None,
                    "connected": False,
                }
            try:
                self._writer.write(b"".join(chunks))
                await self._writer.drain()
            except (ConnectionError, OSError):
                self._writer = None
                self._connected_at = None
                self._dropped_total += count
                self._last_batch_at = _now()
                return {
                    "sent": 0,
                    "dropped": count,
                    "value": value,
                    "first_seq": seqs[0] if seqs else None,
                    "last_seq": seqs[-1] if seqs else None,
                    "connected": False,
                }
            self._sent_total += count
            self._constant_total += count
            self._last_batch_at = _now()
            return {
                "sent": count,
                "dropped": 0,
                "value": value,
                "first_seq": seqs[0] if seqs else None,
                "last_seq": seqs[-1] if seqs else None,
                "connected": True,
            }

    async def skip_ahead(self, gap: int) -> dict:
        """Emit one sample whose sequence number jumps by ``gap``."""
        async with self._lock:
            seq, line, _ = self._next_line(jump=max(1, gap))
            self._skips += 1
            if self._writer is None:
                self._dropped_total += 1
                self._last_batch_at = _now()
                return {"sent": 0, "dropped": 1, "seq": seq, "gap": gap, "connected": False}
            try:
                self._writer.write(line)
                await self._writer.drain()
            except (ConnectionError, OSError):
                self._writer = None
                self._connected_at = None
                self._dropped_total += 1
                self._last_batch_at = _now()
                return {"sent": 0, "dropped": 1, "seq": seq, "gap": gap, "connected": False}
            self._sent_total += 1
            self._last_batch_at = _now()
            return {"sent": 1, "dropped": 0, "seq": seq, "gap": gap, "connected": True}

    async def disconnect(self) -> dict:
        """Drop the TCP connection, simulating a cable pull."""
        async with self._lock:
            self._disconnects += 1
            writer, self._writer = self._writer, None
            self._connected_at = None
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            return {"disconnected": True, "was_connected": True, "connected": False}
        return {"disconnected": True, "was_connected": False, "connected": False}

    async def set_auto(self, rate: float, batch: int) -> dict:
        """Continuously emit ``batch`` samples at ``rate`` batches/second."""
        await self.stop_auto()
        if rate <= 0:
            return {"auto": False, "rate": 0}
        batch = max(1, min(1000, batch))
        interval = 1.0 / min(rate, 50.0)

        async def loop() -> None:
            try:
                while True:
                    await asyncio.sleep(interval)
                    await self.send_batch(batch)
            except asyncio.CancelledError:
                pass

        self._auto_rate = rate
        self._auto_task = asyncio.create_task(loop(), name="auto-emit")
        return {"auto": True, "rate": self._auto_rate, "batch": batch}

    async def stop_auto(self) -> None:
        task, self._auto_task = self._auto_task, None
        self._auto_rate = 0.0
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def state(self) -> dict:
        return {
            "connected": self._writer is not None,
            "connected_at": self._connected_at,
            "connections_served": self._connections_served,
            "next_seq": self._seq + 1,
            "last_seq": self._seq if self._sample_index else None,
            "sent_total": self._sent_total,
            "dropped_total": self._dropped_total,
            "constant_total": self._constant_total,
            "skips": self._skips,
            "disconnects": self._disconnects,
            "last_batch_at": self._last_batch_at,
            "auto": self._auto_task is not None,
            "auto_rate": self._auto_rate,
            "tcp_port": TCP_PORT,
        }

    async def shutdown(self) -> None:
        await self.stop_auto()
        writer, self._writer = self._writer, None
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


emitter = Emitter()
server: asyncio.AbstractServer | None = None


class BatchRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=5000)


class ConstantRequest(BaseModel):
    count: int = Field(default=80, ge=1, le=5000)
    value: float = Field(default=0.85, allow_inf_nan=False)


class SkipRequest(BaseModel):
    gap: int = Field(default=10, ge=1, le=100000)


class AutoRequest(BaseModel):
    enabled: bool = True
    rate: float = Field(default=5, ge=0.1, le=50)
    batch: int = Field(default=20, ge=1, le=1000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global server
    server = await asyncio.start_server(
        emitter.handle_client, HOST, TCP_PORT, reuse_address=True
    )
    app.state.server = server
    try:
        yield
    finally:
        await emitter.shutdown()
        server.close()
        await server.wait_closed()
        server = None


app = FastAPI(title="仪器模拟器", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/api/state")
async def get_state() -> JSONResponse:
    return JSONResponse(emitter.state())


@app.post("/api/send")
async def send(req: BatchRequest) -> JSONResponse:
    return JSONResponse(await emitter.send_batch(req.count))


@app.post("/api/constant")
async def constant(req: ConstantRequest) -> JSONResponse:
    return JSONResponse(await emitter.send_constant(req.count, req.value))


@app.post("/api/skip")
async def skip(req: SkipRequest) -> JSONResponse:
    return JSONResponse(await emitter.skip_ahead(req.gap))


@app.post("/api/disconnect")
async def disconnect() -> JSONResponse:
    return JSONResponse(await emitter.disconnect())


@app.post("/api/auto")
async def auto(req: AutoRequest) -> JSONResponse:
    if req.enabled:
        return JSONResponse(await emitter.set_auto(req.rate, req.batch))
    await emitter.stop_auto()
    return JSONResponse({"auto": False, "rate": 0})


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
