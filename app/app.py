from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from .capture import (
    PRE_COUNT,
    CaptureService,
    FailedRecording,
    IncompleteRecording,
    InsufficientHistory,
    UnknownRecording,
)
from .instrument import InstrumentLink

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

INSTRUMENT_HOST = os.environ.get("INSTRUMENT_HOST", "instrument-simulator")
INSTRUMENT_PORT = int(os.environ.get("INSTRUMENT_PORT", "5000"))

capture = CaptureService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    link = InstrumentLink(capture, INSTRUMENT_HOST, INSTRUMENT_PORT)
    app.state.link = link
    await link.start()
    try:
        yield
    finally:
        await link.stop()


app = FastAPI(title="设备振动录波工作站", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/api/status")
async def status() -> JSONResponse:
    return JSONResponse(capture.snapshot())


@app.post("/api/recordings")
async def create_recording() -> JSONResponse:
    try:
        rec = capture.start_recording()
    except InsufficientHistory as exc:
        return JSONResponse(
            {"detail": str(exc), "reason": "insufficient_history"},
            status_code=409,
        )
    return JSONResponse(rec.as_dict(), status_code=201)


@app.get("/api/preview")
async def preview(count: int = 240) -> JSONResponse:
    count = max(1, min(2000, count))
    return JSONResponse(
        {
            "pre_target": PRE_COUNT,
            "samples": capture.preview_tail(count),
        }
    )


@app.get("/api/recordings")
async def list_recordings() -> JSONResponse:
    return JSONResponse({"recordings": capture.recording_dicts()})


@app.get("/api/recordings/{rid}")
async def get_recording(rid: str) -> JSONResponse:
    data = capture.recording_dict(rid)
    if data is None:
        return JSONResponse({"detail": "录波不存在"}, status_code=404)
    return JSONResponse(data)


@app.get("/api/const-alarms")
async def const_alarms() -> JSONResponse:
    """Stuck-sensor check: candidate run plus all confirmed alarms."""
    return JSONResponse(capture.const_check())


@app.get("/api/recordings/{rid}/download")
async def download_recording(rid: str):
    try:
        payload, filename = capture.download_payload(rid)
    except UnknownRecording:
        return JSONResponse({"detail": "录波不存在"}, status_code=404)
    except FailedRecording as exc:
        return JSONResponse(
            {"detail": f"{exc}；该录波不提供下载"}, status_code=409
        )
    except IncompleteRecording as exc:
        return JSONResponse({"detail": str(exc)}, status_code=409)
    return Response(
        content=payload,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
