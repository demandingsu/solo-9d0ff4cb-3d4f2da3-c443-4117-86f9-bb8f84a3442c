"""Waveform capture core.

The server owns a single TCP connection to the instrument.  Every complete
CSV line that arrives is appended to a bounded in-memory history and fanned out
to every active recording.  Recordings are independent snapshots: one page
closing, or one recording failing, never affects another.

A recording is anchored on the *latest* sample known when the request arrives.
It keeps the last ``PRE_COUNT`` samples up to and including the anchor, then
keeps streaming until it has ``POST_COUNT`` samples after the anchor.

Whenever the sample stream is broken -- TCP disconnect, unparsable line, or a
non-consecutive sequence number -- the current history is discarded and all
in-flight recordings fail.  A fresh contiguous run of ``PRE_COUNT`` samples is
then required before a new recording may start.  Completed recordings are never
touched by later breaks and remain downloadable.
"""

from __future__ import annotations

import csv
import io
import itertools
import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .constcheck import ConstTracker

log = logging.getLogger("capture")

PRE_COUNT = 100
POST_COUNT = 200
TOTAL_COUNT = PRE_COUNT + POST_COUNT

HEADER_FIRST = {"序号", "seq", "sequence", "no", "index", "n"}
HEADER_SECOND = {"采样时间", "sample_time", "timestamp", "time", "ts"}
HEADER_THIRD = {"加速度", "acceleration", "accel", "value", "a"}


class SampleError(ValueError):
    """A line that cannot be parsed as a valid sample."""


class InsufficientHistory(RuntimeError):
    """Fewer than PRE_COUNT contiguous samples are available."""


class UnknownRecording(KeyError):
    """Requested recording id does not exist."""


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class Sample:
    seq: int
    ts: str
    accel: float
    received_at: str

    def csv_row(self) -> list[str]:
        return [str(self.seq), self.ts, repr(self.accel)]


def is_header(line: str) -> bool:
    parts = [p.strip().lower() for p in line.strip().split(",")]
    return (
        len(parts) == 3
        and parts[0] in HEADER_FIRST
        and parts[1] in HEADER_SECOND
        and parts[2] in HEADER_THIRD
    )


def parse_line(line: str) -> Sample:
    """Parse ``seq,sample_time,acceleration`` with optional whitespace."""
    parts = [p.strip() for p in line.strip().split(",")]
    if len(parts) != 3:
        raise SampleError(f"应为 3 个字段，实际 {len(parts)} 个")
    seq_s, ts, accel_s = parts
    if not seq_s:
        raise SampleError("序号为空")
    try:
        seq = int(seq_s, 10)
    except ValueError:
        raise SampleError(f"序号不是整数：{seq_s!r}") from None
    try:
        accel = float(accel_s)
    except ValueError:
        raise SampleError(f"加速度不是数字：{accel_s!r}") from None
    if accel != accel or accel in (float("inf"), float("-inf")):
        raise SampleError("加速度不是有限数值")
    if not ts:
        raise SampleError("采样时间为空")
    return Sample(seq=seq, ts=ts, accel=accel, received_at=iso_now())


def format_csv(samples: list[Sample]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["序号", "采样时间", "加速度"])
    for s in samples:
        writer.writerow(s.csv_row())
    return buf.getvalue().encode("utf-8")


@dataclass
class Recording:
    id: str
    anchor_seq: int
    anchor_ts: str
    created_at: str
    status: str = "recording"  # recording | completed | failed
    samples: list[Sample] = field(default_factory=list)
    received_total: int = 0
    error: str | None = None
    finished_at: str | None = None
    # Sampling segment (epoch) this recording belongs to.  Sequence numbers
    # are only comparable inside one segment: after a break the instrument
    # may restart numbering, and same-numbered samples of another segment
    # must never be mixed into this window or its constant-value hits.
    epoch: int = 0
    start_seq: int | None = None
    end_seq: int | None = None

    @property
    def received(self) -> int:
        return len(self.samples) if self.samples else self.received_total

    @property
    def status_label(self) -> str:
        return {
            "recording": "录波中",
            "completed": "已完成",
            "failed": "失败",
        }[self.status]

    def as_dict(self, const_hits: list[dict] | None = None) -> dict:
        return {
            "id": self.id,
            "anchor_seq": self.anchor_seq,
            "anchor_ts": self.anchor_ts,
            "status": self.status,
            "status_label": self.status_label,
            "received": self.received,
            "total": TOTAL_COUNT,
            "pre_count": min(PRE_COUNT, self.received),
            "post_count": max(0, self.received - PRE_COUNT),
            "pre_target": PRE_COUNT,
            "post_target": POST_COUNT,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "downloadable": self.status == "completed",
            "epoch": self.epoch,
            "start_seq": self.start_seq,
            "end_seq": self.end_seq,
            "const_hits": const_hits if const_hits is not None else [],
        }


class CaptureService:
    """Single-connection sample store plus recording bookkeeping."""

    def __init__(self, history_size: int = 200_000) -> None:
        self._lock = threading.RLock()
        # History only ever holds one contiguous run: _break_locked clears it.
        self._history: deque[Sample] = deque(maxlen=history_size)
        self._recordings: dict[str, Recording] = {}
        self._ids = itertools.count(1)
        self._epoch = 0
        self._last_sample: Sample | None = None
        self._link_state = "disconnected"
        self._link_since: str | None = None
        self._link_reason: str | None = None
        self._received_total = 0
        self._bad_lines = 0
        self._discontinuities = 0
        self._last_error: dict | None = None
        # Stuck-sensor (constant value) tracking; driven under _lock only.
        self._const = ConstTracker()

    # -- link lifecycle ---------------------------------------------------
    def link_connected(self) -> None:
        with self._lock:
            self._link_state = "connected"
            self._link_since = iso_now()
            self._link_reason = None
            # Keep last_error visible until a new contiguous sample proves
            # the stream is healthy again.
            log.info("instrument link connected")

    def link_connect_failed(self, reason: str) -> None:
        """Record a failed dial attempt without fabricating a discontinuity.

        Nothing was received over this attempt, so the current contiguous run
        (if any) is untouched; only the link state and error banner change.
        """
        with self._lock:
            self._link_state = "disconnected"
            self._link_since = iso_now()
            self._link_reason = reason
            if self._last_sample is None and not self._history:
                self._last_error = {"error": reason, "at": iso_now()}

    def link_disconnected(self, reason: str) -> None:
        with self._lock:
            was_connected = self._link_state == "connected"
            self._link_state = "disconnected"
            self._link_since = iso_now()
            self._link_reason = reason
            log.info("instrument link lost: %s", reason)
            if was_connected or self._last_sample is not None:
                self._break_locked(
                    f"连接中断（{reason}），录波跨越断点", count=True
                )

    def _break_locked(self, message: str, *, count: bool) -> None:
        """Discard the current contiguous run and fail in-flight recordings."""
        if self._last_sample is None and not self._history:
            if count:
                self._last_error = {"error": message, "at": iso_now()}
            return
        self._epoch += 1
        if count:
            self._discontinuities += 1
        self._last_error = {"error": message, "at": iso_now()}
        # A break ends any in-progress constant-value alarm as interrupted and
        # wipes the candidate run; the next segment starts from zero and is
        # never spliced onto this one.
        self._const.break_stream()
        for rec in self._recordings.values():
            if rec.status == "recording":
                rec.received_total = len(rec.samples)
                rec.status = "failed"
                rec.error = message
                rec.finished_at = iso_now()
                # A failed recording must never be downloadable: drop its data.
                rec.samples = []
                log.info("recording %s failed: %s", rec.id, message)
        self._history.clear()
        self._last_sample = None

    # -- sample ingest ----------------------------------------------------
    def ingest_line(self, line: str) -> Sample | None:
        """Ingest one raw line; returns the parsed sample, if any."""
        text = line.strip()
        if not text:
            return None
        with self._lock:
            if is_header(text):
                return None
            try:
                sample = parse_line(text)
            except SampleError as exc:
                self._bad_lines += 1
                self._break_locked(f"收到无法解析的采样行：{exc}", count=True)
                return None

            if self._last_sample is not None:
                expected = self._last_sample.seq + 1
                if sample.seq != expected:
                    # The offending sample itself is part of the break and
                    # must not seed the new run; the caller has to re-send a
                    # fully consecutive sequence.
                    self._break_locked(
                        f"序号不连续：期望 {expected}，实际 {sample.seq}",
                        count=True,
                    )
                    return None

            self._history.append(sample)
            self._last_sample = sample
            self._received_total += 1
            self._last_error = None
            alarm = self._const.ingest(sample, self._epoch)
            if alarm is not None:
                log.info(
                    "constant-value alarm %s: %s g from seq %s (epoch %s)",
                    alarm.id, alarm.value, alarm.start_seq, alarm.epoch,
                )
            for rec in self._recordings.values():
                if rec.status != "recording":
                    continue
                rec.samples.append(sample)
                if len(rec.samples) >= TOTAL_COUNT:
                    rec.status = "completed"
                    rec.end_seq = sample.seq
                    rec.finished_at = iso_now()
                    log.info(
                        "recording %s completed (%s..%s)",
                        rec.id, rec.samples[0].seq, rec.samples[-1].seq,
                    )
            return sample

    # -- recording API ----------------------------------------------------
    def start_recording(self) -> Recording:
        with self._lock:
            available = len(self._history)
            if available < PRE_COUNT:
                raise InsufficientHistory(
                    f"前段连续采样不足 {PRE_COUNT} 条（当前 {available} 条），"
                    "无法录波"
                )
            pre = list(self._history)[-PRE_COUNT:]
            anchor = pre[-1]
            rec = Recording(
                id=f"R{next(self._ids):04d}",
                anchor_seq=anchor.seq,
                anchor_ts=anchor.ts,
                created_at=iso_now(),
                samples=pre,
                epoch=self._epoch,
                start_seq=pre[0].seq,
            )
            self._recordings[rec.id] = rec
            log.info("recording %s started, anchor seq %s", rec.id, anchor.seq)
            return rec

    def recording(self, rid: str) -> Recording | None:
        with self._lock:
            return self._recordings.get(rid)

    def recordings(self) -> list[Recording]:
        with self._lock:
            return list(self._recordings.values())

    # -- constant-value (stuck sensor) check -------------------------------
    def _const_hits_locked(self, rec: Recording) -> list[dict]:
        """Confirmed alarms of the same segment intersecting a completed rec.

        Computed on read, under the lock, so a hit appears as soon as the
        alarm is confirmed -- even when the 64th identical sample arrives
        after the recording has already completed -- and always reflects the
        same sampling progress as the alarm list itself.
        """
        if (
            rec.status != "completed"
            or rec.start_seq is None
            or rec.end_seq is None
        ):
            return []
        hits = []
        for alarm in self._const.alarms:
            if alarm.epoch != rec.epoch:
                # same sequence numbers in a different segment are unrelated
                continue
            lo = max(rec.start_seq, alarm.start_seq)
            hi = min(rec.end_seq, alarm.last_seq)
            if lo > hi:
                continue
            hits.append(
                {
                    "alarm_id": alarm.id,
                    "epoch": alarm.epoch,
                    "value": alarm.value,
                    "overlap_start": lo,
                    "overlap_end": hi,
                    "alarm_status": alarm.status,
                    "alarm_status_label": alarm.status_label,
                }
            )
        return hits

    def _recording_dict_locked(self, rec: Recording) -> dict:
        return rec.as_dict(const_hits=self._const_hits_locked(rec))

    def recording_dicts(self) -> list[dict]:
        with self._lock:
            return [self._recording_dict_locked(r) for r in self._recordings.values()]

    def recording_dict(self, rid: str) -> dict | None:
        with self._lock:
            rec = self._recordings.get(rid)
            return None if rec is None else self._recording_dict_locked(rec)

    def _const_check_locked(self) -> dict:
        completed = [
            r
            for r in self._recordings.values()
            if r.status == "completed"
            and r.start_seq is not None
            and r.end_seq is not None
        ]
        alarms = []
        for alarm in self._const.alarms:
            d = alarm.as_dict()
            # every overlapping completed recording of the same segment may
            # be linked to this alarm
            d["recordings"] = [
                r.id
                for r in completed
                if r.epoch == alarm.epoch
                and max(r.start_seq, alarm.start_seq)
                <= min(r.end_seq, alarm.last_seq)
            ]
            alarms.append(d)
        run = self._const.current_run()
        if run is not None:
            run["epoch"] = self._epoch
        return {
            "min_run": self._const.min_run,
            "current_run": run,
            "alarms": alarms,
            "alarm_total": len(alarms),
            "active_total": sum(
                1 for a in self._const.alarms if a.status == "active"
            ),
        }

    def const_check(self) -> dict:
        with self._lock:
            return self._const_check_locked()

    def preview_tail(self, count: int) -> list[dict]:
        with self._lock:
            return [
                {"seq": s.seq, "ts": s.ts, "accel": s.accel}
                for s in list(self._history)[-count:]
            ]

    def download_payload(self, rid: str) -> tuple[bytes, str]:
        """Return (csv bytes, suggested filename) for a completed recording."""
        with self._lock:
            rec = self._recordings.get(rid)
            if rec is None:
                raise UnknownRecording(rid)
            if rec.status == "failed":
                raise FailedRecording(rec.error or "录波失败，不提供下载")
            if rec.status != "completed":
                raise IncompleteRecording(
                    f"录波尚未完成（{len(rec.samples)}/{TOTAL_COUNT}）"
                )
            data = format_csv(rec.samples)
            return data, f"waveform_{rec.id}_anchor_{rec.anchor_seq}.csv"

    def snapshot(self) -> dict:
        with self._lock:
            tail = self._last_sample
            return {
                "link": {
                    "state": self._link_state,
                    "since": self._link_since,
                    "reason": self._link_reason,
                },
                "stream": {
                    "epoch": self._epoch,
                    "received_total": self._received_total,
                    "bad_lines": self._bad_lines,
                    "discontinuities": self._discontinuities,
                    "contiguous_run": len(self._history),
                    "can_record": len(self._history) >= PRE_COUNT,
                    "pre_target": PRE_COUNT,
                    "last_sample": None if tail is None else {
                        "seq": tail.seq,
                        "ts": tail.ts,
                        "accel": tail.accel,
                        "received_at": tail.received_at,
                    },
                    "last_error": self._last_error,
                },
                "recordings": [
                    self._recording_dict_locked(r)
                    for r in self._recordings.values()
                ],
                "const_check": self._const_check_locked(),
            }


class FailedRecording(RuntimeError):
    pass


class IncompleteRecording(RuntimeError):
    pass
