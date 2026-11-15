"""Real-time stuck-sensor (constant value) detection.

Within one contiguous sampling segment (no sequence jumps, no disconnects,
no unparsable lines), ``CONST_RUN_MIN`` consecutive samples carrying exactly
the same acceleration form a single alarm.  Later identical samples extend
that alarm; the first different value ends it.  A stream break ends an
in-progress alarm as *interrupted* and resets the candidate run, so identical
values from the next segment are never spliced onto a previous segment's run.

The tracker is pure bookkeeping and owns no lock.  ``CaptureService`` drives
it while holding its own lock, so alarms and recording associations are always
observed from the same sampling progress and an alarm can never be opened
twice for one run.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

CONST_RUN_MIN = 64

STATUS_LABELS = {
    "active": "进行中",
    "ended": "异值结束",
    "interrupted": "中断结束",
}


class _Sample(Protocol):
    seq: int
    accel: float


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class ConstAlarm:
    """One confirmed constant-value run (>= CONST_RUN_MIN identical samples)."""

    id: str
    epoch: int
    value: float
    start_seq: int
    last_seq: int
    count: int
    created_at: str
    status: str = "active"  # active | ended | interrupted
    ended_at: str | None = None

    @property
    def end_seq(self) -> int | None:
        """Final sequence of the run; None while the alarm is in progress."""
        return None if self.status == "active" else self.last_seq

    @property
    def status_label(self) -> str:
        return STATUS_LABELS[self.status]

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "epoch": self.epoch,
            "value": self.value,
            "start_seq": self.start_seq,
            "last_seq": self.last_seq,
            "end_seq": self.end_seq,
            "count": self.count,
            "status": self.status,
            "status_label": self.status_label,
            "created_at": self.created_at,
            "ended_at": self.ended_at,
        }


class ConstTracker:
    """Tracks the current identical-value run and the confirmed alarms."""

    def __init__(self, min_run: int = CONST_RUN_MIN) -> None:
        self.min_run = min_run
        self.alarms: list[ConstAlarm] = []
        self._ids = itertools.count(1)
        # candidate run state (reset on every stream break)
        self._value: float | None = None
        self._count = 0
        self._start_seq: int | None = None
        self._last_seq: int | None = None
        self._alarm: ConstAlarm | None = None  # alarm of the current run

    def ingest(self, sample: _Sample, epoch: int) -> ConstAlarm | None:
        """Track one accepted sample; returns the alarm the moment it forms."""
        if self._count and sample.accel == self._value:
            self._count += 1
            self._last_seq = sample.seq
            alarm = self._alarm
            if alarm is not None:
                # same alarm keeps growing; never a second one for this run
                alarm.count = self._count
                alarm.last_seq = sample.seq
                return None
            if self._count == self.min_run:
                # count > 1 here, so the run fields are always populated
                assert self._value is not None and self._start_seq is not None
                alarm = ConstAlarm(
                    id=f"C{next(self._ids):04d}",
                    epoch=epoch,
                    value=self._value,
                    start_seq=self._start_seq,
                    last_seq=sample.seq,
                    count=self._count,
                    created_at=_now(),
                )
                self.alarms.append(alarm)
                self._alarm = alarm
                return alarm
            return None
        # first different value closes the current run (and its alarm)
        if self._alarm is not None:
            self._alarm.status = "ended"
            self._alarm.ended_at = _now()
            self._alarm = None
        self._value = sample.accel
        self._count = 1
        self._start_seq = sample.seq
        self._last_seq = sample.seq
        return None

    def break_stream(self) -> None:
        """End an in-progress alarm as interrupted; reset the candidate run.

        Called on sequence jumps, unparsable lines and disconnects.  A run
        that never reached ``min_run`` samples simply vanishes; confirmed
        alarms keep their samples but are marked ``interrupted`` so the page
        can tell a break apart from a genuine value change.
        """
        if self._alarm is not None:
            self._alarm.status = "interrupted"
            self._alarm.ended_at = _now()
            self._alarm = None
        self._value = None
        self._count = 0
        self._start_seq = None
        self._last_seq = None

    def current_run(self) -> dict | None:
        """The live identical-value candidate run, if any."""
        if not self._count:
            return None
        return {
            "value": self._value,
            "count": self._count,
            "start_seq": self._start_seq,
            "last_seq": self._last_seq,
            "needed": self.min_run,
            "alarm_id": None if self._alarm is None else self._alarm.id,
        }
