"""Constant-value (stuck sensor) detection and recording association."""
import threading

import pytest

from app.capture import POST_COUNT, PRE_COUNT, CaptureService
from app.constcheck import CONST_RUN_MIN, ConstTracker


def line(seq: int, accel: float | None = None) -> str:
    if accel is None:
        accel = seq * 0.01
    return f"{seq},2026-11-13T10:00:{seq % 60:02d}Z,{accel}"


def feed(svc: CaptureService, start: int, count: int, accel: float | None = None) -> None:
    for seq in range(start, start + count):
        svc.ingest_line(line(seq, accel))


def const(svc: CaptureService, start: int, count: int, value: float = 0.85) -> None:
    feed(svc, start, count, accel=value)


def alarms(svc: CaptureService) -> list[dict]:
    return svc.snapshot()["const_check"]["alarms"]


def current_run(svc: CaptureService) -> dict | None:
    return svc.snapshot()["const_check"]["current_run"]


# ---------------------------------------------------------------- tracker
def test_tracker_needs_64_identical_samples():
    tr = ConstTracker()
    for i in range(1, CONST_RUN_MIN):
        assert tr.ingest(line_sample(i, 0.5), epoch=0) is None
    assert tr.alarms == []
    assert tr.current_run()["count"] == CONST_RUN_MIN - 1
    # the 64th identical sample confirms exactly one alarm
    alarm = tr.ingest(line_sample(CONST_RUN_MIN, 0.5), epoch=0)
    assert alarm is not None and alarm.id == "C0001"
    assert alarm.start_seq == 1 and alarm.last_seq == CONST_RUN_MIN
    assert alarm.count == CONST_RUN_MIN and alarm.status == "active"
    assert len(tr.alarms) == 1


def line_sample(seq: int, accel: float):
    class S:
        pass

    s = S()
    s.seq = seq
    s.accel = accel
    return s


def test_tracker_extends_single_alarm_and_ends_on_change():
    tr = ConstTracker()
    for i in range(1, 90):
        tr.ingest(line_sample(i, 1.25), epoch=0)
    assert len(tr.alarms) == 1  # still one alarm, not one per sample
    alarm = tr.alarms[0]
    assert alarm.count == 89 and alarm.last_seq == 89 and alarm.status == "active"
    tr.ingest(line_sample(90, 0.4), epoch=0)
    assert alarm.status == "ended"
    assert alarm.end_seq == 89 and alarm.ended_at is not None
    # a new run of a different value can raise its own alarm
    for i in range(90, 90 + CONST_RUN_MIN):
        tr.ingest(line_sample(i, 0.4), epoch=0)
    assert len(tr.alarms) == 2
    assert tr.alarms[1].id == "C0002" and tr.alarms[1].start_seq == 90


def test_tracker_break_interrupts_and_forbids_splicing():
    tr = ConstTracker()
    for i in range(1, 80):
        tr.ingest(line_sample(i, 0.5), epoch=0)
    tr.break_stream()
    assert tr.alarms[0].status == "interrupted"
    assert tr.alarms[0].end_seq == 79
    assert tr.current_run() is None
    # same value in the next segment must NOT extend the old run: the 15
    # samples needed to reach 64 are not carried over
    for i in range(1, 16):
        tr.ingest(line_sample(i, 0.5), epoch=1)
    assert len(tr.alarms) == 1
    for i in range(16, 16 + CONST_RUN_MIN - 15):
        tr.ingest(line_sample(i, 0.5), epoch=1)
    assert len(tr.alarms) == 2
    assert tr.alarms[1].epoch == 1 and tr.alarms[1].start_seq == 1


def test_tracker_break_below_threshold_leaves_no_alarm():
    tr = ConstTracker()
    for i in range(1, 63):
        tr.ingest(line_sample(i, 0.5), epoch=0)
    tr.break_stream()
    assert tr.alarms == []
    assert tr.current_run() is None


# ------------------------------------------------------------- integration
def test_alarm_lifecycle_via_service():
    svc = CaptureService()
    const(svc, 1, CONST_RUN_MIN - 1)
    assert alarms(svc) == []
    run = current_run(svc)
    assert run["count"] == CONST_RUN_MIN - 1 and run["alarm_id"] is None

    const(svc, CONST_RUN_MIN, 1)
    (alarm,) = alarms(svc)
    assert alarm["start_seq"] == 1 and alarm["end_seq"] is None
    assert alarm["status"] == "active" and alarm["status_label"] == "进行中"

    const(svc, CONST_RUN_MIN + 1, 36)  # 100 identical in total
    (alarm,) = alarms(svc)
    assert alarm["count"] == 100 and alarm["last_seq"] == 100

    svc.ingest_line(line(101, 0.5))  # first different value ends it
    (alarm,) = alarms(svc)
    assert alarm["status"] == "ended"
    assert alarm["end_seq"] == 100 and alarm["count"] == 100
    assert current_run(svc)["value"] == 0.5


def test_sequence_jump_marks_interrupted_and_starts_new_segment():
    svc = CaptureService()
    const(svc, 1, 80)
    svc.ingest_line(line(200, 0.85))  # jump: offending sample is discarded
    (alarm,) = alarms(svc)
    assert alarm["status"] == "interrupted"
    assert alarm["status_label"] == "中断结束"
    assert alarm["end_seq"] == 80 and alarm["epoch"] == 0
    assert current_run(svc) is None
    # the same value after the jump cannot continue the old run
    const(svc, 201, CONST_RUN_MIN - 1)
    assert len(alarms(svc)) == 1
    const(svc, 201 + CONST_RUN_MIN - 1, 1)
    assert len(alarms(svc)) == 2
    assert alarms(svc)[1]["epoch"] == 1
    assert alarms(svc)[1]["start_seq"] == 201


def test_disconnect_marks_interrupted():
    svc = CaptureService()
    svc.link_connected()
    const(svc, 1, 70)
    svc.link_disconnected("cable pull")
    (alarm,) = alarms(svc)
    assert alarm["status"] == "interrupted" and alarm["end_seq"] == 70
    svc.link_connected()
    const(svc, 1, CONST_RUN_MIN)  # fresh segment, fresh run
    assert len(alarms(svc)) == 2
    assert alarms(svc)[1]["epoch"] == 1


def test_completed_recording_marks_alarm_and_overlap():
    svc = CaptureService()
    feed(svc, 1, PRE_COUNT)  # 1..100 varying
    rec = svc.start_recording()  # window 1..300
    const(svc, 101, 80)  # alarm 101..180
    feed(svc, 181, POST_COUNT - 80)  # finish post window
    assert rec.status == "completed"
    (hit,) = svc.recording_dict(rec.id)["const_hits"]
    assert hit["alarm_id"] == "C0001"
    assert hit["overlap_start"] == 101 and hit["overlap_end"] == 180
    assert hit["alarm_status"] == "ended"
    # alarm side lists the recording
    (alarm,) = alarms(svc)
    assert alarm["recordings"] == [rec.id]


def test_retroactive_association_when_alarm_confirms_after_completion():
    """Recording completes while the run is < 64; the alarm confirms later
    and the completed recording must still be linked (补关联)."""
    svc = CaptureService()
    feed(svc, 1, PRE_COUNT)
    rec = svc.start_recording()  # window 1..300
    feed(svc, 101, 150)  # varying 101..250
    const(svc, 251, 50)  # constant run starts at 251, only 50 samples so far
    assert rec.status == "completed"  # 100 pre + 200 post, ends at seq 300
    assert alarms(svc) == []  # run has not reached 64 yet
    assert svc.recording_dict(rec.id)["const_hits"] == []

    const(svc, 301, 14)  # same run continues; the 64th identical sample
    (alarm,) = alarms(svc)
    assert alarm["start_seq"] == 251 and alarm["count"] == 64
    (hit,) = svc.recording_dict(rec.id)["const_hits"]
    assert hit["alarm_id"] == alarm["id"]
    assert (hit["overlap_start"], hit["overlap_end"]) == (251, 300)
    assert alarm["recordings"] == [rec.id]


def test_overlapping_recordings_share_one_alarm():
    svc = CaptureService()
    feed(svc, 1, 120)
    rec_a = svc.start_recording()  # window 21..320
    feed(svc, 121, 50)
    rec_b = svc.start_recording()  # window 71..370
    const(svc, 171, 100)  # alarm 171..270 inside both windows
    feed(svc, 271, 100)
    assert rec_a.status == "completed" and rec_b.status == "completed"
    (alarm,) = alarms(svc)
    assert alarm["count"] == 100
    hit_a = svc.recording_dict(rec_a.id)["const_hits"][0]
    hit_b = svc.recording_dict(rec_b.id)["const_hits"][0]
    assert hit_a["alarm_id"] == hit_b["alarm_id"] == alarm["id"]
    assert (hit_a["overlap_start"], hit_a["overlap_end"]) == (171, 270)
    assert (hit_b["overlap_start"], hit_b["overlap_end"]) == (171, 270)
    assert sorted(alarm["recordings"]) == sorted([rec_a.id, rec_b.id])


def test_same_seq_in_other_segment_is_not_mixed():
    svc = CaptureService()
    svc.link_connected()
    const(svc, 1, 100)  # alarm A in segment 0: seqs 1..100
    feed(svc, 101, 50)  # contiguous run grows to 150
    rec_old = svc.start_recording()  # window 51..350 in segment 0
    feed(svc, 151, POST_COUNT)
    assert rec_old.status == "completed"
    svc.link_disconnected("cable pull")  # segment 1 begins
    svc.link_connected()
    const(svc, 1, 100)  # same seq numbers, new segment: alarm B
    feed(svc, 101, 50)
    rec_new = svc.start_recording()  # window 51..350 in segment 1
    feed(svc, 151, POST_COUNT)
    assert rec_new.status == "completed"

    a1, a2 = alarms(svc)
    assert a1["epoch"] == 0 and a2["epoch"] == 1
    assert a1["id"] != a2["id"]
    old_hits = svc.recording_dict(rec_old.id)["const_hits"]
    new_hits = svc.recording_dict(rec_new.id)["const_hits"]
    # each recording is linked only to the alarm of its own segment
    assert [h["alarm_id"] for h in old_hits] == [a1["id"]]
    assert [h["alarm_id"] for h in new_hits] == [a2["id"]]
    assert (old_hits[0]["overlap_start"], old_hits[0]["overlap_end"]) == (51, 100)
    assert (new_hits[0]["overlap_start"], new_hits[0]["overlap_end"]) == (51, 100)
    assert a1["recordings"] == [rec_old.id]
    assert a2["recordings"] == [rec_new.id]


def test_in_progress_recording_has_no_hits_until_completed():
    svc = CaptureService()
    feed(svc, 1, PRE_COUNT)
    rec = svc.start_recording()
    const(svc, 101, 80)  # alarm already confirmed, recording still running
    assert rec.status == "recording"
    assert svc.recording_dict(rec.id)["const_hits"] == []
    feed(svc, 181, POST_COUNT - 80)
    assert rec.status == "completed"
    assert len(svc.recording_dict(rec.id)["const_hits"]) == 1


def test_failed_recording_has_no_hits():
    svc = CaptureService()
    feed(svc, 1, PRE_COUNT)
    rec = svc.start_recording()
    const(svc, 101, 80)
    svc.ingest_line(line(500, 0.1))  # jump fails the recording
    assert rec.status == "failed"
    assert svc.recording_dict(rec.id)["const_hits"] == []


def test_const_check_snapshot_shape():
    svc = CaptureService()
    cc = svc.snapshot()["const_check"]
    assert cc == {
        "min_run": CONST_RUN_MIN,
        "current_run": None,
        "alarms": [],
        "alarm_total": 0,
        "active_total": 0,
    }
    const(svc, 1, 70)
    cc = svc.snapshot()["const_check"]
    assert cc["alarm_total"] == 1 and cc["active_total"] == 1
    assert cc["current_run"]["count"] == 70
    assert cc["current_run"]["alarm_id"] == "C0001"
    svc.ingest_line(line(71, 0.2))
    cc = svc.snapshot()["const_check"]
    assert cc["active_total"] == 0


def test_concurrent_ingest_and_queries_single_alarm():
    """Queries racing ingestion always see a consistent progress and the
    64-sample crossing opens exactly one alarm."""
    svc = CaptureService()
    stop = threading.Event()
    problems: list[str] = []

    def reader() -> None:
        while not stop.is_set():
            snap = svc.snapshot()
            cc = snap["const_check"]
            ids = [a["id"] for a in cc["alarms"]]
            if len(ids) != len(set(ids)):
                problems.append(f"duplicate alarm ids: {ids}")
            for rec in snap["recordings"]:
                for hit in rec["const_hits"]:
                    if hit["alarm_id"] not in ids:
                        problems.append(
                            f"hit {hit['alarm_id']} missing from alarm list"
                        )
            svc.recording_dicts()
            svc.const_check()

    svc.ingest_line(line(1, 0.85))
    threads = [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    for seq in range(2, 400):
        svc.ingest_line(line(seq, 0.85))
    stop.set()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive()

    assert not problems, problems
    cc = svc.snapshot()["const_check"]
    assert cc["alarm_total"] == 1
    assert cc["alarms"][0]["count"] == 399
