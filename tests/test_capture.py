import pytest

from app.capture import (
    POST_COUNT,
    PRE_COUNT,
    TOTAL_COUNT,
    CaptureService,
    FailedRecording,
    IncompleteRecording,
    InsufficientHistory,
    SampleError,
    UnknownRecording,
    format_csv,
    is_header,
    parse_line,
)


def line(seq: int, accel: float | None = None, ts: str | None = None) -> str:
    if accel is None:
        accel = seq * 0.01
    if ts is None:
        ts = f"2026-11-13T10:00:{seq:02d}.000000Z"
    return f"{seq},{ts},{accel}"


def feed(svc: CaptureService, start: int, count: int) -> None:
    for seq in range(start, start + count):
        svc.ingest_line(line(seq))


def test_parse_line_ok_and_header():
    s = parse_line("12,2026-11-13T10:00:00Z,-0.25")
    assert s.seq == 12
    assert s.accel == -0.25
    assert s.ts.endswith("Z")
    assert is_header("序号,采样时间,加速度")
    assert is_header("seq,timestamp,accel")
    assert not is_header("1,a,0.1")


@pytest.mark.parametrize("bad", ["", "   ", "1,a", "a,2026,0.1", "1,2026,x",
                                  "1,2026,inf", "1,2026,nan", "1,2026,0.1,extra"])
def test_parse_line_rejects_bad_input(bad):
    with pytest.raises(SampleError):
        parse_line(bad)


def test_recording_requires_100_contiguous_samples():
    svc = CaptureService()
    with pytest.raises(InsufficientHistory):
        svc.start_recording()
    feed(svc, 1, PRE_COUNT - 1)
    with pytest.raises(InsufficientHistory):
        svc.start_recording()
    svc.ingest_line(line(PRE_COUNT))
    rec = svc.start_recording()
    assert rec.received == PRE_COUNT
    assert rec.anchor_seq == PRE_COUNT


def test_window_is_100_inclusive_plus_200_after():
    svc = CaptureService()
    feed(svc, 1, 105)
    rec = svc.start_recording()
    assert rec.anchor_seq == 105
    assert [s.seq for s in rec.samples] == list(range(6, 106))

    feed(svc, 106, POST_COUNT)
    assert rec.status == "completed"
    assert rec.received == TOTAL_COUNT
    assert rec.samples[0].seq == 6
    assert rec.samples[-1].seq == 305

    payload, filename = svc.download_payload(rec.id)
    text = payload.decode("utf-8")
    rows = text.strip().split("\n")
    assert rows[0] == "序号,采样时间,加速度"
    assert len(rows) == TOTAL_COUNT + 1
    assert rows[1].startswith("6,")
    assert rows[-1].startswith("305,")
    assert filename == f"waveform_{rec.id}_anchor_105.csv"
    assert format_csv(rec.samples).count(b"\n") == TOTAL_COUNT + 1


def test_overlapping_recordings_complete_independently():
    svc = CaptureService()
    feed(svc, 1, 120)
    rec_a = svc.start_recording()  # anchor 120, pre 21..120
    feed(svc, 121, 50)
    rec_b = svc.start_recording()  # anchor 170, pre 71..170
    assert [s.seq for s in rec_b.samples] == list(range(71, 171))
    feed(svc, 171, 200)  # through 370

    assert rec_a.status == "completed" and rec_b.status == "completed"
    assert [s.seq for s in rec_a.samples] == list(range(21, 321))
    assert [s.seq for s in rec_b.samples] == list(range(71, 371))
    payload_a, _ = svc.download_payload(rec_a.id)
    payload_b, _ = svc.download_payload(rec_b.id)
    assert payload_a != payload_b


def test_sequence_jump_fails_in_flight_and_requires_new_run():
    svc = CaptureService()
    feed(svc, 1, 100)
    rec = svc.start_recording()
    feed(svc, 101, 40)
    assert rec.status == "recording"

    # jump: expected 141, get 150 instead
    svc.ingest_line(line(150))
    assert rec.status == "failed"
    assert "序号不连续" in (rec.error or "")
    assert rec.samples == []
    with pytest.raises(FailedRecording):
        svc.download_payload(rec.id)
    assert svc.snapshot()["stream"]["discontinuities"] == 1
    # the offending jump sample is itself discarded; a new run starts empty
    assert svc.snapshot()["stream"]["contiguous_run"] == 0

    # only 99 fresh samples -> still refused
    feed(svc, 151, PRE_COUNT - 1)
    with pytest.raises(InsufficientHistory):
        svc.start_recording()
    svc.ingest_line(line(250))
    rec2 = svc.start_recording()
    assert rec2.anchor_seq == 250


def test_unparsable_line_breaks_continuity():
    svc = CaptureService()
    feed(svc, 1, 100)
    rec = svc.start_recording()
    svc.ingest_line("garbage,line,value")
    assert rec.status == "failed"
    assert svc.snapshot()["stream"]["bad_lines"] == 1
    with pytest.raises(InsufficientHistory):
        svc.start_recording()


def test_disconnect_fails_recording_but_completed_remains_downloadable():
    svc = CaptureService()
    svc.link_connected()
    feed(svc, 1, 300)
    done = svc.start_recording()
    # anchor 300, needs 200 post samples
    feed(svc, 301, POST_COUNT)
    assert done.status == "completed"
    payload_before, _ = svc.download_payload(done.id)

    feed(svc, 501, 100)
    active = svc.start_recording()
    svc.link_disconnected("仪器关闭了连接")
    assert active.status == "failed"
    with pytest.raises(FailedRecording):
        svc.download_payload(active.id)

    # completed file survives
    payload_after, _ = svc.download_payload(done.id)
    assert payload_before == payload_after

    # fresh run of 100 required
    with pytest.raises(InsufficientHistory):
        svc.start_recording()
    feed(svc, 601, PRE_COUNT)
    again = svc.start_recording()
    assert again.anchor_seq == 700


def test_disconnect_without_samples_does_not_poison_future_run():
    svc = CaptureService()
    svc.link_connected()
    svc.link_disconnected("startup failure")
    svc.link_connected()
    feed(svc, 1, PRE_COUNT)
    rec = svc.start_recording()
    assert rec.status == "recording"
    assert svc.snapshot()["stream"]["discontinuities"] == 0


def test_connect_failure_while_down_does_not_add_discontinuity():
    svc = CaptureService()
    feed(svc, 1, 50)
    svc.link_disconnected("仪器关闭了连接")
    assert svc.snapshot()["stream"]["discontinuities"] == 1
    # repeated failed dial attempts must keep the same discontinuity count
    for _ in range(3):
        svc.link_connect_failed("连接被拒绝")
    snap = svc.snapshot()
    assert snap["stream"]["discontinuities"] == 1
    assert snap["link"]["state"] == "disconnected"
    assert "连接被拒绝" in snap["link"]["reason"]


def test_first_sample_after_reconnect_starts_fresh_run():
    svc = CaptureService()
    feed(svc, 1, 100)
    assert svc.snapshot()["stream"]["contiguous_run"] == 100
    svc.link_disconnected("cable pull")
    svc.link_connected()
    # new instrument session starts again at seq 1; not treated as a jump
    svc.ingest_line(line(1))
    snap = svc.snapshot()["stream"]
    assert snap["contiguous_run"] == 1
    assert snap["discontinuities"] == 1
    feed(svc, 2, 99)
    assert svc.snapshot()["stream"]["can_record"] is True


def test_download_state_guards():
    svc = CaptureService()
    with pytest.raises(UnknownRecording):
        svc.download_payload("R9999")
    feed(svc, 1, PRE_COUNT)
    rec = svc.start_recording()
    with pytest.raises(IncompleteRecording):
        svc.download_payload(rec.id)


def test_preview_tail_and_snapshot_fields():
    svc = CaptureService()
    feed(svc, 1, 130)
    tail = svc.preview_tail(10)
    assert [s["seq"] for s in tail] == list(range(121, 131))
    snap = svc.snapshot()
    assert snap["stream"]["contiguous_run"] == 130
    assert snap["stream"]["can_record"] is True
    assert snap["stream"]["last_sample"]["seq"] == 130
    assert len(snap["recordings"]) == 0
