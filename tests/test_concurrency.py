import threading

from app.capture import POST_COUNT, PRE_COUNT, TOTAL_COUNT, CaptureService


def line(seq: int) -> str:
    return f"{seq},2026-11-13T10:00:{seq:02d}Z,0.1"


def test_concurrent_ingest_and_recording_start():
    svc = CaptureService()

    def producer(start: int, count: int) -> None:
        for seq in range(start, start + count):
            svc.ingest_line(line(seq))

    # pre-fill 100, then hammer recordings while more samples flow in
    producer(1, PRE_COUNT)
    errors: list[Exception] = []
    created: list = []

    def worker() -> None:
        try:
            for _ in range(20):
                try:
                    created.append(svc.start_recording())
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    feeder = threading.Thread(target=producer, args=(101, 400))
    for t in threads + [feeder]:
        t.start()
    for t in threads + [feeder]:
        t.join(timeout=10)
        assert not t.is_alive()

    assert not errors, errors
    # every started recording either completed with exactly 300 samples or
    # is still recording; no corrupted window lengths
    for rec in created:
        assert len(rec.samples) in (PRE_COUNT, TOTAL_COUNT) or rec.status == "recording"
        if rec.status == "completed":
            seqs = [s.seq for s in rec.samples]
            assert len(seqs) == TOTAL_COUNT
            assert seqs == list(range(seqs[0], seqs[0] + TOTAL_COUNT))
            assert seqs[PRE_COUNT - 1] == rec.anchor_seq


def test_break_while_many_recordings_are_open():
    svc = CaptureService()
    for seq in range(1, 130):
        svc.ingest_line(line(seq))
    recs = [svc.start_recording()]
    for seq in range(130, 160):
        svc.ingest_line(line(seq))
        if seq % 10 == 0:
            recs.append(svc.start_recording())

    # discontinuity kills all in-flight recordings at once
    svc.ingest_line(line(999))
    assert all(r.status == "failed" for r in recs)
    assert all(r.samples == [] for r in recs)
    assert svc.snapshot()["stream"]["contiguous_run"] == 0

    # and a full 300-sample completed recording stays downloadable
    for seq in range(1, PRE_COUNT + 1):
        svc.ingest_line(line(seq + 2000))
    done = svc.start_recording()
    for seq in range(PRE_COUNT + 1, PRE_COUNT + 1 + POST_COUNT):
        svc.ingest_line(line(seq + 2000))
    assert done.status == "completed"
    payload, _ = svc.download_payload(done.id)
    assert len(payload.strip().split(b"\n")) == TOTAL_COUNT + 1
