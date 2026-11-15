import app.app as web


class DummyLink:
    def __init__(self, *args, **kwargs):
        self.args = args

    async def start(self):
        return None

    async def stop(self):
        return None


web.InstrumentLink = DummyLink

from fastapi.testclient import TestClient  # noqa: E402


def make_client():
    web.capture = type(web.capture)()
    return TestClient(web.app)


def feed(client, start, count):
    for seq in range(start, start + count):
        web.capture.ingest_line(f"{seq},2026-11-13T10:00:{seq:02d}Z,0.01")


def test_index_and_static_assets():
    with make_client() as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "设备振动录波工作站" in page.text
        assert client.get("/static/app.js").status_code == 200
        assert client.get("/static/style.css").status_code == 200


def test_status_endpoint_shape():
    with make_client() as client:
        data = client.get("/api/status").json()
        assert set(data) == {"link", "stream", "recordings", "const_check"}
        assert data["stream"]["can_record"] is False
        assert data["stream"]["pre_target"] == 100
        cc = data["const_check"]
        assert cc["min_run"] == 64
        assert cc["alarms"] == []
        assert cc["current_run"] is None
        assert cc["alarm_total"] == 0 and cc["active_total"] == 0


def test_record_refused_then_accepted_and_downloaded():
    with make_client() as client:
        r = client.post("/api/recordings")
        assert r.status_code == 409
        assert r.json()["reason"] == "insufficient_history"

        feed(client, 1, 100)
        r = client.post("/api/recordings")
        assert r.status_code == 201
        rec = r.json()
        rid = rec["id"]
        assert rec["anchor_seq"] == 100
        assert rec["received"] == 100
        assert rec["downloadable"] is False

        # download while recording -> 409
        r = client.get(f"/api/recordings/{rid}/download")
        assert r.status_code == 409

        feed(client, 101, 200)
        data = client.get("/api/status").json()
        match = [x for x in data["recordings"] if x["id"] == rid][0]
        assert match["status"] == "completed"
        assert match["received"] == 300

        r = client.get(f"/api/recordings/{rid}/download")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        assert "attachment" in r.headers["content-disposition"]
        text = r.content.decode("utf-8")
        rows = text.strip().split("\n")
        assert rows[0] == "序号,采样时间,加速度"
        assert len(rows) == 301
        assert rows[1].startswith("1,")
        assert rows[-1].startswith("300,")

        # unknown and preview endpoints
        assert client.get("/api/recordings/NOPE").status_code == 404
        assert client.get("/api/recordings/NOPE/download").status_code == 404
        prev = client.get("/api/preview?count=20").json()
        assert len(prev["samples"]) == 20
        assert prev["samples"][-1]["seq"] == 300


def test_failed_recording_not_downloadable():
    with make_client() as client:
        feed(client, 1, 100)
        rid = client.post("/api/recordings").json()["id"]
        feed(client, 101, 10)
        # jump from 111 to 200
        web.capture.ingest_line("200,2026-11-13T10:00:00Z,0.5")
        r = client.get(f"/api/recordings/{rid}/download")
        assert r.status_code == 409
        detail = r.json()["detail"]
        assert "不提供下载" in detail
        rec = client.get(f"/api/recordings/{rid}").json()
        assert rec["status"] == "failed"
        assert "序号不连续" in rec["error"]


def const_feed(client, start, count, value=0.85):
    for seq in range(start, start + count):
        web.capture.ingest_line(f"{seq},2026-11-13T10:00:{seq % 60:02d}Z,{value}")


def test_const_alarm_api_and_recording_hits():
    with make_client() as client:
        # normal samples build the pre-window
        for seq in range(1, 101):
            web.capture.ingest_line(f"{seq},2026-11-13T10:00:{seq % 60:02d}Z,{seq * 0.01}")
        rid = client.post("/api/recordings").json()["id"]

        # 80 identical samples: alarm forms at the 64th, ends with the run
        const_feed(client, 101, 80)
        data = client.get("/api/const-alarms").json()
        assert data["min_run"] == 64
        assert data["alarm_total"] == 1
        alarm = data["alarms"][0]
        assert alarm["id"] == "C0001"
        assert alarm["start_seq"] == 101
        assert alarm["last_seq"] == 180
        assert alarm["count"] == 80
        assert alarm["status"] == "active"
        assert alarm["end_seq"] is None

        # first different value ends the alarm
        web.capture.ingest_line("181,2026-11-13T10:00:59Z,0.33")
        alarm = client.get("/api/const-alarms").json()["alarms"][0]
        assert alarm["status"] == "ended"
        assert alarm["status_label"] == "异值结束"
        assert alarm["end_seq"] == 180

        # finish the recording (181 already arrived, 119 more to go)
        for seq in range(182, 182 + 119):
            web.capture.ingest_line(f"{seq},2026-11-13T10:00:{seq % 60:02d}Z,{seq * 0.01}")
        rec = client.get(f"/api/recordings/{rid}").json()
        assert rec["status"] == "completed"
        assert rec["const_hits"] == [
            {
                "alarm_id": "C0001",
                "epoch": 0,
                "value": 0.85,
                "overlap_start": 101,
                "overlap_end": 180,
                "alarm_status": "ended",
                "alarm_status_label": "异值结束",
            }
        ]
        # the alarm cross-references the recording as well
        alarm = client.get("/api/const-alarms").json()["alarms"][0]
        assert alarm["recordings"] == [rid]
        # and the embedded snapshot carries the same view
        snap = client.get("/api/status").json()
        assert snap["const_check"]["alarm_total"] == 1
        match = [r for r in snap["recordings"] if r["id"] == rid][0]
        assert match["const_hits"][0]["alarm_id"] == "C0001"
