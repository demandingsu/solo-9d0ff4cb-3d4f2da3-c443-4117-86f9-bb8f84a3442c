"""End-to-end smoke test with real uvicorn servers and a real TCP socket."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV = {"PYTHONPATH": "/tmp/pylibs", "PATH": "/usr/bin:/bin", "HOME": "/tmp"}


def http(method: str, url: str, body: dict | None = None, timeout: float = 5.0,
         raw: bool = False):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
            if raw:
                return resp.status, payload, dict(resp.headers)
            return resp.status, (json.loads(payload) if payload else None), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        if raw:
            return exc.code, payload, dict(exc.headers)
        try:
            parsed = json.loads(payload) if payload else None
        except json.JSONDecodeError:
            parsed = payload.decode(errors="replace")
        return exc.code, parsed, dict(exc.headers)


def wait_http(url: str, timeout: float = 12.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http("GET", url)
            return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError(f"server never came up: {url}")


def main() -> None:
    sim_env = dict(ENV, SIM_TCP_PORT="5000", SIM_HTTP_PORT="8001")
    app_env = dict(ENV, INSTRUMENT_HOST="127.0.0.1", INSTRUMENT_PORT="5000")

    sim = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "simulator:app",
         "--host", "127.0.0.1", "--port", "8001"],
        cwd=ROOT / "simulator", env=sim_env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    app = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.app:app",
         "--host", "127.0.0.1", "--port", "8000"],
        cwd=ROOT, env=app_env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        wait_http("http://127.0.0.1:8001/api/state")
        wait_http("http://127.0.0.1:8000/api/status")

        # 1. the recorder's link must already be attached before we emit
        wait_for(lambda: http("GET", "http://127.0.0.1:8001/api/state")[1]["connected"],
                 "recorder connected to simulator")
        assert http("GET", "http://127.0.0.1:8000/api/status")[1]["stream"]["received_total"] == 0

        # 2. stream 120 samples through the simulator
        _, out, _ = http("POST", "http://127.0.0.1:8001/api/send", {"count": 120})
        assert out["sent"] == 120, out
        wait_for(lambda: http("GET", "http://127.0.0.1:8000/api/status")[1]
                 ["stream"]["received_total"] >= 120, "120 samples received")
        first_anchor = http("GET", "http://127.0.0.1:8000/api/status")[1]["stream"]["last_sample"]["seq"]

        # 3. start recording -> needs 200 more
        status, rec, _ = http("POST", "http://127.0.0.1:8000/api/recordings")
        assert status == 201 and rec["anchor_seq"] == first_anchor, rec

        # 4. a second overlapping recording after another 30 samples
        http("POST", "http://127.0.0.1:8001/api/send", {"count": 30})
        wait_for(lambda: http("GET", "http://127.0.0.1:8000/api/status")[1]
                 ["stream"]["received_total"] >= first_anchor + 30, "30 more samples")
        second_anchor = http("GET", "http://127.0.0.1:8000/api/status")[1]["stream"]["last_sample"]["seq"]
        _, rec2, _ = http("POST", "http://127.0.0.1:8000/api/recordings")
        assert rec2["anchor_seq"] == second_anchor and rec2["id"] != rec["id"]

        # 5. finish both windows
        http("POST", "http://127.0.0.1:8001/api/send", {"count": 210})
        wait_for(lambda: all(r["status"] == "completed"
                             for r in http("GET", "http://127.0.0.1:8000/api/status")[1]["recordings"]),
                 "both recordings completed")

        status, csv_bytes, headers = http(
            "GET", f"http://127.0.0.1:8000/api/recordings/{rec['id']}/download", raw=True)
        disposition = headers.get("Content-Disposition") or headers.get("content-disposition")
        assert status == 200 and disposition and "attachment" in disposition
        rows = csv_bytes.decode().strip().split("\n")
        assert rows[0] == "序号,采样时间,加速度" and len(rows) == 301

        # 5b. stuck-sensor constant run: alarm confirmed at 64 identical
        # samples and linked to the completed recording it overlaps
        http("POST", "http://127.0.0.1:8001/api/send", {"count": 100})
        wait_for(lambda: http("GET", "http://127.0.0.1:8000/api/status")[1]
                 ["stream"]["can_record"], "can record before constant run")
        status, rec_c, _ = http("POST", "http://127.0.0.1:8000/api/recordings")
        assert status == 201
        _, out, _ = http("POST", "http://127.0.0.1:8001/api/constant",
                         {"count": 80, "value": 0.85})
        assert out["sent"] == 80, out
        http("POST", "http://127.0.0.1:8001/api/send", {"count": 120})
        wait_for(lambda: next(r for r in http("GET", "http://127.0.0.1:8000/api/status")[1]["recordings"]
                              if r["id"] == rec_c["id"])["status"] == "completed",
                 "constant-run recording completed")
        snap = http("GET", "http://127.0.0.1:8000/api/status")[1]
        const_check = snap["const_check"]
        assert const_check["alarm_total"] == 1, const_check
        alarm = const_check["alarms"][0]
        assert alarm["count"] == 80 and alarm["status"] == "ended"
        assert alarm["value"] == 0.85
        mine = next(r for r in snap["recordings"] if r["id"] == rec_c["id"])
        assert mine["const_hits"], mine
        hit = mine["const_hits"][0]
        assert hit["alarm_id"] == alarm["id"]
        assert (hit["overlap_start"], hit["overlap_end"]) == (
            alarm["start_seq"], alarm["end_seq"])
        assert alarm["recordings"] == [rec_c["id"]]
        # dedicated endpoint agrees with the embedded snapshot
        _, alarms_api, _ = http("GET", "http://127.0.0.1:8000/api/const-alarms")
        assert alarms_api["alarm_total"] == 1
        assert alarms_api["alarms"][0]["id"] == alarm["id"]

        # 6. sequence jump must fail a fresh in-flight recording
        http("POST", "http://127.0.0.1:8001/api/send", {"count": 120})
        wait_for(lambda: http("GET", "http://127.0.0.1:8000/api/status")[1]
                 ["stream"]["can_record"], "can record again")
        _, rec3, _ = http("POST", "http://127.0.0.1:8000/api/recordings")
        http("POST", "http://127.0.0.1:8001/api/skip", {"gap": 25})
        wait_for(lambda: next(r for r in http("GET", "http://127.0.0.1:8000/api/status")[1]["recordings"]
                              if r["id"] == rec3["id"])["status"] == "failed",
                 "recording failed on jump")
        status, body, _ = http("GET", f"http://127.0.0.1:8000/api/recordings/{rec3['id']}/download")
        assert status == 409, body

        # 7. disconnect/reconnect: recorder must reconnect and regain 100-run
        http("POST", "http://127.0.0.1:8001/api/disconnect")
        wait_for(lambda: http("GET", "http://127.0.0.1:8000/api/status")[1]
                 ["link"]["state"] == "disconnected", "link disconnected")
        wait_for(lambda: http("GET", "http://127.0.0.1:8001/api/state")[1]["connected"],
                 "recorder reconnected")
        http("POST", "http://127.0.0.1:8001/api/send", {"count": 110})
        wait_for(lambda: http("GET", "http://127.0.0.1:8000/api/status")[1]
                 ["stream"]["contiguous_run"] >= 100, "new 100-sample run")

        # 8. index pages render
        for port in (8000, 8001):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
                assert r.status == 200 and len(r.read()) > 500

        print("E2E OK")
    finally:
        for proc in (sim, app):
            proc.terminate()
            try:
                out = proc.communicate(timeout=8)[0]
            except subprocess.TimeoutExpired:
                proc.kill()
                out = proc.communicate()[0]
            if proc.returncode not in (0, -15):
                print(f"--- {proc.args} output ---\n{out}")


def wait_for(fn, label: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if fn():
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"timeout waiting for: {label} (last={last})")


if __name__ == "__main__":
    main()
