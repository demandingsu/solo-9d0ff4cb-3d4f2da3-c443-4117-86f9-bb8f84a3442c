import os
import socket

os.environ["SIM_TCP_PORT"] = "0"  # chosen at runtime via start_server
os.environ["SIM_HTTP_PORT"] = "8099"

import simulator  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_emitter():
    """Each test starts from sequence 1 with an empty state."""
    simulator.emitter.reset()
    yield
    simulator.emitter.reset()


class TcpProbe:
    """Minimal synchronous TCP client for the simulator."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        self.sock.settimeout(1.0)
        self.buf = b""

    def read_lines(self, n):
        lines = []
        while len(lines) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise AssertionError("simulator closed connection early")
            self.buf += chunk
            while b"\n" in self.buf:
                line, self.buf = self.buf.split(b"\n", 1)
                lines.append(line.decode())
        return lines

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def test_simulator_batch_skip_disconnect_and_auto():
    with TestClient(simulator.app) as client:
        # server is started by lifespan
        assert simulator.server is not None
        port = simulator.server.sockets[0].getsockname()[1]

        state = client.get("/api/state").json()
        assert state["connected"] is False

        probe = TcpProbe(port)
        header = probe.read_lines(1)[0]
        assert header == "序号,采样时间,加速度"

        # wait until the HTTP side sees the connection
        for _ in range(50):
            if client.get("/api/state").json()["connected"]:
                break
            # yield to the event loop handled inside TestClient thread;
            # a quick HTTP call is enough to make progress observable
            client.get("/api/state")

        r = client.post("/api/send", json={"count": 25}).json()
        assert r["sent"] == 25 and r["dropped"] == 0
        assert r["first_seq"] == 1 and r["last_seq"] == 25
        lines = probe.read_lines(25)
        assert lines[0].startswith("1,")
        assert lines[-1].startswith("25,")
        assert len(lines[0].split(",")) == 3

        r = client.post("/api/skip", json={"gap": 10}).json()
        assert r["sent"] == 1
        jumped = probe.read_lines(1)[0]
        # last sample was 25; next normal would be 26, then skip 10 -> 36
        assert jumped.startswith("36,")

        state = client.get("/api/state").json()
        assert state["skips"] == 1
        assert state["next_seq"] == 38
        assert state["connected"] is True

        # disconnect from the simulator side
        r = client.post("/api/disconnect").json()
        assert r["was_connected"] is True
        with probe.sock:
            try:
                assert probe.sock.recv(16) == b""
            except OSError:
                pass
        probe.close()

        state = client.get("/api/state").json()
        assert state["connected"] is False
        assert state["disconnects"] == 1

        # batches while disconnected are dropped but advance the sequence
        r = client.post("/api/send", json={"count": 3}).json()
        assert r["sent"] == 0 and r["dropped"] == 3
        state = client.get("/api/state").json()
        assert state["next_seq"] == 41
        assert state["sent_total"] == 26  # 25 batch + 1 skip, the 3 dropped ones don't count

        # auto mode lifecycle
        on = client.post("/api/auto", json={"enabled": True, "rate": 20, "batch": 5}).json()
        assert on["auto"] is True
        client.post("/api/auto", json={"enabled": False})
        state = client.get("/api/state").json()
        assert state["auto"] is False


def test_simulator_sends_constant_samples():
    with TestClient(simulator.app) as client:
        port = simulator.server.sockets[0].getsockname()[1]
        probe = TcpProbe(port)
        try:
            header = probe.read_lines(1)[0]
            assert header == "序号,采样时间,加速度"
            for _ in range(50):
                if client.get("/api/state").json()["connected"]:
                    break
                client.get("/api/state")

            r = client.post("/api/constant", json={"count": 70, "value": 0.85}).json()
            assert r["sent"] == 70 and r["dropped"] == 0
            assert r["first_seq"] == 1 and r["last_seq"] == 70
            assert r["value"] == 0.85
            lines = probe.read_lines(70)
            for i, raw in enumerate(lines, start=1):
                seq, _, accel = raw.split(",")
                assert int(seq) == i  # sequence stays consecutive
                assert accel == "0.850000"  # every sample identical

            state = client.get("/api/state").json()
            assert state["constant_total"] == 70
            assert state["sent_total"] == 70
            assert state["next_seq"] == 72

            # a normal batch afterwards continues the same sequence
            r = client.post("/api/send", json={"count": 2}).json()
            assert r["first_seq"] == 71 and r["last_seq"] == 72
        finally:
            probe.close()


def test_constant_request_validation():
    with TestClient(simulator.app) as client:
        assert client.post("/api/constant", json={"count": 0}).status_code == 422
        assert client.post("/api/constant", json={"count": 5001}).status_code == 422
        assert client.post(
            "/api/constant", json={"count": 10, "value": "abc"}
        ).status_code == 422
        assert client.post(
            "/api/constant", json={"count": 10, "value": "inf"}
        ).status_code == 422
        # defaults: 80 samples of 0.85 g
        r = client.post("/api/constant", json={}).json()
        assert r["value"] == 0.85
        assert r["sent"] == 0 and r["dropped"] == 80  # no client connected


def test_simulator_serves_control_page():
    with TestClient(simulator.app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "压力机仪器模拟器" in page.text
        assert "恒值" in page.text
        assert client.get("/static/sim.js").status_code == 200


def test_request_validation():
    with TestClient(simulator.app) as client:
        assert client.post("/api/send", json={"count": 0}).status_code == 422
        assert client.post("/api/skip", json={"gap": -2}).status_code == 422
