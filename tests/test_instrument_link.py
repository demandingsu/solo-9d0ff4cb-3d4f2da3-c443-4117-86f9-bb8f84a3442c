import asyncio

from app.capture import CaptureService
from app.instrument import InstrumentLink


async def wait_for(predicate, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition timed out")


async def run_link_test():
    svc = CaptureService()

    async def handle(reader, writer):
        connection = next(handle.counter)
        if connection == 0:
            # first connection: 100 lines, then the instrument drops away
            for seq in range(1, 101):
                writer.write(f"{seq},t{seq},0.1\n".encode())
            await writer.drain()
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
        else:
            # reconnect: fresh instrument session, far-away sequence numbers
            for seq in range(500, 620):
                writer.write(f"{seq},t{seq},0.2\n".encode())
            await writer.drain()
            await reader.read(1)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    handle.counter = iter(range(100))
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    link = InstrumentLink(svc, "127.0.0.1", port,
                          reconnect_min=0.05, reconnect_max=0.1)
    await link.start()
    try:
        await wait_for(lambda: svc.snapshot()["stream"]["received_total"] >= 100)
        # disconnect detection -> discontinuity recorded, history cleared
        await wait_for(lambda: svc.snapshot()["stream"]["discontinuities"] >= 1)
        snap = svc.snapshot()
        assert snap["link"]["state"] == "disconnected"

        # reconnect restarts a fresh contiguous run from seq 500
        await wait_for(lambda: svc.snapshot()["stream"]["contiguous_run"] >= 100)
        snap = svc.snapshot()
        assert snap["stream"]["last_sample"]["seq"] >= 599
        assert snap["link"]["state"] == "connected"
        # can record again after recovering 100 contiguous samples
        rec = svc.start_recording()
        assert rec.anchor_seq >= 599
    finally:
        await link.stop()
        server.close()
        await server.wait_closed()


def test_instrument_link_reconnect_and_discontinuity():
    asyncio.run(run_link_test())
