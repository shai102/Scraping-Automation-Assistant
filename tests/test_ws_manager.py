import asyncio
import unittest

from api.routes.ws import ConnectionManager


class FakeWebSocket:
    def __init__(self):
        self.sent = []

    async def send_text(self, data):
        self.sent.append(data)


class WebSocketManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_sync_uses_running_loop(self):
        manager = ConnectionManager()
        ws = FakeWebSocket()
        manager.active.append(ws)

        manager.broadcast_sync({"type": "record_update", "data": {"id": 1}})
        await asyncio.sleep(0)

        self.assertEqual(1, len(ws.sent))
        self.assertIn('"record_update"', ws.sent[0])

    def test_broadcast_sync_ignores_closed_loop(self):
        manager = ConnectionManager()
        loop = asyncio.new_event_loop()
        manager.set_loop(loop)
        loop.close()

        manager.broadcast_sync({"type": "record_update"})


if __name__ == "__main__":
    unittest.main()
