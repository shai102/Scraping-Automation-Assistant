"""WebSocket hub for real-time status broadcasting."""

import asyncio
import json
import logging
from typing import List

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        data = json.dumps(message, ensure_ascii=False)
        for ws in list(self.active):
            try:
                await ws.send_text(data)
            except Exception:
                self.disconnect(ws)

    def broadcast_sync(self, message: dict):
        """Thread-safe broadcast — schedules the async broadcast on the event loop."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.broadcast(message))
        except RuntimeError:
            # No running loop in this thread — try to get the server's loop
            if hasattr(self, "_loop") and self._loop:
                self._loop.call_soon_threadsafe(
                    asyncio.ensure_future, self.broadcast(message)
                )


manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    # Store the event loop so sync threads can broadcast
    manager._loop = asyncio.get_running_loop()
    try:
        while True:
            # Keep connection alive; client may send pings
            data = await ws.receive_text()
            # Echo back as heartbeat
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)
