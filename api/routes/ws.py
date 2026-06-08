"""WebSocket hub for real-time status broadcasting."""

import asyncio
import json
import logging
from typing import List, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from core.security import auth_enabled, is_valid_token

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

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
                # Silently remove disconnected clients
                self.disconnect(ws)

    def broadcast_sync(self, message: dict):
        """Thread-safe broadcast from worker threads.

        Broadcasting is best-effort UI feedback; a closed or missing event loop
        must never fail the scrape pipeline that produced the update.
        """
        try:
            loop = asyncio.get_running_loop()
            if not loop.is_closed():
                loop.create_task(self.broadcast(message))
            return
        except RuntimeError:
            pass

        loop = self._loop
        if not loop or loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(self.broadcast(message), loop)
        except RuntimeError as err:
            logger.debug("WebSocket broadcast skipped: %s", err)


manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    token = ws.query_params.get("token", "") or ws.cookies.get("media_scraper_auth", "")
    if auth_enabled() and not is_valid_token(token):
        await ws.close(code=1008)
        return
    await manager.connect(ws)
    # Store the event loop so sync threads can broadcast
    manager.set_loop(asyncio.get_running_loop())
    try:
        while True:
            # Keep connection alive; client may send pings
            data = await ws.receive_text()
            # Echo back as heartbeat
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except ConnectionResetError:
        # Windows-specific: client forcibly closed connection
        manager.disconnect(ws)
    except Exception as e:
        # Log unexpected errors only
        if not isinstance(e, (WebSocketDisconnect, ConnectionResetError)):
            logger.error(f"WebSocket error: {e}")
        manager.disconnect(ws)
