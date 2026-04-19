"""FastAPI application entry point — serves API routes and static web frontend."""

import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.routes import monitor, records, settings, symlinks
from api.routes.ws import router as ws_router, manager as ws_manager
from db.database import init_db
from monitor.watcher import FolderWatcher

logger = logging.getLogger(__name__)

_watcher: Optional[FolderWatcher] = None


def get_watcher() -> Optional[FolderWatcher]:
    return _watcher


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _watcher
    # Startup
    init_db()
    _watcher = FolderWatcher(broadcast_fn=ws_manager.broadcast_sync)
    _watcher.start()
    logger.info("Server started — watcher active")
    yield
    # Shutdown
    if _watcher:
        _watcher.stop()
    logger.info("Server stopped")


app = FastAPI(title="刮削助手", version="2.0", lifespan=lifespan)

# CORS — allow local dev frontend (Vite on :5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(monitor.router)
app.include_router(records.router)
app.include_router(settings.router)
app.include_router(symlinks.router)
app.include_router(ws_router)

# Serve built frontend
# In frozen (PyInstaller) mode, static files are unpacked into sys._MEIPASS
if getattr(sys, 'frozen', False):
    _WEB_DIR = os.path.join(sys._MEIPASS, 'web', 'dist')
else:
    _WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'web', 'dist')
if os.path.isdir(_WEB_DIR):
    app.mount("/", StaticFiles(directory=_WEB_DIR, html=True), name="static")
