"""File-system monitor — watches configured directories for new media files,
runs the recognition pipeline headlessly, and records results into SQLite.

Automatically identifiable files are archived; unrecognizable ones are stored
with status ``pending_manual`` for the user to handle in the web UI.
"""

import json
import logging
import os
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Set

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from core.models.media_item import MediaItem
from core.services.worker_context import WorkerContext
from core.workers.task_runner import process_task as _process_task
from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from utils.telegram_notify import NotificationBatcher

logger = logging.getLogger(__name__)

# Debounce: wait this many seconds after last event before processing a file
_DEBOUNCE_SECONDS = 5.0

# Polling: scan folders every N seconds to catch network-written files
_POLL_INTERVAL_SECONDS = 120.0


def _has_nfo(filepath: str) -> bool:
    """Check if a sibling .nfo file exists for the given media file."""
    base = os.path.splitext(filepath)[0]
    return os.path.isfile(base + ".nfo")


def _is_already_scraped(filepath: str, sub_audio_exts: tuple) -> bool:
    """Return True if this file should be treated as already scraped.

    For video files: check same-name .nfo exists.
    For subtitle/audio sidecar files (.ass/.srt/.mka etc.): the pipeline never
    writes a per-file .nfo for them, so instead check whether the containing
    directory already has evidence of a completed scrape — either a season.nfo
    or any per-episode .nfo file (excluding season.nfo / tvshow.nfo / folder.nfo).
    """
    if sub_audio_exts and filepath.lower().endswith(sub_audio_exts):
        parent = os.path.dirname(filepath)
        # Fast path: season.nfo present → Season folder was already scraped
        if os.path.isfile(os.path.join(parent, "season.nfo")):
            return True
        # Fallback: any per-episode .nfo in the same directory
        _SKIP_NAMES = {"season.nfo", "tvshow.nfo", "folder.nfo"}
        try:
            for fn in os.listdir(parent):
                if fn.lower().endswith(".nfo") and fn.lower() not in _SKIP_NAMES:
                    return True
        except OSError:
            pass
        return False
    return _has_nfo(filepath)


def _try_nfo_fast_path(item, ctx) -> bool:
    """Try to resolve subtitle/audio file metadata from an existing tvshow.nfo.

    Searches the file's parent directory and its parent for a tvshow.nfo containing
    a TMDB or BGM ID. If found, fetches episode metadata directly and populates
    item.metadata / item.new_name_only / item.full_target without going through the
    full recognition pipeline.

    Returns True if fast-path succeeded, False if caller should fall back to _process_task.
    """
    import xml.etree.ElementTree as ET
    from guessit import guessit
    from db.tmdb_api import fetch_tmdb_episode_meta, fetch_tmdb_season_poster, fetch_hybrid_episode_meta
    from utils.helpers import safe_filename, safe_str, extract_episode_number

    file_dir = os.path.dirname(item.path)
    # Search current dir then parent dir for tvshow.nfo
    search_dirs = [file_dir, os.path.dirname(file_dir)]
    nfo_path = None
    for d in search_dirs:
        candidate = os.path.join(d, "tvshow.nfo")
        if os.path.isfile(candidate):
            nfo_path = candidate
            break
    if not nfo_path:
        return False

    try:
        tree = ET.parse(nfo_path)
        root_el = tree.getroot()
    except Exception:
        return False

    # Extract TMDB / BGM id
    tmdb_id, bgm_id = "", ""
    for uid in root_el.findall("uniqueid"):
        uid_type = (uid.get("type") or "").lower()
        val = (uid.text or "").strip()
        if not val:
            continue
        if uid_type == "tmdb" and not tmdb_id:
            tmdb_id = val
        elif uid_type in ("bgm", "bangumi") and not bgm_id:
            bgm_id = val
    # Also try <tmdbid> / <id> tags used by some scrapers
    if not tmdb_id:
        el = root_el.find("tmdbid")
        if el is not None and (el.text or "").strip():
            tmdb_id = el.text.strip()

    tid = tmdb_id or bgm_id
    if not tid:
        return False

    use_tmdb = bool(tmdb_id)

    # Extract series title from nfo
    title_el = root_el.find("title")
    series_title = (title_el.text or "").strip() if title_el is not None else ""
    year_el = root_el.find("year")
    year = (year_el.text or "").strip() if year_el is not None else ""

    # Parse season / episode from file name
    from utils.helpers import extract_lang_and_ext
    pure_name, _ = extract_lang_and_ext(item.old_name, ctx.lang_tags.get() if hasattr(ctx, 'lang_tags') else "")
    g = guessit(pure_name)
    raw_s = g.get("season") or 1
    raw_e = g.get("episode")
    if isinstance(raw_e, list):
        raw_e = raw_e[0]
    if raw_e is None:
        raw_e = extract_episode_number(pure_name, g)
    if raw_e is None:
        return False  # Cannot determine episode number

    s = int(raw_s) if str(raw_s).isdigit() else 1
    e = int(raw_e)

    # Fetch episode meta
    api_tmdb = ctx.tmdb_api_key.get().strip() if hasattr(ctx, 'tmdb_api_key') else ""
    api_bgm = ctx.bgm_api_key.get().strip() if hasattr(ctx, 'bgm_api_key') else ""

    ep_n, ep_p, ep_s, s_p = "", "", "", ""
    try:
        if use_tmdb:
            ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(tid, s, e, api_tmdb, series_title, api_bgm)
            s_p = fetch_tmdb_season_poster(tid, s, api_tmdb)
        else:
            ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(series_title, tid, s, e, api_bgm, api_tmdb, year)
    except Exception:
        return False

    # Build sub/audio file new name using the same format as task_runner
    from utils.helpers import extract_lang_and_ext as _ela
    _, ext_full = _ela(item.old_name, ctx.lang_tags.get() if hasattr(ctx, 'lang_tags') else "")
    s_fmt = f"{s:02d}"
    e_fmt = f"{e:02d}"
    safe_t = safe_filename(series_title)
    safe_ep = safe_filename(ep_n or f"第 {e} 集")

    new_fn = (
        ctx.tv_format.get()
        .replace("{title}", safe_t)
        .replace("{year}", safe_str(year))
        .replace("{s:02d}", s_fmt)
        .replace("{s}", s_fmt)
        .replace("{e:02d}", e_fmt)
        .replace("{e}", e_fmt)
        .replace("{ep_name}", safe_ep)
        .replace("{ext}", ext_full)
    )
    import re as _re
    new_fn = _re.sub(r"\s*\(\s*\)", "", new_fn)
    new_fn = _re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
    new_fn = _re.sub(r"\s+(?=\.)", "", new_fn).strip()

    item.metadata = {
        "id": tid,
        "provider": "tmdb" if use_tmdb else "bgm",
        "title": safe_t,
        "year": year,
        "ep_title": ep_n or f"第 {e} 集",
        "overview": "",
        "ep_plot": ep_p,
        "s": s,
        "e": e,
        "poster": None,
        "fanart": None,
        "still": ep_s,
        "s_poster": s_p,
        "type": "episode",
        "actors": [],
        "directors": [],
        "genres": [],
        "studios": [],
        "runtime": None,
        "status": "",
        "rating": 0,
        "votes": 0,
        "release": "",
        "original_title": "",
    }
    item.new_name_only = new_fn

    root_d = ctx.target_root.get().strip() if hasattr(ctx, 'target_root') else ""
    if root_d:
        id_tag = f"tmdbid={tid}" if use_tmdb else f"bgmid={tid}"
        folder_name = safe_filename(f"{safe_t} [{id_tag}]")
        item.full_target = os.path.join(root_d, folder_name, f"Season {s}", new_fn)
    else:
        item.full_target = ""

    logger.info(f"NFO fast-path: {os.path.basename(item.path)} via {os.path.basename(nfo_path)} tid={tid}")
    return True


class _MediaHandler(FileSystemEventHandler):
    """watchdog handler that queues newly created / moved-in media files."""

    def __init__(self, watcher: "FolderWatcher"):
        super().__init__()
        self.watcher = watcher

    def on_created(self, event):
        if not event.is_directory:
            self.watcher.enqueue(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self.watcher.enqueue(event.dest_path)


class FolderWatcher:
    """Manages watchdog observers for all enabled MonitorFolder rows and
    processes new files through the recognition + archive pipeline.
    """

    def __init__(self, broadcast_fn=None):
        """
        Parameters
        ----------
        broadcast_fn : callable(dict), optional
            Called with a status-update dict whenever a ScrapeRecord changes.
            Typically wired to the WebSocket hub.
        """
        self._broadcast = broadcast_fn or (lambda d: None)
        self._observer = Observer()
        self._observer.daemon = True
        self._watches: Dict[int, object] = {}  # folder_id -> ObservedWatch
        self._pending: Dict[str, float] = {}  # path -> last event time
        self._pending_lock = threading.Lock()
        self._processed: Set[str] = set()
        self._pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="scrape")
        self._running = False
        self._worker_ctx: Optional[WorkerContext] = None
        self._debounce_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._symlink_export_paths: Set[str] = set()
        self._tg_batcher = NotificationBatcher(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._running = True
        self._worker_ctx = WorkerContext()
        self._observer = Observer()
        self._observer.daemon = True
        self._observer.start()
        self._debounce_thread = threading.Thread(target=self._debounce_loop, daemon=True)
        self._debounce_thread.start()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        self._sync_watches()
        logger.info("FolderWatcher started")

    def stop(self):
        self._running = False
        try:
            self._observer.stop()
            self._observer.join(timeout=3)
        except Exception:
            pass
        self._watches.clear()
        self._pool.shutdown(wait=False)
        logger.info("FolderWatcher stopped")

    def _sync_watches(self):
        """Synchronize watchdog watches with the database."""
        db = SessionLocal()
        try:
            folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
            # Cache symlink_export folder paths for enqueue bypass
            self._symlink_export_paths = {
                os.path.normpath(f.path) for f in folders
                if getattr(f, 'organize_mode', 'move') == 'symlink_export'
            }
            active_ids = set()
            for f in folders:
                active_ids.add(f.id)
                if f.id not in self._watches and os.path.isdir(f.path):
                    try:
                        w = self._observer.schedule(
                            _MediaHandler(self), f.path, recursive=True
                        )
                        self._watches[f.id] = w
                        logger.info(f"Watching: {f.path}")
                    except Exception as e:
                        logger.error(f"Failed to watch {f.path}: {e}")

            # Remove watches for disabled / deleted folders
            for fid in list(self._watches):
                if fid not in active_ids:
                    try:
                        self._observer.unschedule(self._watches[fid])
                    except Exception:
                        pass
                    del self._watches[fid]
        finally:
            db.close()

    def refresh(self):
        """Called after monitor folder CRUD to resync watches."""
        self._sync_watches()

    # ------------------------------------------------------------------
    # Enqueue / debounce
    # ------------------------------------------------------------------

    def enqueue(self, path: str):
        """Called by the watchdog handler for each new file event."""
        if not self._worker_ctx:
            return
        # Bypass extension filter for symlink_export folders (all files)
        norm = os.path.normpath(path)
        is_symlink_export = any(
            norm.startswith(p + os.sep) or norm == p
            for p in self._symlink_export_paths
        )
        if not is_symlink_export:
            exts = self._worker_ctx.get_media_exts()
            if not path.lower().endswith(exts):
                return
        with self._pending_lock:
            if norm in self._processed:
                return
            self._pending[norm] = time.time()

    def _debounce_loop(self):
        while self._running:
            time.sleep(1.0)
            now = time.time()
            ready = []
            with self._pending_lock:
                for p, t in list(self._pending.items()):
                    if now - t >= _DEBOUNCE_SECONDS:
                        ready.append(p)
                        del self._pending[p]
            for p in ready:
                with self._pending_lock:
                    if p in self._processed:
                        continue
                    self._processed.add(p)
                self._pool.submit(self._process_file, p)
                time.sleep(0.1)  # 避免批量提交瞬间占满线程池队列，降低 CPU 峰值

    def _poll_loop(self):
        """Periodically scan all enabled folders for new files not yet recorded.
        This catches files written over the network where watchdog events are not delivered.
        """
        while self._running:
            time.sleep(_POLL_INTERVAL_SECONDS)
            if not self._running:
                break
            try:
                self._poll_once()
            except Exception as e:
                logger.error(f"Poll error: {e}")

    def _poll_once(self):
        """Single pass: walk enabled folders and enqueue any file not yet in ScrapeRecord."""
        if not self._worker_ctx:
            return
        exts = self._worker_ctx.get_media_exts()
        db = SessionLocal()
        try:
            folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
            for folder in folders:
                if not os.path.isdir(folder.path):
                    continue
                is_sl_export = getattr(folder, 'organize_mode', 'move') == 'symlink_export'
                skip_scraped = getattr(folder, 'skip_if_scraped', False) and not is_sl_export
                # 批量加载已记录路径，避免逐文件查 DB（N+1 问题）
                if is_sl_export:
                    recorded = set(
                        r.original_path for r in
                        db.query(SymlinkRecord.original_path)
                        .filter(SymlinkRecord.folder_id == folder.id).all()
                    )
                else:
                    recorded = set(
                        r for row in
                        db.query(ScrapeRecord.original_path, ScrapeRecord.target_path)
                        .filter(ScrapeRecord.folder_id == folder.id).all()
                        for r in (row.original_path, row.target_path) if r
                    )
                for dirpath, _, filenames in os.walk(folder.path):
                    for fn in filenames:
                        if not is_sl_export and not fn.lower().endswith(exts):
                            continue
                        full = os.path.normpath(os.path.join(dirpath, fn))
                        with self._pending_lock:
                            if full in self._processed or full in self._pending:
                                continue
                        # Skip files that already have a sibling .nfo (video) or scraped dir marker (sub/audio)
                        _sub_exts_poll = self._worker_ctx.get_sub_audio_exts() if self._worker_ctx else ()
                        if skip_scraped and _is_already_scraped(full, _sub_exts_poll):
                            with self._pending_lock:
                                self._processed.add(full)
                            continue
                        # Check against pre-loaded set
                        if full in recorded:
                            with self._pending_lock:
                                self._processed.add(full)
                            continue
                        # New file — enqueue via debounce
                        with self._pending_lock:
                            self._pending[full] = time.time()
                        logger.debug(f"Poll found new file: {full}")
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Full scan
    # ------------------------------------------------------------------

    def scan_folder(self, folder_id: int):
        """Manually trigger a full scan of one monitored folder."""
        db = SessionLocal()
        try:
            folder = db.query(MonitorFolder).get(folder_id)
            if not folder or not os.path.isdir(folder.path):
                return
            exts = self._worker_ctx.get_media_exts() if self._worker_ctx else ()
            is_sl_export = getattr(folder, 'organize_mode', 'move') == 'symlink_export'
            skip_scraped = getattr(folder, 'skip_if_scraped', False) and not is_sl_export
            # 批量加载已记录路径，避免逐文件查 DB（N+1 问题）
            if is_sl_export:
                recorded = set(
                    r.original_path for r in
                    db.query(SymlinkRecord.original_path)
                    .filter(SymlinkRecord.folder_id == folder.id).all()
                )
            else:
                recorded = set(
                    r for row in
                    db.query(ScrapeRecord.original_path)
                    .filter(ScrapeRecord.folder_id == folder.id).all()
                    for r in (row.original_path,) if r
                )
            for dirpath, _, filenames in os.walk(folder.path):
                for fn in filenames:
                    if not is_sl_export and not fn.lower().endswith(exts):
                        continue
                    full = os.path.normpath(os.path.join(dirpath, fn))
                    # Skip files that already have a sibling .nfo (video) or scraped dir marker (sub/audio)
                    _sub_exts_scan = self._worker_ctx.get_sub_audio_exts() if self._worker_ctx else ()
                    if skip_scraped and _is_already_scraped(full, _sub_exts_scan):
                        with self._pending_lock:
                            self._processed.add(full)
                        # 写 skipped 记录，重启后不会再重复判断
                        skip_rec = ScrapeRecord(
                            folder_id=folder.id,
                            original_path=full,
                            original_name=os.path.basename(full),
                            status="skipped",
                            error_msg="已有元数据（.nfo），跳过刮削",
                        )
                        db.add(skip_rec)
                        db.commit()
                        continue
                    # Skip already-recorded files
                    if full in recorded:
                        continue
                    with self._pending_lock:
                        if full not in self._processed:
                            self._processed.add(full)
                    self._pool.submit(self._process_file, full)
                    time.sleep(0.1)
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _find_folder(self, path: str, db) -> Optional[MonitorFolder]:
        """Find the MonitorFolder that owns *path*."""
        folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
        norm = os.path.normpath(path)
        best = None
        for f in folders:
            fp = os.path.normpath(f.path)
            if norm.startswith(fp + os.sep) or norm == fp:
                if best is None or len(fp) > len(os.path.normpath(best.path)):
                    best = f
        return best

    def _process_file(self, path: str):
        """Run the full recognition + archive pipeline for a single file."""
        if not os.path.isfile(path):
            return

        db = SessionLocal()
        try:
            # Check for duplicate
            folder = self._find_folder(path, db)

            # For symlink_export, check SymlinkRecord instead of ScrapeRecord
            organize_mode_check = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'
            if organize_mode_check == 'symlink_export':
                existing = db.query(SymlinkRecord).filter(SymlinkRecord.original_path == path).first()
            else:
                existing = db.query(ScrapeRecord).filter(ScrapeRecord.original_path == path).first()
            if existing:
                return

            # skip_if_scraped: 文件旁已有同名 .nfo（视频）或目录内有 season.nfo/集数 .nfo（字幕/音频）则跳过
            is_sl_export_check = organize_mode_check == 'symlink_export'
            _sub_exts_skip = self._worker_ctx.get_sub_audio_exts() if self._worker_ctx else ()
            if (not is_sl_export_check
                    and folder
                    and getattr(folder, 'skip_if_scraped', False)
                    and _is_already_scraped(path, _sub_exts_skip)):
                logger.info(f"跳过已有元数据（.nfo）的文件: {path}")
                record = ScrapeRecord(
                    folder_id=folder.id,
                    original_path=path,
                    original_name=os.path.basename(path),
                    status="skipped",
                    error_msg="已有元数据（.nfo），跳过刮削",
                )
                db.add(record)
                db.commit()
                db.refresh(record)
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            # Decimal episode (e.g. 4.5) = 总集篇 — skip
            from utils.helpers import is_decimal_episode
            pure_name = os.path.splitext(os.path.basename(path))[0]
            # ---- symlink_export mode: write SymlinkRecord, no scraping ----
            organize_mode_early = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

            if is_decimal_episode(pure_name) and organize_mode_early != 'symlink_export':
                logger.info(f"\u8df3\u8fc7\u5c0f\u6570\u96c6\uff08\u603b\u96c6\u7bc7\uff09: {path}")
                record = ScrapeRecord(
                    folder_id=folder.id if folder else None,
                    original_path=path,
                    original_name=os.path.basename(path),
                    status="skipped",
                    error_msg="\u5c0f\u6570\u96c6\uff08\u603b\u96c6\u7bc7\uff09\uff0c\u5df2\u8df3\u8fc7",
                )
                db.add(record)
                db.commit()
                db.refresh(record)
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            # ---- symlink_export mode: write SymlinkRecord, no scraping ----
            if organize_mode_early == 'symlink_export' and folder:
                target_root = (folder.target_root or '').strip()
                if not target_root or not os.path.isdir(target_root):
                    slr = SymlinkRecord(
                        folder_id=folder.id,
                        original_path=path,
                        link_path="",
                        status="failed",
                        error_msg="导出软链接模式需要设置有效的归档目标目录",
                    )
                    db.add(slr); db.commit(); db.refresh(slr)
                    self._broadcast({"type": "symlink_update", "data": _symlink_record_to_dict(slr)})
                    return
                rel = os.path.relpath(path, os.path.normpath(folder.path))
                link = os.path.join(target_root, rel)
                if os.path.lexists(link):  # lexists=True even for broken symlinks
                    slr = SymlinkRecord(
                        folder_id=folder.id,
                        original_path=path,
                        link_path=link,
                        status="success",
                        error_msg="软链接已存在",
                    )
                    db.add(slr); db.commit(); db.refresh(slr)
                    self._broadcast({"type": "symlink_update", "data": _symlink_record_to_dict(slr)})
                    return
                try:
                    os.makedirs(os.path.dirname(link), exist_ok=True)
                    try:
                        os.symlink(os.path.abspath(path), link)
                        logger.info(f"Symlink export: {link} -> {path}")
                    except OSError as _sym_err:
                        shutil.copy2(path, link)
                        logger.warning(f"Symlink failed ({_sym_err}), copied instead: {link}")
                    slr = SymlinkRecord(
                        folder_id=folder.id,
                        original_path=path,
                        link_path=link,
                        status="success",
                    )
                    db.add(slr); db.commit(); db.refresh(slr)
                    self._broadcast({"type": "symlink_update", "data": _symlink_record_to_dict(slr)})
                except Exception as e:
                    logger.error(f"Symlink export failed for {path}: {e}")
                    slr = SymlinkRecord(
                        folder_id=folder.id,
                        original_path=path,
                        link_path=link,
                        status="failed",
                        error_msg=f"创建软链接失败: {e}",
                    )
                    db.add(slr); db.commit(); db.refresh(slr)
                    self._broadcast({"type": "symlink_update", "data": _symlink_record_to_dict(slr)})
                return

            # Create record
            record = ScrapeRecord(
                folder_id=folder.id if folder else None,
                original_path=path,
                original_name=os.path.basename(path),
                status="processing",
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            self._broadcast({"type": "record_update", "data": _record_to_dict(record)})

            # Build a per-call WorkerContext to avoid concurrent mutation of shared state.
            # Each thread gets its own copy of the config; the global _worker_ctx is only
            # used for extension filtering (enqueue / _poll_once).
            if not self._worker_ctx:
                return
            ctx = WorkerContext(config=dict(self._worker_ctx._cfg))
            # 共享目录缓存：同目录第二个文件可复用 AI 识别结果，避免重复调用 AI
            ctx.dir_cache = self._worker_ctx.dir_cache
            ctx.cache_lock = self._worker_ctx.cache_lock

            # Apply folder-level overrides onto this thread-local ctx
            if folder:
                if folder.target_root:
                    ctx.target_root.set(folder.target_root)
                # rename 模式：以监控目录本身作为归档根目录，必须在 _process_task 之前设置
                # 否则 _process_task 内 root_d 为空，full_target 不会生成正确的层级结构
                if getattr(folder, 'organize_mode', 'move') == 'rename':
                    ctx.target_root.set(folder.path)
                if folder.data_source:
                    ctx.source_var.set(folder.data_source)
                if folder.media_type == "movie":
                    ctx.media_type_override.set("电影")
                elif folder.media_type == "tv":
                    ctx.media_type_override.set("电视剧")
                else:
                    ctx.media_type_override.set("自动判断")

            item = MediaItem(
                id=str(uuid.uuid4()),
                path=path,
                dir=os.path.dirname(path),
                old_name=os.path.basename(path),
                ext=os.path.splitext(path)[1],
            )
            ctx.file_list = [item]

            # === NFO fast-path for subtitle/audio sidecar files ===
            # If the parent Season folder already has a tvshow.nfo with a TMDB ID,
            # we can skip the full recognition pipeline and directly fetch episode meta.
            _nfo_fast_path_done = False
            _sub_exts_fp = ctx.get_sub_audio_exts()
            if os.path.basename(path).lower().endswith(_sub_exts_fp):
                _nfo_fast_path_done = _try_nfo_fast_path(item, ctx)

            # === Recognition (process_task) ===
            if not _nfo_fast_path_done:
                try:
                    _process_task(ctx, 0)
                except Exception as e:
                    logger.error(f"Recognition failed for {path}: {e}")
                    record.status = "failed"
                    record.error_msg = str(e)[:500]
                    db.commit()
                    self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                    return

            # Check recognition result
            tid = (item.metadata or {}).get("id", "None")
            if tid == "None" or not item.new_name_only:
                record.status = "pending_manual"
                record.matched_title = (item.metadata or {}).get("title")
                record.error_msg = "无法自动识别"
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            # === Archive (move + sidecar) ===
            try:
                organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

                target = item.full_target or os.path.join(item.dir, item.new_name_only)
                target_dir = os.path.dirname(target)
                if target_dir:
                    os.makedirs(target_dir, exist_ok=True)

                if os.path.normcase(item.path) != os.path.normcase(target):
                    if os.path.exists(target):
                        record.status = "failed"
                        record.error_msg = "目标文件已存在"
                        db.commit()
                        self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                        return
                    src_dir = os.path.dirname(item.path)

                    if organize_mode == 'copy':
                        shutil.copy2(item.path, target)
                    elif organize_mode == 'symlink':
                        os.symlink(os.path.abspath(item.path), target)
                    elif organize_mode == 'hardlink':
                        os.link(item.path, target)
                    else:
                        # move / rename — both use shutil.move
                        shutil.move(item.path, target)

                    # For modes that keep the source file, don't clean up source dirs
                    if organize_mode not in ('copy', 'symlink', 'hardlink'):
                        item.path = target
                        watch_root = os.path.normpath(folder.path) if folder else None
                        _remove_empty_dirs(src_dir, stop_at=watch_root)
                    else:
                        # Update item.path to the target for sidecar writing
                        item.path = target

                # Write sidecar files
                ctx._write_sidecar_files(item, target)

                record.status = "success"
                record.matched_title = (item.metadata or {}).get("title")
                record.matched_id = str(tid)
                record.matched_provider = (item.metadata or {}).get("provider")
                record.target_path = target
                record.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                logger.info(f"Archived: {os.path.basename(path)} -> {target}")

                # Telegram batch notification
                try:
                    self._tg_batcher.add(
                        folder.id if folder else 0,
                        os.path.basename(folder.path) if folder else "",
                        item,
                    )
                except Exception as _tg_err:
                    logger.debug(f"TG 通知排队失败: {_tg_err}")

            except Exception as e:
                logger.error(f"Archive failed for {path}: {e}")
                record.status = "failed"
                record.error_msg = str(e)[:500]
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})

        except Exception as e:
            logger.error(f"Unexpected error processing {path}: {e}")
        finally:
            db.close()


def _remove_empty_dirs(start_dir: str, stop_at: Optional[str] = None):
    """Walk upward from *start_dir* removing each directory that is empty.
    Stops before removing *stop_at* (the monitored root folder itself).
    """
    current = os.path.normpath(start_dir)
    while True:
        if stop_at and os.path.normcase(current) == os.path.normcase(stop_at):
            break  # never remove the watch root itself
        parent = os.path.dirname(current)
        if parent == current:
            break  # filesystem root
        try:
            if os.path.isdir(current) and not os.listdir(current):
                os.rmdir(current)
                logger.debug(f"Removed empty dir: {current}")
            else:
                break  # directory not empty, stop climbing
        except Exception as e:
            logger.warning(f"Could not remove dir {current}: {e}")
            break
        current = parent


def _symlink_record_to_dict(r: SymlinkRecord) -> dict:
    return {
        "id": r.id,
        "folder_id": r.folder_id,
        "original_path": r.original_path,
        "link_path": r.link_path,
        "status": r.status,
        "error_msg": r.error_msg,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _record_to_dict(record: ScrapeRecord) -> dict:
    _parse_source = None
    if record.metadata_json:
        try:
            import json as _json
            _parse_source = _json.loads(record.metadata_json).get("parse_source")
        except Exception:
            pass
    return {
        "id": record.id,
        "folder_id": record.folder_id,
        "original_path": record.original_path,
        "original_name": record.original_name,
        "status": record.status,
        "matched_title": record.matched_title,
        "matched_id": record.matched_id,
        "matched_provider": record.matched_provider,
        "target_path": record.target_path,
        "parse_source": _parse_source,
        "error_msg": record.error_msg,
        "created_at": record.created_at.isoformat() if record.created_at else None,
        "updated_at": record.updated_at.isoformat() if record.updated_at else None,
    }
