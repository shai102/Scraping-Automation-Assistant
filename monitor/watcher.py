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
from utils.helpers import (
    build_existing_library_target,
    invalidate_cache_prefix,
    metadata_is_incomplete,
    metadata_missing_fields,
    normalize_parse_source,
)
from utils.telegram_notify import NotificationBatcher
from utils.emby_notify import EmbyNotifier

logger = logging.getLogger(__name__)

# Debounce: wait this many seconds after last event before processing a file
_DEBOUNCE_SECONDS = 5.0

# Polling: scan folders every N seconds to catch network-written files
_POLL_INTERVAL_SECONDS = 30.0

# Metadata refresh: default interval (12 hours) and lookback (14 days)
_METADATA_REFRESH_DEFAULT_INTERVAL_HOURS = 12
_METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS = 14


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


def _symlink_record_needs_repair(row) -> bool:
    if not row or str(getattr(row, "status", "") or "").lower() != "success":
        return False
    original_path = str(getattr(row, "original_path", "") or "").strip()
    link_path = str(getattr(row, "link_path", "") or "").strip()
    if not original_path or not os.path.isfile(original_path):
        return False
    if not link_path:
        return True
    return not os.path.lexists(link_path)


def _symlink_record_consumed_downstream(row, db, ctx) -> bool:
    """Return True when a missing exported link was intentionally moved/renamed
    by another monitored folder and already has a healthy downstream record.
    """
    if db is None:
        return False

    link_path = str(getattr(row, "link_path", "") or "").strip()
    if not link_path:
        return False

    downstream = (
        db.query(ScrapeRecord)
        .filter(
            ScrapeRecord.original_path == link_path,
            ScrapeRecord.status == "success",
        )
        .order_by(ScrapeRecord.id.desc())
        .first()
    )
    if not downstream:
        return False

    return not _scrape_record_needs_repair(downstream, ctx)


def _scrape_record_needs_repair(row, ctx) -> bool:
    if not row or str(getattr(row, "status", "") or "").lower() != "success":
        return False

    target_path = str(getattr(row, "target_path", "") or "").strip()
    original_path = str(getattr(row, "original_path", "") or "").strip()
    sub_audio_exts = ctx.get_sub_audio_exts() if ctx else ()

    if target_path and os.path.isfile(target_path):
        return not _is_already_scraped(target_path, sub_audio_exts)

    if original_path and os.path.isfile(original_path):
        return True

    return False


def _reset_scrape_record_for_rebuild(record):
    record._repairing = True
    record._previous_target = str(getattr(record, "target_path", "") or "")
    record.status = "processing"
    record.matched_title = None
    record.matched_id = None
    record.matched_provider = None
    record.metadata_json = None
    record.error_msg = None


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
    raw_e = extract_episode_number(pure_name, g)
    if raw_e is None:
        raw_e = g.get("episode")
    if isinstance(raw_e, list):
        raw_e = raw_e[0]
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

    new_fn, media_suffix = ctx._render_media_filename(
        ctx.tv_format.get(),
        title=safe_t,
        year=year,
        season=s_fmt,
        episode=e_fmt,
        ep_name=safe_ep,
        ext=ext_full,
        source_filename=item.old_name,
        pure_name=pure_name,
        source_provider="tmdb" if use_tmdb else "bgm",
        media_id=tid,
        is_tv=True,
    )

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
        "media_suffix": media_suffix,
    }
    item.media_suffix = media_suffix
    item.new_name_only = new_fn

    root_d = ctx.target_root.get().strip() if hasattr(ctx, 'target_root') else ""
    if root_d:
        preserved_target = ""
        preserve_var = getattr(ctx, "preserve_existing_folder", None)
        if preserve_var is not None:
            getter = getattr(preserve_var, "get", None)
            preserve_enabled = bool(getter()) if callable(getter) else bool(preserve_var)
            if preserve_enabled:
                preserved_target = build_existing_library_target(
                    item.path, new_fn, item.metadata
                )
        if preserved_target:
            item.full_target = preserved_target
        else:
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

    def on_deleted(self, event):
        if event.is_directory:
            self.watcher.on_dir_deleted(event.src_path)
        else:
            self.watcher.on_file_deleted(event.src_path)


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
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scrape")
        self._symlink_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="symlink-export")
        self._dir_gate = threading.Condition()
        self._active_dirs: Set[str] = set()
        self._running = False
        self._worker_ctx: Optional[WorkerContext] = None
        self._debounce_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._metadata_refresh_thread: Optional[threading.Thread] = None
        self._symlink_export_paths: Set[str] = set()
        self._tg_batcher = NotificationBatcher(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )
        self._emby_notifier = EmbyNotifier(
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
        self._refresh_pool_workers()
        self._observer = Observer()
        self._observer.daemon = True
        self._observer.start()
        self._debounce_thread = threading.Thread(target=self._debounce_loop, daemon=True)
        self._debounce_thread.start()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        self._metadata_refresh_thread = threading.Thread(
            target=self._metadata_refresh_loop, daemon=True
        )
        self._metadata_refresh_thread.start()
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
        self._symlink_pool.shutdown(wait=False)
        logger.info("FolderWatcher stopped")

    def _desired_pool_workers(self) -> int:
        if self._worker_ctx:
            return self._worker_ctx._get_preview_workers()
        return 1

    def _desired_symlink_pool_workers(self) -> int:
        if self._worker_ctx:
            return self._worker_ctx._get_symlink_export_workers()
        return 3

    def _refresh_pool_workers(self):
        desired = self._desired_pool_workers()
        current = getattr(self._pool, "_max_workers", None)
        if current != desired:
            old_pool = self._pool
            self._pool = ThreadPoolExecutor(max_workers=desired, thread_name_prefix="scrape")
            try:
                old_pool.shutdown(wait=False)
            except Exception:
                pass

        desired_symlink = self._desired_symlink_pool_workers()
        current_symlink = getattr(self._symlink_pool, "_max_workers", None)
        if current_symlink != desired_symlink:
            old_symlink_pool = self._symlink_pool
            self._symlink_pool = ThreadPoolExecutor(max_workers=desired_symlink, thread_name_prefix="symlink-export")
            try:
                old_symlink_pool.shutdown(wait=False)
            except Exception:
                pass

    def reload_runtime_config(self):
        if self._worker_ctx:
            self._worker_ctx.reload_config()
            self._refresh_pool_workers()

    def _acquire_dir_slot(self, path: str) -> str:
        dir_key = os.path.normcase(os.path.normpath(os.path.dirname(path) or path))
        with self._dir_gate:
            while dir_key in self._active_dirs:
                self._dir_gate.wait(timeout=0.5)
            self._active_dirs.add(dir_key)
        return dir_key

    def _release_dir_slot(self, dir_key: str):
        with self._dir_gate:
            self._active_dirs.discard(dir_key)
            self._dir_gate.notify_all()

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
    # Deletion sync
    # ------------------------------------------------------------------

    def on_file_deleted(self, path: str):
        """watchdog 回调：监控路径内有文件被删除时触发，提交到线程池处理。"""
        norm = os.path.normpath(path)
        self._pool.submit(self._handle_deleted, norm)

    def on_dir_deleted(self, path: str):
        """watchdog 回调：监控路径内有目录被删除时触发，提交到线程池处理。"""
        norm = os.path.normpath(path)
        self._pool.submit(self._handle_dir_deleted, norm)

    def _handle_dir_deleted(self, dir_path: str):
        """同步删除导出目标目录中该目录下所有对应文件及其伴随文件，并清理空目录。

        支持的整理模式：
        - symlink_export : 删除目标目录中的软链接/复制文件，清理空目录
        - copy / symlink / hardlink : 删除目标文件及同名 NFO/图片，清理空目录
        - move / rename : 源文件已被移走，忽略
        """
        db = SessionLocal()
        try:
            folder = self._find_folder(dir_path, db)
            if not folder:
                return

            organize_mode = getattr(folder, 'organize_mode', 'move') or 'move'
            stop_root = (folder.target_root or '').strip() or None

            # 匹配前缀：dir_path\ 下的所有文件（含子目录）
            prefix = dir_path + os.sep

            if organize_mode == 'symlink_export':
                slrs = db.query(SymlinkRecord).filter(
                    SymlinkRecord.folder_id == folder.id
                ).all()
                slrs_matched = [r for r in slrs if r.original_path and
                        os.path.normpath(r.original_path).startswith(prefix)]

                # ── 文件系统兑备：若 DB 无匹配记录，直接按 target_root+相对路径推算并删除 ──
                target_root_fs = (folder.target_root or '').strip()
                folder_norm = os.path.normpath(folder.path)
                if not slrs_matched and target_root_fs and os.path.isdir(target_root_fs):
                    rel = os.path.relpath(dir_path, folder_norm)
                    link_dir_fs = os.path.normpath(os.path.join(target_root_fs, rel))
                    if os.path.isdir(link_dir_fs):
                        try:
                            import shutil as _shutil
                            _shutil.rmtree(link_dir_fs)
                            logger.info(f"文件系统兑备删除软链接目录: {link_dir_fs} (DB 无记录)")
                        except Exception as e:
                            logger.warning(f"兑备删除软链接目录失败 {link_dir_fs}: {e}")
                    self._broadcast({
                        "type": "dir_deleted",
                        "data": {"original_path": dir_path, "mode": organize_mode},
                    })
                    return

                deleted_dirs: Set[str] = set()
                link_paths: list = []  # 收集所有软链接路径，用于链式追查
                for slr in slrs_matched:
                    link = slr.link_path
                    if link and os.path.lexists(link):
                        try:
                            os.remove(link)
                            logger.info(f"同步删除软链接: {link} (源目录已删除: {dir_path})")
                        except Exception as e:
                            logger.warning(f"删除软链接失败 {link}: {e}")
                    if link:
                        deleted_dirs.add(os.path.dirname(link))
                        link_paths.append(link)
                    with self._pending_lock:
                        self._processed.discard(os.path.normpath(slr.original_path))
                    db.delete(slr)
                db.commit()

                # ── 链式追查：软链接若已被第二级监控目录刮削整理，同步删除刮削后的目标文件 ──
                for link in link_paths:
                    scraped = db.query(ScrapeRecord).filter(
                        ScrapeRecord.original_path == link,
                        ScrapeRecord.status == 'success',
                    ).first()
                    if scraped and scraped.target_path:
                        s_target = scraped.target_path
                        s_folder = db.query(MonitorFolder).get(scraped.folder_id) if scraped.folder_id else None
                        s_stop = (s_folder.target_root or '').strip() or None if s_folder else None
                        if os.path.exists(s_target) or os.path.lexists(s_target):
                            try:
                                os.remove(s_target)
                                logger.info(f"链式删除刮削目标: {s_target} (源目录软链接: {link})")
                            except Exception as e:
                                logger.warning(f"链式删除刮削目标失败 {s_target}: {e}")
                        _delete_per_file_sidecars(s_target)
                        deleted_dirs.add((os.path.dirname(s_target), s_stop))
                        db.delete(scraped)
                db.commit()

                # 清理空目录（兼容两种 entry：纯路径字符串 或 (路径, stop) 元组）
                for entry in sorted(deleted_dirs, key=lambda x: len(x[0]) if isinstance(x, tuple) else len(x), reverse=True):
                    if isinstance(entry, tuple):
                        _remove_empty_dirs(entry[0], stop_at=entry[1])
                    else:
                        _remove_empty_dirs(entry, stop_at=stop_root)
                self._broadcast({
                    "type": "dir_deleted",
                    "data": {"original_path": dir_path, "mode": organize_mode},
                })

            elif organize_mode in ('copy', 'symlink', 'hardlink'):
                recs = db.query(ScrapeRecord).filter(
                    ScrapeRecord.folder_id == folder.id,
                    ScrapeRecord.status == 'success',
                ).all()
                recs = [r for r in recs if r.original_path and
                        os.path.normpath(r.original_path).startswith(prefix)]
                deleted_dirs: Set[str] = set()
                for rec in recs:
                    target = rec.target_path
                    if target and (os.path.exists(target) or os.path.lexists(target)):
                        try:
                            os.remove(target)
                            logger.info(f"同步删除目标文件: {target} (源目录已删除: {dir_path})")
                        except Exception as e:
                            logger.warning(f"删除目标文件失败 {target}: {e}")
                        _delete_per_file_sidecars(target)
                        deleted_dirs.add(os.path.dirname(target))
                    with self._pending_lock:
                        self._processed.discard(os.path.normpath(rec.original_path))
                    db.delete(rec)
                db.commit()
                for d in sorted(deleted_dirs, key=len, reverse=True):
                    _remove_empty_dirs(d, stop_at=stop_root)
                self._broadcast({
                    "type": "dir_deleted",
                    "data": {"original_path": dir_path, "mode": organize_mode},
                })

            # move / rename 模式：源文件已被移走，整理完成后不再监听原路径的删除事件

        except Exception as e:
            logger.error(f"处理目录删除事件失败 {dir_path}: {e}")
        finally:
            db.close()

    def _handle_deleted(self, path: str):
        """同步删除导出目标目录中对应的文件及其伴随文件。

        支持的整理模式：
        - symlink_export : 删除目标目录中的软链接/复制文件
        - copy / symlink / hardlink : 删除目标目录中的归档文件及同名 NFO/图片
        - move / rename : 源文件已被移走，忽略删除事件
        """
        db = SessionLocal()
        try:
            folder = self._find_folder(path, db)
            if not folder:
                return

            organize_mode = getattr(folder, 'organize_mode', 'move') or 'move'
            stop_root = (folder.target_root or '').strip() or None

            if organize_mode == 'symlink_export':
                slr = db.query(SymlinkRecord).filter(
                    SymlinkRecord.original_path == path
                ).first()

                # ── 推算软链接路径：DB 有记录用记录，无记录用文件系统兼容逻辑推算 ──
                if slr:
                    link = slr.link_path
                else:
                    # DB 记录已被清空：根据 target_root + 相对路径直接推算
                    target_root = (folder.target_root or '').strip()
                    folder_path = os.path.normpath(folder.path)
                    if target_root and path.startswith(folder_path):
                        rel = os.path.relpath(path, folder_path)
                        link = os.path.join(target_root, rel)
                    else:
                        link = None

                if link and os.path.lexists(link):
                    try:
                        os.remove(link)
                        logger.info(f"同步删除软链接: {link} (源文件已删除: {path})")
                    except Exception as e:
                        logger.warning(f"删除软链接失败 {link}: {e}")
                link_dir = os.path.dirname(link) if link else None
                if slr:
                    db.delete(slr)
                    db.commit()

                # ── 链式追查：软链接若已被第二级监控目录刮削整理，同步删除刮削后的目标文件 ──
                if link:
                    scraped = db.query(ScrapeRecord).filter(
                        ScrapeRecord.original_path == link,
                        ScrapeRecord.status == 'success',
                    ).first()
                    if scraped and scraped.target_path:
                        s_target = scraped.target_path
                        s_folder = db.query(MonitorFolder).get(scraped.folder_id) if scraped.folder_id else None
                        s_stop = (s_folder.target_root or '').strip() or None if s_folder else None
                        if os.path.exists(s_target) or os.path.lexists(s_target):
                            try:
                                os.remove(s_target)
                                logger.info(f"链式删除刮削目标: {s_target} (源软链接: {link})")
                            except Exception as e:
                                logger.warning(f"链式删除刮削目标失败 {s_target}: {e}")
                        _delete_per_file_sidecars(s_target)
                        s_dir = os.path.dirname(s_target)
                        db.delete(scraped)
                        db.commit()
                        _remove_empty_dirs(s_dir, stop_at=s_stop)
                    elif not scraped:
                        logger.debug(f"链式追查：未找到刮削记录 (软链接已删除或记录已清空): {link}")

                if link_dir:
                    _remove_empty_dirs(link_dir, stop_at=stop_root)
                self._broadcast({
                    "type": "symlink_deleted",
                    "data": {"original_path": path, "link_path": link},
                })

                if not slr and not link:
                    return

            elif organize_mode in ('copy', 'symlink', 'hardlink'):
                rec = db.query(ScrapeRecord).filter(
                    ScrapeRecord.original_path == path,
                    ScrapeRecord.status == 'success',
                ).first()
                if not rec or not rec.target_path:
                    return
                target = rec.target_path
                if os.path.exists(target) or os.path.lexists(target):
                    try:
                        os.remove(target)
                        logger.info(f"同步删除目标文件: {target} (源文件已删除: {path})")
                    except Exception as e:
                        logger.warning(f"删除目标文件失败 {target}: {e}")
                _delete_per_file_sidecars(target)
                target_dir = os.path.dirname(target)
                db.delete(rec)
                db.commit()
                _remove_empty_dirs(target_dir, stop_at=stop_root)
                self._broadcast({
                    "type": "record_deleted",
                    "data": {"original_path": path, "target_path": target},
                })

            # move / rename 模式：源文件已被移走，整理完成后不再监听原路径的删除事件

        except Exception as e:
            logger.error(f"处理文件删除事件失败 {path}: {e}")
        finally:
            # 从已处理集合中移除，允许将来同路径新文件重新入队
            with self._pending_lock:
                self._processed.discard(path)
            db.close()

    # ------------------------------------------------------------------
    # Enqueue / debounce
    # ------------------------------------------------------------------

    def _is_symlink_export_path(self, path: str) -> bool:
        norm = os.path.normpath(path)
        return any(
            norm.startswith(p + os.sep) or norm == p
            for p in self._symlink_export_paths
        )

    def enqueue(self, path: str):
        """Called by the watchdog handler for each new file event."""
        if not self._worker_ctx:
            return
        # Bypass extension filter for symlink_export folders (all files)
        norm = os.path.normpath(path)
        is_symlink_export = self._is_symlink_export_path(path)
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
                target_pool = self._symlink_pool if self._is_symlink_export_path(p) else self._pool
                target_pool.submit(self._process_file, p)
                time.sleep(0.03 if target_pool is self._symlink_pool else 0.1)  # ????????????AI ???????????

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
                    recorded = set()
                    rows = db.query(SymlinkRecord).filter(
                        SymlinkRecord.folder_id == folder.id
                    ).all()
                    for row in rows:
                        if (
                            _symlink_record_needs_repair(row)
                            and not _symlink_record_consumed_downstream(row, db, self._worker_ctx)
                        ):
                            continue
                        if row.original_path:
                            recorded.add(row.original_path)
                else:
                    recorded = set()
                    rows = db.query(ScrapeRecord).filter(
                        ScrapeRecord.folder_id == folder.id
                    ).all()
                    for row in rows:
                        if _scrape_record_needs_repair(row, self._worker_ctx):
                            continue
                        for recorded_path in (row.original_path, row.target_path):
                            if recorded_path:
                                recorded.add(recorded_path)
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
    # Metadata refresh patrol
    # ------------------------------------------------------------------

    def _metadata_refresh_loop(self):
        """Periodically scan recent successful records and refresh incomplete metadata."""
        # Wait a bit before the first run to let the server fully start
        time.sleep(60)
        while self._running:
            cfg = self._worker_ctx._cfg if self._worker_ctx else {}
            enabled = cfg.get("metadata_refresh_enabled", True)
            interval_hours = cfg.get(
                "metadata_refresh_interval_hours",
                _METADATA_REFRESH_DEFAULT_INTERVAL_HOURS,
            )
            interval_seconds = max(1800, interval_hours * 3600)  # min 30 min

            if enabled:
                try:
                    self._refresh_incomplete_records()
                except Exception as e:
                    logger.error(f"Metadata refresh error: {e}")

            # Sleep in small increments so we can exit quickly on stop
            slept = 0.0
            while slept < interval_seconds and self._running:
                time.sleep(min(30.0, interval_seconds - slept))
                slept += 30.0

    def _refresh_incomplete_records(self):
        """Single pass: find recent incomplete records and refresh their metadata."""
        if not self._worker_ctx:
            return

        cfg = self._worker_ctx._cfg if self._worker_ctx else {}
        lookback_days = cfg.get(
            "metadata_refresh_lookback_days",
            _METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS,
        )

        import datetime
        cutoff = datetime.datetime.now() - datetime.timedelta(days=lookback_days)

        db = SessionLocal()
        try:
            rows = (
                db.query(ScrapeRecord)
                .filter(
                    ScrapeRecord.status == "success",
                    ScrapeRecord.updated_at >= cutoff,
                    ScrapeRecord.metadata_json.isnot(None),
                    ScrapeRecord.matched_id.isnot(None),
                )
                .order_by(ScrapeRecord.id.desc())
                .all()
            )
            # Filter to those that are actually incomplete
            incomplete = [r for r in rows if metadata_is_incomplete(r.metadata_json or "")]
            if not incomplete:
                return

            logger.info(
                f"元数据巡检: 发现 {len(incomplete)} 条不完整记录，开始刷新"
            )

            refreshed = 0
            for record in incomplete:
                if not self._running:
                    break
                title = str(record.matched_title or record.path or "").strip()
                target_path = str(record.target_path or "").strip()
                missing_fields = metadata_missing_fields(record.metadata_json or "")
                logger.info(
                    "元数据巡检项: "
                    f"record_id={record.id} | title={title or '-'} | "
                    f"target_path={target_path or '-'} | "
                    f"missing_fields={','.join(missing_fields) or '-'}"
                )
                try:
                    updated = self._refresh_single_record(record, db)
                    if updated:
                        refreshed += 1
                        self._broadcast({
                            "type": "record_update",
                            "data": _record_to_dict(record),
                        })
                except Exception as e:
                    logger.warning(
                        "元数据刷新失败: "
                        f"record_id={record.id} | title={title or '-'} | "
                        f"target_path={target_path or '-'} | reason={e}"
                    )
                # Rate-limit friendly: small pause between records
                time.sleep(2.0)

            logger.info(
                f"元数据巡检完成: 刷新了 {refreshed}/{len(incomplete)} 条记录"
            )

        finally:
            db.close()

    def _refresh_single_record(self, record, db) -> bool:
        """Refresh metadata for a single ScrapeRecord. Returns True if updated."""
        return refresh_record_metadata(record, db, self._worker_ctx, self._broadcast)

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
                recorded = set()
                rows = db.query(SymlinkRecord).filter(
                    SymlinkRecord.folder_id == folder.id
                ).all()
                for row in rows:
                    if (
                        _symlink_record_needs_repair(row)
                        and not _symlink_record_consumed_downstream(row, db, self._worker_ctx)
                    ):
                        continue
                    if row.original_path:
                        recorded.add(row.original_path)
            else:
                recorded = set()
                rows = db.query(ScrapeRecord).filter(
                    ScrapeRecord.folder_id == folder.id
                ).all()
                for row in rows:
                    if _scrape_record_needs_repair(row, self._worker_ctx):
                        continue
                    for recorded_path in (row.original_path, row.target_path):
                        if recorded_path:
                            recorded.add(recorded_path)
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

        dir_slot = self._acquire_dir_slot(path)
        db = SessionLocal()
        try:
            # Check for duplicate
            folder = self._find_folder(path, db)
            record = None

            # For symlink_export, check SymlinkRecord instead of ScrapeRecord
            organize_mode_check = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'
            if organize_mode_check == 'symlink_export':
                from sqlalchemy import or_

                existing = db.query(SymlinkRecord).filter(
                    or_(
                        SymlinkRecord.original_path == path,
                        SymlinkRecord.link_path == path,
                    )
                ).first()
            else:
                from sqlalchemy import or_

                existing = db.query(ScrapeRecord).filter(
                    or_(
                        ScrapeRecord.original_path == path,
                        ScrapeRecord.target_path == path,
                    )
                ).first()
            if existing:
                if organize_mode_check == 'symlink_export':
                    if (
                        _symlink_record_needs_repair(existing)
                        and not _symlink_record_consumed_downstream(existing, db, self._worker_ctx)
                    ):
                        logger.info(f"检测到缺失的软链接产物，准备自动重建: {existing.original_path}")
                        db.delete(existing)
                        db.commit()
                    else:
                        return
                else:
                    if _scrape_record_needs_repair(existing, self._worker_ctx):
                        logger.info(f"检测到缺失的刮削产物，准备自动修复: {path}")
                        if existing.target_path and os.path.isfile(existing.target_path):
                            path = existing.target_path
                        record = existing
                        _reset_scrape_record_for_rebuild(record)
                        db.commit()
                        self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                    else:
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
                    # Retry up to 5 times (10 s total) for WinError 32 (file locked by another process)
                    _last_err: Optional[Exception] = None
                    for _attempt in range(5):
                        try:
                            os.symlink(os.path.abspath(path), link)
                            logger.info(f"Symlink export: {link} -> {path}")
                            _last_err = None
                            break
                        except OSError as _sym_err:
                            if getattr(_sym_err, 'winerror', None) == 32:
                                _last_err = _sym_err
                                time.sleep(2)
                                continue
                            # Not a locking error — fall back to copy once
                            try:
                                shutil.copy2(path, link)
                                logger.warning(f"Symlink failed ({_sym_err}), copied instead: {link}")
                            except OSError as _copy_err:
                                if getattr(_copy_err, 'winerror', None) == 32:
                                    _last_err = _copy_err
                                    time.sleep(2)
                                    continue
                                raise
                            _last_err = None
                            break
                    else:
                        # All retries exhausted
                        raise _last_err  # type: ignore[misc]
                    if _last_err is not None:
                        raise _last_err
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
            if record is None:
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
            # Share recognition caches across per-file contexts so files in the
            # same folder can reuse both title parsing and DB candidate picks.
            ctx.dir_cache = self._worker_ctx.dir_cache
            ctx.dir_parse_events = self._worker_ctx.dir_parse_events
            ctx.db_cache = self._worker_ctx.db_cache
            ctx.db_resolution_events = self._worker_ctx.db_resolution_events
            ctx.embedding_cache = self._worker_ctx.embedding_cache
            ctx.ollama_embed_endpoint = self._worker_ctx.ollama_embed_endpoint
            ctx.cache_lock = self._worker_ctx.cache_lock

            # Apply folder-level overrides onto this thread-local ctx
            if folder:
                if folder.target_root:
                    ctx.target_root.set(folder.target_root)
                # rename 模式：以监控目录本身作为归档根目录，必须在 _process_task 之前设置
                # 否则 _process_task 内 root_d 为空，full_target 不会生成正确的层级结构
                if getattr(folder, 'organize_mode', 'move') == 'rename':
                    ctx.target_root.set(folder.path)
                ctx.preserve_existing_folder.set(
                    getattr(folder, 'preserve_existing_folder', False)
                )
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
            logger.info(f"开始识别: {path}")

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
                meta = item.metadata or {}
                logger.warning(
                    "识别失败: %s 未匹配到 %s 媒体信息，跳过文件整理",
                    path,
                    "TMDB" if getattr(folder, "data_source", "siliconflow_tmdb") == "siliconflow_tmdb" else "BGM",
                )
                record.matched_title = meta.get("title")
                record.matched_provider = meta.get("provider")
                record.metadata_json = json.dumps(meta, ensure_ascii=False)
                if meta.get("error_code") == "rate_limited":
                    record.status = "failed"
                    record.error_msg = meta.get("error_msg") or "AI接口限流，请稍后重试"
                else:
                    record.status = "pending_manual"
                    # 优先使用匹配阶段记录的具体原因，兜底为通用提示
                    record.error_msg = meta.get("pending_reason") or "无法自动识别"
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            meta = item.metadata or {}
            logger.info(
                "识别完成: title=%s | path=%s | name=%s | year=%s | type=%s | season=%s | episode=%s | provider=%s | id=%s | parse_source=%s | resolution=%s | source=%s | video_codec=%s | audio_codec=%s | release_group=%s",
                meta.get("title") or "",
                path,
                item.new_name_only or "",
                meta.get("year") or "",
                meta.get("type") or "",
                meta.get("s") if meta.get("s") is not None else "",
                meta.get("e") if meta.get("e") is not None else "",
                meta.get("provider") or "",
                tid,
                meta.get("parse_source") or "",
                meta.get("resolution") or "",
                meta.get("source") or "",
                meta.get("video_codec") or "",
                meta.get("audio_codec") or "",
                meta.get("release_group") or "",
            )

            # === Archive (move + sidecar) ===
            try:
                organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

                target = item.full_target or os.path.join(item.dir, item.new_name_only)
                target_dir = os.path.dirname(target)
                if target_dir:
                    os.makedirs(target_dir, exist_ok=True)

                if os.path.normcase(item.path) != os.path.normcase(target):
                    target_exists = os.path.exists(target)
                    target_lexists = os.path.lexists(target)
                    is_repair_target = (
                        getattr(record, "_repairing", False)
                        and os.path.normcase(getattr(record, "_previous_target", ""))
                        == os.path.normcase(target)
                    )
                    # Check if source and target are actually the same file on disk
                    # (e.g. already organized, just needs scraping)
                    is_same_file = False
                    if target_exists and os.path.isfile(item.path):
                        try:
                            is_same_file = os.path.samefile(item.path, target)
                        except (OSError, ValueError):
                            pass
                    if is_same_file:
                        # File is already in the right place — skip move, just update path
                        item.path = target
                    elif target_lexists and is_repair_target and target_exists:
                        item.path = target
                    elif target_lexists and is_repair_target and not target_exists:
                        try:
                            os.remove(target)
                        except Exception as e:
                            record.status = "failed"
                            record.target_path = target
                            record.error_msg = f"清理失效目标失败: {e}"
                            db.commit()
                            self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                            return
                    if target_exists and not is_repair_target:
                        record.status = "failed"
                        record.target_path = target
                        record.error_msg = f"目标文件已存在: {target}"
                        db.commit()
                        self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                        return
                    src_dir = os.path.dirname(item.path)

                    if os.path.normcase(item.path) == os.path.normcase(target):
                        pass
                    elif organize_mode == 'copy':
                        shutil.copy2(item.path, target)
                    elif organize_mode == 'symlink':
                        os.symlink(os.path.abspath(item.path), target)
                    elif organize_mode == 'hardlink':
                        os.link(item.path, target)
                    else:
                        # move / rename — both use shutil.move
                        shutil.move(item.path, target)

                    # For modes that keep the source file, don't clean up source dirs
                    if os.path.normcase(item.path) != os.path.normcase(target) and organize_mode not in ('copy', 'symlink', 'hardlink'):
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

                # Emby / Jellyfin library refresh notification
                try:
                    self._emby_notifier.notify_success(target)
                except Exception as _emby_err:
                    logger.debug(f"Emby 通知失败: {_emby_err}")

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
            self._release_dir_slot(dir_slot)


def _delete_per_file_sidecars(file_path: str):
    """删除与媒体文件同名的伴随文件（NFO、缩略图等）。"""
    if not file_path:
        return
    base = os.path.splitext(file_path)[0]
    for suffix in ('.nfo', '-thumb.jpg', '-poster.jpg', '-fanart.jpg'):
        sidecar = base + suffix
        if os.path.isfile(sidecar):
            try:
                os.remove(sidecar)
                logger.debug(f"删除伴随文件: {sidecar}")
            except Exception as e:
                logger.warning(f"删除伴随文件失败 {sidecar}: {e}")


# ---------------------------------------------------------------------------
# Metadata refresh — re-fetch from TMDB/BGM and update NFO/images
# ---------------------------------------------------------------------------

def refresh_record_metadata(record, db, worker_ctx, broadcast_fn=None) -> bool:
    """Re-fetch metadata for a single ScrapeRecord and update NFO + images.

    Does NOT re-run the recognition pipeline — only refreshes metadata for an
    already-matched record. Returns True if the record was updated.

    Parameters
    ----------
    record : ScrapeRecord
        Must have status='success', matched_id set, and metadata_json populated.
    db : SQLAlchemy Session
        Active database session (caller commits or rolls back).
    worker_ctx : WorkerContext
        Runtime context with API keys and sidecar-writing methods.
    broadcast_fn : callable, optional
        WebSocket broadcast function.
    """
    from db.tmdb_api import (
        fetch_tmdb_by_id,
        fetch_tmdb_credits,
        fetch_tmdb_episode_meta,
        fetch_tmdb_season_poster,
        fetch_hybrid_episode_meta,
        fetch_bgm_by_id,
    )

    if not record.metadata_json or not record.matched_id:
        return False

    try:
        old_meta = json.loads(record.metadata_json)
    except Exception:
        return False

    mid = str(record.matched_id or "None")
    if mid == "None":
        return False

    provider = str(record.matched_provider or old_meta.get("provider") or "tmdb").strip().lower()
    is_tmdb = provider == "tmdb"
    media_type = str(old_meta.get("type") or "episode").strip().lower()
    is_tv = media_type == "episode"

    api_tmdb = worker_ctx.tmdb_api_key.get().strip() if worker_ctx else ""
    api_bgm = worker_ctx.bgm_api_key.get().strip() if worker_ctx else ""

    # Clear API cache for this ID so we get fresh data from TMDB
    if is_tmdb:
        invalidate_cache_prefix(f"tmdb_detail:{mid}_")
        invalidate_cache_prefix(f"tmdb_credits:{mid}_")
        if is_tv:
            s_num = old_meta.get("s", 1)
            e_num = old_meta.get("e", 1)
            invalidate_cache_prefix(f"tmdb_ep_v3:{mid}_")
            invalidate_cache_prefix(f"tmdb_season_poster:{mid}_")
    else:
        invalidate_cache_prefix(f"bgm_detail:{mid}_")

    # Fetch fresh detail metadata
    if is_tmdb:
        _t, _tid, _msg, detail_meta = fetch_tmdb_by_id(mid, is_tv, api_tmdb)
    else:
        _t, _tid, _msg, detail_meta = fetch_bgm_by_id(mid, api_bgm)

    if not detail_meta or _tid == "None":
        return False  # API returned nothing useful

    # Merge: only fill in fields that were empty before
    new_meta = dict(old_meta)
    for key in ("overview", "poster", "fanart", "release", "original_title", "status"):
        old_val = str(new_meta.get(key) or "").strip()
        new_val = str(detail_meta.get(key) or "").strip()
        if not old_val and new_val:
            new_meta[key] = new_val

    for key in ("genres", "studios"):
        if not new_meta.get(key) and detail_meta.get(key):
            new_meta[key] = detail_meta[key]

    for key in ("rating", "votes", "runtime"):
        try:
            old_v = float(new_meta.get(key) or 0)
        except (TypeError, ValueError):
            old_v = 0
        try:
            new_v = float(detail_meta.get(key) or 0)
        except (TypeError, ValueError):
            new_v = 0
        if old_v == 0 and new_v > 0:
            new_meta[key] = detail_meta[key]

    # Fetch credits if missing
    if is_tmdb and not new_meta.get("actors"):
        actors, directors = fetch_tmdb_credits(mid, is_tv=is_tv, api_key=api_tmdb)
        if actors:
            new_meta["actors"] = actors
        if directors and not new_meta.get("directors"):
            new_meta["directors"] = directors

    # Fetch episode-level data for TV
    if is_tv:
        s_num = new_meta.get("s", 1)
        e_num = new_meta.get("e", 1)

        ep_n, ep_p, ep_s = "", "", ""
        s_p = ""

        if is_tmdb:
            title_for_ep = new_meta.get("title") or record.matched_title or ""
            ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                mid, s_num, e_num, api_tmdb, title_for_ep, api_bgm
            )
            s_p = fetch_tmdb_season_poster(mid, s_num, api_tmdb)
        else:
            title_for_ep = new_meta.get("title") or record.matched_title or ""
            year_for_ep = new_meta.get("year")
            ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                title_for_ep, mid, s_num, e_num, api_bgm, api_tmdb, year_for_ep
            )

        # Fill missing episode fields
        import re as _re
        _GENERIC_EP_RE = _re.compile(r"^第\s*\d+\s*集$")
        old_ep_title = str(new_meta.get("ep_title") or "").strip()
        if ep_n and (not old_ep_title or _GENERIC_EP_RE.match(old_ep_title)):
            new_meta["ep_title"] = ep_n

        if ep_p and not str(new_meta.get("ep_plot") or "").strip():
            new_meta["ep_plot"] = ep_p

        if ep_s and not str(new_meta.get("still") or "").strip():
            new_meta["still"] = ep_s

        if s_p and not str(new_meta.get("s_poster") or "").strip():
            new_meta["s_poster"] = s_p

    updated_fields = sorted(
        key for key in set(old_meta.keys()) | set(new_meta.keys())
        if old_meta.get(key) != new_meta.get(key)
    )

    # Check if anything actually changed
    if not updated_fields:
        return False

    # Determine target path for sidecar refresh
    target_path = str(record.target_path or "").strip()
    if not target_path or not os.path.exists(target_path):
        # Even if the target file is gone, still update the DB record
        record.metadata_json = json.dumps(new_meta, ensure_ascii=False)
        db.commit()
        logger.info(
            "元数据刷新: "
            f"record_id={record.id} | title={new_meta.get('title') or '-'} | "
            f"id={mid} | provider={provider} | "
            f"target_path={target_path or '-'} | "
            f"updated_fields={','.join(updated_fields) or '-'}"
        )
        return True

    # Refresh NFO files and images
    ctx = WorkerContext(config=dict(worker_ctx._cfg))
    updated = ctx._refresh_sidecar_files(target_path, old_meta, new_meta)

    # Update DB record
    record.metadata_json = json.dumps(new_meta, ensure_ascii=False)
    db.commit()

    if updated:
        logger.info(
            "元数据刷新: "
            f"record_id={record.id} | title={new_meta.get('title') or '-'} | "
            f"id={mid} | provider={provider} | "
            f"target_path={target_path or '-'} | "
            f"updated_fields={','.join(updated_fields) or '-'}"
        )

    return True


# Windows / macOS auto-generated metadata files that should be ignored when
# deciding whether a directory is "effectively empty".
_IGNORABLE_FILES = frozenset({
    "desktop.ini", "thumbs.db", ".ds_store", "picasa.ini",
    ".picasa.ini", "folder.jpg", ".bridgesort",
})


def _dir_real_entries(dir_path: str) -> list[str]:
    """Return entries in *dir_path* that are NOT ignorable system metadata files."""
    try:
        return [n for n in os.listdir(dir_path) if n.lower() not in _IGNORABLE_FILES]
    except Exception:
        return ["<error>"]  # treat as non-empty on error


def _remove_empty_dirs(start_dir: str, stop_at: Optional[str] = None):
    """Walk upward from *start_dir* removing each directory that is empty.

    Directories containing only ignorable system files (desktop.ini, Thumbs.db,
    .DS_Store, …) are treated as effectively empty: those files are removed first
    and then the directory itself is deleted.

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
            if not os.path.isdir(current):
                break
            real_entries = _dir_real_entries(current)
            if real_entries:
                break  # directory has real content, stop climbing
            # Only ignorable system files remain — delete them first, then rmdir
            for name in os.listdir(current):
                try:
                    os.remove(os.path.join(current, name))
                except Exception:
                    pass
            os.rmdir(current)
            logger.debug(f"Removed empty dir: {current}")
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
            _parse_source = normalize_parse_source(
                _json.loads(record.metadata_json).get("parse_source")
            )
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
