import logging
import os
import shutil
from typing import Optional, Set

from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.record_state import (
    scrape_record_needs_repair,
    symlink_record_consumed_downstream,
    symlink_record_needs_repair,
)


logger = logging.getLogger(__name__)

_IGNORABLE_FILES = frozenset(
    {
        "desktop.ini",
        "thumbs.db",
        ".ds_store",
        "picasa.ini",
        ".picasa.ini",
        "folder.jpg",
        ".bridgesort",
    }
)


def delete_per_file_sidecars(file_path: str):
    if not file_path:
        return
    base = os.path.splitext(file_path)[0]
    for suffix in (".nfo", "-thumb.jpg", "-poster.jpg", "-fanart.jpg"):
        sidecar = base + suffix
        if os.path.isfile(sidecar):
            try:
                os.remove(sidecar)
                logger.debug(f"删除伴随文件: {sidecar}")
            except Exception as err:
                logger.warning(f"删除伴随文件失败 {sidecar}: {err}")


def dir_real_entries(dir_path: str) -> list[str]:
    try:
        return [name for name in os.listdir(dir_path) if name.lower() not in _IGNORABLE_FILES]
    except Exception:
        return ["<error>"]


def remove_empty_dirs(start_dir: str, stop_at: Optional[str] = None):
    current = os.path.normpath(start_dir)
    while True:
        if stop_at and os.path.normcase(current) == os.path.normcase(stop_at):
            break
        parent = os.path.dirname(current)
        if parent == current:
            break
        try:
            if not os.path.isdir(current):
                break
            real_entries = dir_real_entries(current)
            if real_entries:
                break
            for name in os.listdir(current):
                try:
                    os.remove(os.path.join(current, name))
                except Exception:
                    pass
            os.rmdir(current)
            logger.debug(f"Removed empty dir: {current}")
        except Exception as err:
            logger.warning(f"Could not remove dir {current}: {err}")
            break
        current = parent


class DeleteSyncService:
    def __init__(
        self,
        watcher,
        *,
        record_to_dict,
        symlink_record_to_dict,
    ):
        self.watcher = watcher
        self._record_to_dict = record_to_dict
        self._symlink_record_to_dict = symlink_record_to_dict

    def handle_dir_deleted(self, dir_path: str):
        db = SessionLocal()
        try:
            folder = self.watcher._find_folder(dir_path, db)
            if not folder:
                return

            organize_mode = getattr(folder, "organize_mode", "move") or "move"
            stop_root = (folder.target_root or "").strip() or None
            prefix = dir_path + os.sep

            if organize_mode == "symlink_export":
                symlink_rows = db.query(SymlinkRecord).filter(SymlinkRecord.folder_id == folder.id).all()
                matched_rows = [
                    row
                    for row in symlink_rows
                    if row.original_path and os.path.normpath(row.original_path).startswith(prefix)
                ]
                target_root_fs = (folder.target_root or "").strip()
                folder_norm = os.path.normpath(folder.path)
                if not matched_rows and target_root_fs and os.path.isdir(target_root_fs):
                    rel = os.path.relpath(dir_path, folder_norm)
                    link_dir_fs = os.path.normpath(os.path.join(target_root_fs, rel))
                    if os.path.isdir(link_dir_fs):
                        try:
                            shutil.rmtree(link_dir_fs)
                            logger.info(f"文件系统兑备删除软链接目录: {link_dir_fs} (DB 无记录)")
                        except Exception as err:
                            logger.warning(f"兑备删除软链接目录失败 {link_dir_fs}: {err}")
                    self.watcher._broadcast(
                        {"type": "dir_deleted", "data": {"original_path": dir_path, "mode": organize_mode}}
                    )
                    return

                deleted_dirs = set()
                link_paths = []
                for row in matched_rows:
                    link = row.link_path
                    if link and os.path.lexists(link):
                        try:
                            os.remove(link)
                            logger.info(f"同步删除软链接: {link} (源目录已删除: {dir_path})")
                        except Exception as err:
                            logger.warning(f"删除软链接失败 {link}: {err}")
                    if link:
                        deleted_dirs.add(os.path.dirname(link))
                        link_paths.append(link)
                    with self.watcher._pending_lock:
                        self.watcher._processed.discard(os.path.normpath(row.original_path))
                    db.delete(row)
                db.commit()

                for link in link_paths:
                    scraped = (
                        db.query(ScrapeRecord)
                        .filter(
                            ScrapeRecord.original_path == link,
                            ScrapeRecord.status == "success",
                        )
                        .first()
                    )
                    if scraped and scraped.target_path:
                        target_path = scraped.target_path
                        target_folder = db.query(MonitorFolder).get(scraped.folder_id) if scraped.folder_id else None
                        target_stop = (target_folder.target_root or "").strip() or None if target_folder else None
                        if os.path.exists(target_path) or os.path.lexists(target_path):
                            try:
                                os.remove(target_path)
                                logger.info(f"链式删除刮削目标: {target_path} (源目录软链接: {link})")
                            except Exception as err:
                                logger.warning(f"链式删除刮削目标失败 {target_path}: {err}")
                        delete_per_file_sidecars(target_path)
                        deleted_dirs.add((os.path.dirname(target_path), target_stop))
                        db.delete(scraped)
                db.commit()

                for entry in sorted(
                    deleted_dirs,
                    key=lambda value: len(value[0]) if isinstance(value, tuple) else len(value),
                    reverse=True,
                ):
                    if isinstance(entry, tuple):
                        remove_empty_dirs(entry[0], stop_at=entry[1])
                    else:
                        remove_empty_dirs(entry, stop_at=stop_root)
                self.watcher._broadcast(
                    {"type": "dir_deleted", "data": {"original_path": dir_path, "mode": organize_mode}}
                )
                return

            if organize_mode in ("copy", "symlink", "hardlink"):
                records = (
                    db.query(ScrapeRecord)
                    .filter(
                        ScrapeRecord.folder_id == folder.id,
                        ScrapeRecord.status == "success",
                    )
                    .all()
                )
                records = [
                    row
                    for row in records
                    if row.original_path and os.path.normpath(row.original_path).startswith(prefix)
                ]
                deleted_dirs = set()
                for record in records:
                    target_path = record.target_path
                    if target_path and (os.path.exists(target_path) or os.path.lexists(target_path)):
                        try:
                            os.remove(target_path)
                            logger.info(f"同步删除目标文件: {target_path} (源目录已删除: {dir_path})")
                        except Exception as err:
                            logger.warning(f"删除目标文件失败 {target_path}: {err}")
                        delete_per_file_sidecars(target_path)
                        deleted_dirs.add(os.path.dirname(target_path))
                    with self.watcher._pending_lock:
                        self.watcher._processed.discard(os.path.normpath(record.original_path))
                    db.delete(record)
                db.commit()
                for dir_name in sorted(deleted_dirs, key=len, reverse=True):
                    remove_empty_dirs(dir_name, stop_at=stop_root)
                self.watcher._broadcast(
                    {"type": "dir_deleted", "data": {"original_path": dir_path, "mode": organize_mode}}
                )
        except Exception as err:
            logger.error(f"处理目录删除事件失败 {dir_path}: {err}")
        finally:
            db.close()

    def handle_file_deleted(self, path: str):
        db = SessionLocal()
        try:
            folder = self.watcher._find_folder(path, db)
            if not folder:
                return

            organize_mode = getattr(folder, "organize_mode", "move") or "move"
            stop_root = (folder.target_root or "").strip() or None

            if organize_mode == "symlink_export":
                symlink_row = (
                    db.query(SymlinkRecord)
                    .filter(SymlinkRecord.original_path == path)
                    .first()
                )
                if symlink_row:
                    link_path = symlink_row.link_path
                else:
                    target_root = (folder.target_root or "").strip()
                    folder_path = os.path.normpath(folder.path)
                    if target_root and path.startswith(folder_path):
                        rel = os.path.relpath(path, folder_path)
                        link_path = os.path.join(target_root, rel)
                    else:
                        link_path = None

                if link_path and os.path.lexists(link_path):
                    try:
                        os.remove(link_path)
                        logger.info(f"同步删除软链接: {link_path} (源文件已删除: {path})")
                    except Exception as err:
                        logger.warning(f"删除软链接失败 {link_path}: {err}")
                link_dir = os.path.dirname(link_path) if link_path else None
                if symlink_row:
                    db.delete(symlink_row)
                    db.commit()

                if link_path:
                    scraped = (
                        db.query(ScrapeRecord)
                        .filter(
                            ScrapeRecord.original_path == link_path,
                            ScrapeRecord.status == "success",
                        )
                        .first()
                    )
                    if scraped and scraped.target_path:
                        target_path = scraped.target_path
                        target_folder = db.query(MonitorFolder).get(scraped.folder_id) if scraped.folder_id else None
                        target_stop = (target_folder.target_root or "").strip() or None if target_folder else None
                        if os.path.exists(target_path) or os.path.lexists(target_path):
                            try:
                                os.remove(target_path)
                                logger.info(f"链式删除刮削目标: {target_path} (源软链接: {link_path})")
                            except Exception as err:
                                logger.warning(f"链式删除刮削目标失败 {target_path}: {err}")
                        delete_per_file_sidecars(target_path)
                        db.delete(scraped)
                        db.commit()
                        remove_empty_dirs(os.path.dirname(target_path), stop_at=target_stop)
                    elif not scraped:
                        logger.debug(f"链式追查：未找到刮削记录 (软链接已删除或记录已清空): {link_path}")

                if link_dir:
                    remove_empty_dirs(link_dir, stop_at=stop_root)
                self.watcher._broadcast(
                    {"type": "symlink_deleted", "data": {"original_path": path, "link_path": link_path}}
                )
                if not symlink_row and not link_path:
                    return
                return

            if organize_mode in ("copy", "symlink", "hardlink"):
                record = (
                    db.query(ScrapeRecord)
                    .filter(
                        ScrapeRecord.original_path == path,
                        ScrapeRecord.status == "success",
                    )
                    .first()
                )
                if not record or not record.target_path:
                    return
                target_path = record.target_path
                if os.path.exists(target_path) or os.path.lexists(target_path):
                    try:
                        os.remove(target_path)
                        logger.info(f"同步删除目标文件: {target_path} (源文件已删除: {path})")
                    except Exception as err:
                        logger.warning(f"删除目标文件失败 {target_path}: {err}")
                delete_per_file_sidecars(target_path)
                db.delete(record)
                db.commit()
                remove_empty_dirs(os.path.dirname(target_path), stop_at=stop_root)
                self.watcher._broadcast(
                    {"type": "record_deleted", "data": {"original_path": path, "target_path": target_path}}
                )
        except Exception as err:
            logger.error(f"处理文件删除事件失败 {path}: {err}")
        finally:
            with self.watcher._pending_lock:
                self.watcher._processed.discard(path)
            db.close()
