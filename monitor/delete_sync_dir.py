import logging
import os
import shutil

from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.delete_sync_common import delete_per_file_sidecars, remove_empty_dirs


logger = logging.getLogger(__name__)


def handle_dir_deleted(service, dir_path: str):
    db = SessionLocal()
    try:
        folder = service.watcher._find_folder(dir_path, db)
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
                service.watcher._broadcast(
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
                with service.watcher._pending_lock:
                    service.watcher._processed.discard(os.path.normpath(row.original_path))
                db.delete(row)
            db.commit()

            for link in link_paths:
                scraped = (
                    db.query(ScrapeRecord)
                    .filter(ScrapeRecord.original_path == link, ScrapeRecord.status == "success")
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
            service.watcher._broadcast(
                {"type": "dir_deleted", "data": {"original_path": dir_path, "mode": organize_mode}}
            )
            return

        if organize_mode in ("copy", "symlink", "hardlink"):
            records = (
                db.query(ScrapeRecord)
                .filter(ScrapeRecord.folder_id == folder.id, ScrapeRecord.status == "success")
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
                with service.watcher._pending_lock:
                    service.watcher._processed.discard(os.path.normpath(record.original_path))
                db.delete(record)
            db.commit()
            for dir_name in sorted(deleted_dirs, key=len, reverse=True):
                remove_empty_dirs(dir_name, stop_at=stop_root)
            service.watcher._broadcast(
                {"type": "dir_deleted", "data": {"original_path": dir_path, "mode": organize_mode}}
            )
    except Exception as err:
        logger.error(f"处理目录删除事件失败 {dir_path}: {err}")
    finally:
        db.close()
