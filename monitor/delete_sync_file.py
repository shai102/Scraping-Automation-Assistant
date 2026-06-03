import logging
import os

from db.database import SessionLocal
from db.scrape_models import ScrapeRecord, SymlinkRecord
from monitor.delete_sync_common import delete_per_file_sidecars, remove_empty_dirs


logger = logging.getLogger(__name__)


def handle_file_deleted(service, path: str):
    db = SessionLocal()
    try:
        folder = service.watcher._find_folder(path, db)
        if not folder:
            return

        organize_mode = getattr(folder, "organize_mode", "move") or "move"
        stop_root = (folder.target_root or "").strip() or None

        if organize_mode == "symlink_export":
            symlink_row = db.query(SymlinkRecord).filter(SymlinkRecord.original_path == path).first()
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
                    .filter(ScrapeRecord.original_path == link_path, ScrapeRecord.status == "success")
                    .first()
                )
                if scraped and scraped.target_path:
                    target_path = scraped.target_path
                    target_folder = db.query(service.watcher._folder_model).get(scraped.folder_id) if scraped.folder_id else None
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
            service.watcher._broadcast(
                {"type": "symlink_deleted", "data": {"original_path": path, "link_path": link_path}}
            )
            if not symlink_row and not link_path:
                return
            return

        if organize_mode in ("copy", "symlink", "hardlink"):
            record = (
                db.query(ScrapeRecord)
                .filter(ScrapeRecord.original_path == path, ScrapeRecord.status == "success")
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
            service.watcher._broadcast(
                {"type": "record_deleted", "data": {"original_path": path, "target_path": target_path}}
            )
    except Exception as err:
        logger.error(f"处理文件删除事件失败 {path}: {err}")
    finally:
        with service.watcher._pending_lock:
            service.watcher._processed.discard(path)
        db.close()
