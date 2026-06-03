import logging
import os
import shutil
import time
from typing import Optional

from db.scrape_models import SymlinkRecord
from monitor.delete_sync import remove_empty_dirs
from monitor.record_payloads import attach_record_metadata_json, scrape_record_to_dict, symlink_record_to_dict


logger = logging.getLogger(__name__)


def handle_symlink_export(watcher, folder, path: str, db) -> bool:
    target_root = (folder.target_root or "").strip()
    if not target_root or not os.path.isdir(target_root):
        symlink_record = SymlinkRecord(
            folder_id=folder.id,
            original_path=path,
            link_path="",
            status="failed",
            error_msg="导出软链接模式需要设置有效的归档目标目录",
        )
        db.add(symlink_record)
        db.commit()
        db.refresh(symlink_record)
        watcher._broadcast({"type": "symlink_update", "data": symlink_record_to_dict(symlink_record)})
        return True

    rel_path = os.path.relpath(path, os.path.normpath(folder.path))
    link_path = os.path.join(target_root, rel_path)
    if os.path.lexists(link_path):
        symlink_record = SymlinkRecord(
            folder_id=folder.id,
            original_path=path,
            link_path=link_path,
            status="success",
            error_msg="软链接已存在",
        )
        db.add(symlink_record)
        db.commit()
        db.refresh(symlink_record)
        watcher._broadcast({"type": "symlink_update", "data": symlink_record_to_dict(symlink_record)})
        return True

    try:
        os.makedirs(os.path.dirname(link_path), exist_ok=True)
        last_err: Optional[Exception] = None
        for _attempt in range(5):
            try:
                os.symlink(os.path.abspath(path), link_path)
                logger.info("Symlink export: %s -> %s", link_path, path)
                last_err = None
                break
            except OSError as sym_err:
                if getattr(sym_err, "winerror", None) == 32:
                    last_err = sym_err
                    time.sleep(2)
                    continue
                try:
                    shutil.copy2(path, link_path)
                    logger.warning("Symlink failed (%s), copied instead: %s", sym_err, link_path)
                except OSError as copy_err:
                    if getattr(copy_err, "winerror", None) == 32:
                        last_err = copy_err
                        time.sleep(2)
                        continue
                    raise
                last_err = None
                break
        else:
            raise last_err  # type: ignore[misc]
        if last_err is not None:
            raise last_err

        symlink_record = SymlinkRecord(
            folder_id=folder.id,
            original_path=path,
            link_path=link_path,
            status="success",
        )
        db.add(symlink_record)
        db.commit()
        db.refresh(symlink_record)
        watcher._broadcast({"type": "symlink_update", "data": symlink_record_to_dict(symlink_record)})
    except Exception as err:
        logger.error("Symlink export failed for %s: %s", path, err)
        symlink_record = SymlinkRecord(
            folder_id=folder.id,
            original_path=path,
            link_path=link_path,
            status="failed",
            error_msg=f"创建软链接失败: {err}",
        )
        db.add(symlink_record)
        db.commit()
        db.refresh(symlink_record)
        watcher._broadcast({"type": "symlink_update", "data": symlink_record_to_dict(symlink_record)})
    return True


def finalize_processed_item(watcher, folder, db, record, item, path: str):
    organize_mode = getattr(folder, "organize_mode", "move") or "move" if folder else "move"

    target = item.full_target or os.path.join(item.dir, item.new_name_only)
    target_dir = os.path.dirname(target)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    if os.path.normcase(item.path) != os.path.normcase(target):
        target_exists = os.path.exists(target)
        target_lexists = os.path.lexists(target)
        is_repair_target = (
            getattr(record, "_repairing", False)
            and os.path.normcase(getattr(record, "_previous_target", "")) == os.path.normcase(target)
        )
        is_same_file = False
        if target_exists and os.path.isfile(item.path):
            try:
                is_same_file = os.path.samefile(item.path, target)
            except (OSError, ValueError):
                pass
        if is_same_file:
            item.path = target
        elif target_lexists and is_repair_target and target_exists:
            item.path = target
        elif target_lexists and is_repair_target and not target_exists:
            try:
                os.remove(target)
            except Exception as err:
                record.status = "failed"
                record.target_path = target
                record.error_msg = f"清理失效目标失败: {err}"
                db.commit()
                watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
                return False
        if target_exists and not is_repair_target:
            record.status = "failed"
            record.target_path = target
            record.error_msg = f"目标文件已存在: {target}"
            db.commit()
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
            return False
        src_dir = os.path.dirname(item.path)

        if os.path.normcase(item.path) == os.path.normcase(target):
            pass
        elif organize_mode == "copy":
            shutil.copy2(item.path, target)
        elif organize_mode == "symlink":
            os.symlink(os.path.abspath(item.path), target)
        elif organize_mode == "hardlink":
            os.link(item.path, target)
        else:
            shutil.move(item.path, target)

        if (
            os.path.normcase(item.path) != os.path.normcase(target)
            and organize_mode not in ("copy", "symlink", "hardlink")
        ):
            item.path = target
            watch_root = os.path.normpath(folder.path) if folder else None
            remove_empty_dirs(src_dir, stop_at=watch_root)
        else:
            item.path = target

    watcher._worker_ctx._write_sidecar_files(item, target)

    record.status = "success"
    record.matched_title = (item.metadata or {}).get("title")
    record.matched_id = str((item.metadata or {}).get("id", "None"))
    record.matched_provider = (item.metadata or {}).get("provider")
    record.target_path = target
    attach_record_metadata_json(record, item.metadata or {})
    db.commit()
    watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
    logger.info("Archived: %s -> %s", os.path.basename(path), target)

    try:
        watcher._tg_batcher.add(
            folder.id if folder else 0,
            os.path.basename(folder.path) if folder else "",
            item,
        )
    except Exception as tg_err:
        logger.debug("TG 通知排队失败: %s", tg_err)

    try:
        watcher._emby_notifier.notify_success(target)
    except Exception as emby_err:
        logger.debug("Emby 通知失败: %s", emby_err)

    return True
