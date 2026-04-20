"""Scrape record queries, manual match, retry, delete."""

import json
import logging
import os
import shutil
import threading
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from core.models.media_item import MediaItem
from core.services.worker_context import WorkerContext
from core.workers.task_runner import process_task as _process_task
from db.database import get_db
from db.scrape_models import ScrapeRecord, MonitorFolder
from db.tmdb_api import (
    fetch_bgm_candidates,
    fetch_tmdb_candidates,
    fetch_tmdb_by_id,
    fetch_bgm_by_id,
)
from utils.helpers import candidate_to_result

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/records", tags=["records"])


class RecordOut(BaseModel):
    id: int
    folder_id: Optional[int] = None
    original_path: str
    original_name: str
    status: str
    matched_title: Optional[str] = None
    matched_id: Optional[str] = None
    matched_provider: Optional[str] = None
    target_path: Optional[str] = None
    media_type: Optional[str] = None
    parse_source: Optional[str] = None
    error_msg: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ManualMatchBody(BaseModel):
    candidate_id: str
    candidate_title: str
    provider: str  # tmdb / bgm
    is_tv: bool = True
    season_override: Optional[int] = None
    episode_offset: int = 0
    scope: str = "single"  # "single" | "folder"


class SearchCandidatesBody(BaseModel):
    query: str
    year: Optional[int] = None
    is_tv: bool = True
    source: str = "siliconflow_tmdb"


def _row_to_out(r: ScrapeRecord) -> RecordOut:
    # Extract media_type and parse_source from stored metadata_json
    _media_type = None
    _parse_source = None
    if r.metadata_json:
        try:
            _meta = json.loads(r.metadata_json)
            _media_type = _meta.get("type")  # "episode" or "movie"
            _parse_source = _meta.get("parse_source")  # "guessit" or "ai"
        except Exception:
            pass
    return RecordOut(
        id=r.id,
        folder_id=r.folder_id,
        original_path=r.original_path,
        original_name=r.original_name,
        status=r.status,
        matched_title=r.matched_title,
        matched_id=r.matched_id,
        matched_provider=r.matched_provider,
        target_path=r.target_path,
        media_type=_media_type,
        parse_source=_parse_source,
        error_msg=r.error_msg,
        created_at=r.created_at.isoformat() if r.created_at else None,
        updated_at=r.updated_at.isoformat() if r.updated_at else None,
    )


@router.get("", response_model=dict)
def list_records(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    dir: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(ScrapeRecord)
    if status:
        q = q.filter(ScrapeRecord.status == status)
    if keyword:
        q = q.filter(ScrapeRecord.original_name.ilike(f"%{keyword}%"))
    if media_type:
        q = q.join(MonitorFolder, ScrapeRecord.folder_id == MonitorFolder.id, isouter=True)
        q = q.filter(MonitorFolder.media_type == media_type)
    if dir:
        # Filter records whose original_path is directly inside the given directory
        norm_dir = os.path.normpath(dir)
        # 用 SQL LIKE 过滤，避免全表加载到内存
        q = q.filter(
            ScrapeRecord.original_path.like(norm_dir.replace('\\', '/') + '/%') |
            ScrapeRecord.original_path.like(norm_dir + os.sep + '%')
        )
    total = q.count()
    rows = q.order_by(ScrapeRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_row_to_out(r).model_dump() for r in rows],
    }


@router.get("/grouped")
def list_records_grouped(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return records grouped by source directory (summary only, no full records)."""
    q = db.query(ScrapeRecord)
    if status:
        q = q.filter(ScrapeRecord.status == status)
    if keyword:
        q = q.filter(ScrapeRecord.original_name.ilike(f"%{keyword}%"))
    if media_type:
        q = q.join(MonitorFolder, ScrapeRecord.folder_id == MonitorFolder.id, isouter=True)
        q = q.filter(MonitorFolder.media_type == media_type)
    rows = q.order_by(ScrapeRecord.id.desc()).all()

    groups: dict = {}
    for r in rows:
        dir_path = os.path.normpath(os.path.dirname(r.original_path))
        if dir_path not in groups:
            groups[dir_path] = {
                "dir_path": dir_path,
                "dir_name": os.path.basename(dir_path),
                "folder_id": r.folder_id,
                "total": 0,
                "success": 0,
                "failed": 0,
                "pending": 0,
                "ids": [],
            }
        g = groups[dir_path]
        g["total"] += 1
        g["ids"].append(r.id)
        if r.status == "success":
            g["success"] += 1
        elif r.status == "failed":
            g["failed"] += 1
        elif r.status == "pending_manual":
            g["pending"] += 1

    return {"groups": list(groups.values())}


@router.delete("/{record_id}")
def delete_record(record_id: int, db: Session = Depends(get_db)):
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    db.delete(row)
    db.commit()
    return {"ok": True}


class BatchDeleteBody(BaseModel):
    ids: list[int]


@router.post("/batch-delete")
def batch_delete(body: BatchDeleteBody, db: Session = Depends(get_db)):
    """Delete multiple records by IDs."""
    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(body.ids)).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.post("/clear-failed")
def clear_failed(db: Session = Depends(get_db)):
    """Delete all failed records."""
    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.status == "failed").delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.post("/clear-all")
def clear_all(db: Session = Depends(get_db)):
    """Delete all records."""
    deleted = db.query(ScrapeRecord).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.post("/batch-retry")
def batch_retry(body: BatchDeleteBody, db: Session = Depends(get_db)):
    """Retry multiple records by deleting them and re-enqueuing the original files."""
    rows = db.query(ScrapeRecord).filter(
        ScrapeRecord.id.in_(body.ids),
    ).all()

    # Collect paths of files that still exist on disk before deleting records
    paths_to_retry = [
        row.original_path for row in rows
        if os.path.isfile(row.original_path)
    ]

    # Delete the old records so _process_file can re-create them fresh
    for row in rows:
        db.delete(row)
    db.commit()

    # Re-enqueue via watcher
    from server import get_watcher
    w = get_watcher()
    count = 0
    if w:
        for path in paths_to_retry:
            norm = os.path.normpath(path)
            with w._pending_lock:
                # Temporarily remove so _process_file can run, then re-add to block
                # _poll_loop from also picking up the file while processing is in flight.
                w._processed.discard(norm)
            w._pool.submit(w._process_file, path)
            # Re-add to _processed so _poll_loop won't double-enqueue the same file
            with w._pending_lock:
                w._processed.add(norm)
            count += 1

    return {"ok": True, "count": count}


@router.post("/search-candidates")
def search_candidates(body: SearchCandidatesBody, db: Session = Depends(get_db)):
    """Search TMDB/BGM candidates for manual matching."""
    from utils.helpers import CONFIG_FILE
    ctx = WorkerContext()
    api_key = ctx.tmdb_api_key.get() if body.source == "siliconflow_tmdb" else ctx.bgm_api_key.get()

    if body.source == "siliconflow_tmdb":
        results = fetch_tmdb_candidates(body.query, body.year, body.is_tv, api_key)
    else:
        results = fetch_bgm_candidates(body.query, body.year, api_key)

    return {"candidates": results or []}


# ------------------------------------------------------------------
# Helpers: restore a record to pre-archive state
# ------------------------------------------------------------------

def _delete_file_sidecars(file_path: str):
    """Delete per-file sidecar files (NFO + thumbnail) for a given media file."""
    stem = os.path.splitext(file_path)[0]
    for suffix in (".nfo", "-thumb.jpg"):
        p = stem + suffix
        if os.path.isfile(p):
            try:
                os.remove(p)
                logger.debug(f"Deleted sidecar: {p}")
            except Exception as e:
                logger.warning(f"Failed to delete sidecar {p}: {e}")


def _restore_record_file(row: ScrapeRecord, folder, db: Session):
    """Restore an already-archived record back to its original state.

    - Deletes per-file sidecar files at target_path
    - Moves / removes the target file depending on organize_mode
    - Validates that original_path exists afterwards
    - Resets row fields (does NOT commit — caller must commit)
    """
    tgt = row.target_path
    if not tgt or not os.path.exists(tgt):
        # Nothing archived yet, or file gone — just validate original
        if not os.path.isfile(row.original_path):
            raise HTTPException(400, detail="源文件不存在，无法恢复")
        return

    organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

    # 1. Delete per-file sidecar files at target location
    _delete_file_sidecars(tgt)

    # 2. Restore / remove the target file
    if os.path.normcase(os.path.normpath(tgt)) != os.path.normcase(os.path.normpath(row.original_path)):
        if organize_mode in ('move', 'rename'):
            # File was moved — move it back
            orig_dir = os.path.dirname(row.original_path)
            os.makedirs(orig_dir, exist_ok=True)
            shutil.move(tgt, row.original_path)
            logger.info(f"Restored: {tgt} -> {row.original_path}")
            # Clean up empty directories left behind
            from monitor.watcher import _remove_empty_dirs
            watch_root = os.path.normpath(folder.path) if folder else None
            _remove_empty_dirs(os.path.dirname(tgt), stop_at=watch_root)
        else:
            # copy / symlink / hardlink — original is still in place, just remove target
            try:
                os.remove(tgt)
                logger.info(f"Removed target copy/link: {tgt}")
                from monitor.watcher import _remove_empty_dirs
                watch_root = os.path.normpath(folder.path) if folder else None
                _remove_empty_dirs(os.path.dirname(tgt), stop_at=watch_root)
            except Exception as e:
                logger.warning(f"Failed to remove target {tgt}: {e}")

    if not os.path.isfile(row.original_path):
        raise HTTPException(400, detail="源文件恢复失败，文件不存在")

    # 3. Reset record fields
    row.target_path = None
    row.status = "processing"
    row.error_msg = None
    row.matched_title = None
    row.matched_id = None
    row.matched_provider = None
    row.metadata_json = None
    db.flush()


def _archive_file(item, row, folder, ctx, tid, provider, db):
    """Archive a single file after successful process_task, respecting organize_mode."""
    organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

    # For 'rename' mode, use the monitored folder path as target_root
    if organize_mode == 'rename' and folder:
        ctx.target_root.set(folder.path)

    target = item.full_target or os.path.join(item.dir, item.new_name_only or item.old_name)
    target_dir = os.path.dirname(target)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    if os.path.normcase(item.path) != os.path.normcase(target):
        if os.path.exists(target):
            row.status = "failed"
            row.error_msg = "目标文件已存在"
            db.commit()
            raise HTTPException(400, detail="目标文件已存在")
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
            from monitor.watcher import _remove_empty_dirs
            _remove_empty_dirs(src_dir, stop_at=watch_root)
        else:
            item.path = target

    ctx._write_sidecar_files(item, target)

    row.status = "success"
    row.matched_title = (item.metadata or {}).get("title")
    row.matched_id = str(tid)
    row.matched_provider = provider
    row.target_path = target
    row.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
    row.error_msg = None
    db.flush()
    return target


def _process_single_manual(row, body, folder, db):
    """Process a single record through manual match: restore → recognize → archive.
    Returns the updated row on success, raises HTTPException on failure.
    """
    # Restore if already archived
    if row.target_path and os.path.exists(row.target_path):
        _restore_record_file(row, folder, db)
        db.commit()

    if not os.path.isfile(row.original_path):
        row.status = "failed"
        row.error_msg = "源文件不存在"
        db.commit()
        raise HTTPException(400, detail="源文件不存在")

    # Fetch full metadata by ID
    ctx = WorkerContext()
    if body.provider == "tmdb":
        t, tid, msg, meta = fetch_tmdb_by_id(body.candidate_id, body.is_tv, ctx.tmdb_api_key.get())
    else:
        t, tid, msg, meta = fetch_bgm_by_id(body.candidate_id, ctx.bgm_api_key.get())

    if tid == "None":
        raise HTTPException(400, detail="候选 ID 无效")

    item = MediaItem(
        id=str(uuid.uuid4()),
        path=row.original_path,
        dir=os.path.dirname(row.original_path),
        old_name=row.original_name,
        ext=os.path.splitext(row.original_name)[1],
    )

    # Apply folder settings
    if folder and folder.target_root:
        ctx.target_root.set(folder.target_root)
    if folder and folder.data_source:
        ctx.source_var.set(folder.data_source)

    ctx.media_type_override.set("电视剧" if body.is_tv else "电影")

    ctx.manual_locks[item.path] = (body.candidate_title, str(body.candidate_id), f"手动/{body.provider}命中", meta or {})
    if body.season_override is not None:
        ctx.forced_seasons[item.path] = body.season_override
    if body.episode_offset != 0:
        ctx.forced_offsets[item.path] = body.episode_offset
    ctx.file_list = [item]

    try:
        _process_task(ctx, 0)
    except Exception as e:
        logger.error(f"Manual match process_task failed: {e}")
        row.status = "failed"
        row.error_msg = str(e)[:500]
        db.commit()
        raise HTTPException(500, detail=f"识别失败: {str(e)[:100]}")

    try:
        _archive_file(item, row, folder, ctx, tid, body.provider, db)
        db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Manual match archive failed: {e}")
        row.status = "failed"
        row.error_msg = str(e)[:500]
        db.commit()
        raise HTTPException(500, detail=f"归档失败: {str(e)[:100]}")

    # Broadcast
    from server import get_watcher
    w = get_watcher()
    if w and w._broadcast:
        from monitor.watcher import _record_to_dict
        w._broadcast({"type": "record_update", "data": _record_to_dict(row)})

    # TG notification
    if w and hasattr(w, '_tg_batcher') and folder:
        w._tg_batcher.add(folder.id, os.path.basename(folder.path), item)

    return row


@router.post("/{record_id}/manual-match")
def manual_match(record_id: int, body: ManualMatchBody, db: Session = Depends(get_db)):
    """Apply a manually chosen candidate to a pending record, then archive."""
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)

    folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None

    # Process the primary record
    _process_single_manual(row, body, folder, db)

    processed_count = 1

    # Folder scope: also process sibling files in the same directory
    if body.scope == "folder":
        org_dir = os.path.normpath(os.path.dirname(row.original_path))
        siblings = db.query(ScrapeRecord).filter(
            ScrapeRecord.folder_id == row.folder_id,
            ScrapeRecord.id != row.id,
        ).all()
        siblings = [s for s in siblings
                    if os.path.normpath(os.path.dirname(s.original_path)) == org_dir]

        for sib in siblings:
            try:
                _process_single_manual(sib, body, folder, db)
                processed_count += 1
            except Exception as e:
                logger.error(f"Folder-scope manual match failed for {sib.original_path}: {e}")

    return {"ok": True, "processed": processed_count}


@router.post("/{record_id}/retry")
def retry_record(record_id: int, db: Session = Depends(get_db)):
    """Re-run automatic recognition on a failed/pending record."""
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    if not os.path.isfile(row.original_path):
        row.status = "failed"
        row.error_msg = "源文件不存在"
        db.commit()
        raise HTTPException(400, detail="源文件不存在")

    row.status = "processing"
    row.error_msg = None
    db.commit()

    from server import get_watcher
    w = get_watcher()

    def _run():
        if w:
            w._process_file(row.original_path)

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": "重试已启动"}
