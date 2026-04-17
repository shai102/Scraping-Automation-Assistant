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
    error_msg: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ManualMatchBody(BaseModel):
    candidate_id: str
    candidate_title: str
    provider: str  # tmdb / bgm
    is_tv: bool = True


class SearchCandidatesBody(BaseModel):
    query: str
    year: Optional[int] = None
    is_tv: bool = True
    source: str = "siliconflow_tmdb"


def _row_to_out(r: ScrapeRecord) -> RecordOut:
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
        error_msg=r.error_msg,
        created_at=r.created_at.isoformat() if r.created_at else None,
        updated_at=r.updated_at.isoformat() if r.updated_at else None,
    )


@router.get("", response_model=dict)
def list_records(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
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
    total = q.count()
    rows = q.order_by(ScrapeRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_row_to_out(r).model_dump() for r in rows],
    }


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
    """Retry multiple records (sets status back to processing)."""
    rows = db.query(ScrapeRecord).filter(
        ScrapeRecord.id.in_(body.ids),
        ScrapeRecord.status.in_(["failed", "pending_manual"]),
    ).all()
    for row in rows:
        row.status = "processing"
        row.error_msg = None
    db.commit()
    # Trigger re-processing in background
    from server import get_watcher
    w = get_watcher()
    if w:
        for row in rows:
            if os.path.isfile(row.original_path):
                import threading
                # Remove from processed set so watcher accepts it
                with w._pending_lock:
                    w._processed.discard(os.path.normpath(row.original_path))
                # Delete the record so _process_file can recreate
                # Actually, let's just re-enqueue. We need to remove old record first
                pass
    return {"ok": True, "count": len(rows)}


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


@router.post("/{record_id}/manual-match")
def manual_match(record_id: int, body: ManualMatchBody, db: Session = Depends(get_db)):
    """Apply a manually chosen candidate to a pending record, then archive."""
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)

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

    # Build item and run through naming pipeline
    item = MediaItem(
        id=str(uuid.uuid4()),
        path=row.original_path,
        dir=os.path.dirname(row.original_path),
        old_name=row.original_name,
        ext=os.path.splitext(row.original_name)[1],
    )

    # Apply folder settings
    folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None
    if folder and folder.target_root:
        ctx.target_root.set(folder.target_root)
    if folder and folder.data_source:
        ctx.source_var.set(folder.data_source)

    # Inject the manual match into the context's manual_locks so process_task uses it
    ctx.manual_locks[item.path] = (body.candidate_title, str(body.candidate_id), f"手动/{body.provider}命中", meta or {})
    ctx.file_list = [item]

    try:
        _process_task(ctx, 0)
    except Exception as e:
        logger.error(f"Manual match process_task failed: {e}")
        row.status = "failed"
        row.error_msg = str(e)[:500]
        db.commit()
        raise HTTPException(500, detail=f"识别失败: {str(e)[:100]}")

    # Archive
    try:
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
            shutil.move(item.path, target)
            item.path = target
            from monitor.watcher import _remove_empty_dirs
            watch_root = os.path.normpath(folder.path) if folder else None
            _remove_empty_dirs(src_dir, stop_at=watch_root)

        ctx._write_sidecar_files(item, target)

        row.status = "success"
        row.matched_title = (item.metadata or {}).get("title")
        row.matched_id = str(tid)
        row.matched_provider = body.provider
        row.target_path = target
        row.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
        row.error_msg = None
        db.commit()

        # Broadcast
        from server import get_watcher
        w = get_watcher()
        if w and w._broadcast:
            from monitor.watcher import _record_to_dict
            w._broadcast({"type": "record_update", "data": _record_to_dict(row)})

        return _row_to_out(row).model_dump()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Manual match archive failed: {e}")
        row.status = "failed"
        row.error_msg = str(e)[:500]
        db.commit()
        raise HTTPException(500, detail=f"归档失败: {str(e)[:100]}")


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
