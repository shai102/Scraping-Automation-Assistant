"""Symlink record API — query / delete symlink_export records."""

import os
import threading
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List

from db.database import get_db
from db.scrape_models import SymlinkRecord, MonitorFolder

router = APIRouter(prefix="/api/symlinks", tags=["symlinks"])


class SymlinkOut(BaseModel):
    id: int
    folder_id: Optional[int] = None
    original_path: str
    link_path: str
    status: str
    error_msg: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


@router.get("", response_model=dict)
def list_symlinks(
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(SymlinkRecord)
    if folder_id:
        q = q.filter(SymlinkRecord.folder_id == folder_id)
    if status:
        q = q.filter(SymlinkRecord.status == status)
    if keyword:
        q = q.filter(SymlinkRecord.original_path.contains(keyword))
    total = q.count()
    rows = q.order_by(SymlinkRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
    items = []
    for r in rows:
        items.append(SymlinkOut(
            id=r.id,
            folder_id=r.folder_id,
            original_path=r.original_path,
            link_path=r.link_path or "",
            status=r.status,
            error_msg=r.error_msg,
            created_at=r.created_at.isoformat() if r.created_at else None,
        ))
    return {"total": total, "items": items}


@router.get("/stats")
def symlink_stats(db: Session = Depends(get_db)):
    """Return count of success / failed symlink records."""
    total = db.query(func.count(SymlinkRecord.id)).scalar() or 0
    success = db.query(func.count(SymlinkRecord.id)).filter(SymlinkRecord.status == "success").scalar() or 0
    failed = db.query(func.count(SymlinkRecord.id)).filter(SymlinkRecord.status == "failed").scalar() or 0
    return {"total": total, "success": success, "failed": failed}


@router.delete("/all")
def clear_all(db: Session = Depends(get_db)):
    """Delete all symlink records."""
    deleted = db.query(SymlinkRecord).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.delete("/{record_id}")
def delete_symlink(record_id: int, db: Session = Depends(get_db)):
    row = db.query(SymlinkRecord).get(record_id)
    if not row:
        raise HTTPException(404, detail="记录不存在")
    db.delete(row)
    db.commit()
    return {"ok": True}


@router.post("/batch-delete")
def batch_delete(body: dict, db: Session = Depends(get_db)):
    ids = body.get("ids", [])
    if not ids:
        return {"ok": True, "deleted": 0}
    deleted = db.query(SymlinkRecord).filter(SymlinkRecord.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.post("/clear-failed")
def clear_failed(db: Session = Depends(get_db)):
    """Delete all failed symlink records."""
    deleted = db.query(SymlinkRecord).filter(SymlinkRecord.status == "failed").delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.post("/{record_id}/retry")
def retry_symlink(record_id: int, db: Session = Depends(get_db)):
    """Retry a single failed symlink record."""
    row = db.query(SymlinkRecord).get(record_id)
    if not row:
        raise HTTPException(404, detail="记录不存在")
    if not os.path.isfile(row.original_path):
        raise HTTPException(400, detail="源文件不存在")
    path = row.original_path
    db.delete(row)
    db.commit()
    from server import get_watcher
    w = get_watcher()
    def _run():
        if w:
            w._process_file(path)
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True}


@router.post("/retry-failed")
def retry_all_failed(db: Session = Depends(get_db)):
    """Retry all failed symlink records."""
    rows = db.query(SymlinkRecord).filter(SymlinkRecord.status == "failed").all()
    paths = [r.original_path for r in rows if os.path.isfile(r.original_path)]
    for r in rows:
        db.delete(r)
    db.commit()
    from server import get_watcher
    w = get_watcher()
    def _run():
        if w:
            for p in paths:
                w._process_file(p)
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "queued": len(paths)}
