import os
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from db.scrape_models import SymlinkRecord


def list_symlink_records(
    db: Session,
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    dir_path: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
):
    q = db.query(SymlinkRecord)
    if folder_id:
        q = q.filter(SymlinkRecord.folder_id == folder_id)
    if status:
        q = q.filter(SymlinkRecord.status == status)
    if keyword:
        q = q.filter(SymlinkRecord.original_path.contains(keyword))
    if dir_path:
        norm_dir = os.path.normpath(dir_path)
        q = q.filter(
            or_(
                SymlinkRecord.link_path.like(norm_dir.replace("\\", "/") + "/%"),
                SymlinkRecord.link_path.like(norm_dir + os.sep + "%"),
                SymlinkRecord.original_path.like(norm_dir.replace("\\", "/") + "/%"),
                SymlinkRecord.original_path.like(norm_dir + os.sep + "%"),
            )
        )
    total = q.count()
    rows = (
        q.order_by(SymlinkRecord.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    items = []
    for row in rows:
        items.append(
            {
                "id": row.id,
                "folder_id": row.folder_id,
                "original_path": row.original_path,
                "link_path": row.link_path or "",
                "status": row.status,
                "error_msg": row.error_msg,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
        )
    return {"total": total, "items": items}


def get_symlink_stats(db: Session):
    total = db.query(func.count(SymlinkRecord.id)).scalar() or 0
    success = (
        db.query(func.count(SymlinkRecord.id))
        .filter(SymlinkRecord.status == "success")
        .scalar()
        or 0
    )
    failed = (
        db.query(func.count(SymlinkRecord.id))
        .filter(SymlinkRecord.status == "failed")
        .scalar()
        or 0
    )
    return {"total": total, "success": success, "failed": failed}


def _symlink_group_dir(row: SymlinkRecord) -> str:
    link_path = str(row.link_path or "").strip()
    if link_path:
        return os.path.normpath(os.path.dirname(link_path))
    return os.path.normpath(os.path.dirname(row.original_path))


def list_symlink_groups(
    db: Session,
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
):
    q = db.query(SymlinkRecord)
    if folder_id:
        q = q.filter(SymlinkRecord.folder_id == folder_id)
    if status:
        q = q.filter(SymlinkRecord.status == status)
    if keyword:
        q = q.filter(SymlinkRecord.original_path.contains(keyword))
    rows = q.order_by(SymlinkRecord.id.desc()).all()

    groups: dict = {}
    for row in rows:
        dir_path = _symlink_group_dir(row)
        if dir_path not in groups:
            groups[dir_path] = {
                "dir_path": dir_path,
                "dir_name": os.path.basename(dir_path),
                "folder_id": row.folder_id,
                "total": 0,
                "success": 0,
                "failed": 0,
                "ids": [],
            }
        group = groups[dir_path]
        group["total"] += 1
        group["ids"].append(row.id)
        if row.status == "success":
            group["success"] += 1
        elif row.status == "failed":
            group["failed"] += 1

    return {"groups": list(groups.values())}
