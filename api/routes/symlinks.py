"""Symlink record API — query / delete symlink_export records."""

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session
from typing import Optional, List

from db.database import get_db
from core.symlinks.action_service import (
    batch_delete_symlink_records,
    clear_all_symlinks,
    clear_failed_symlinks,
    delete_symlink_group,
    delete_symlink_record,
    retry_all_failed_symlinks,
    retry_symlink_record,
)
from core.symlinks.query_service import (
    get_symlink_stats,
    list_symlink_groups,
    list_symlink_records,
)

router = APIRouter(prefix="/api/symlinks", tags=["symlinks"])


class GroupDeleteBody(BaseModel):
    ids: List[int]
    group_dir: str


class SymlinkOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    folder_id: Optional[int] = None
    original_path: str
    link_path: str
    status: str
    error_msg: Optional[str] = None
    created_at: Optional[str] = None


@router.get("", response_model=dict)
def list_symlinks(
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    dir: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    result = list_symlink_records(db, folder_id, status, keyword, dir, page, page_size)
    return {
        "total": result["total"],
        "items": [SymlinkOut(**item) for item in result["items"]],
    }


@router.get("/stats")
def symlink_stats(db: Session = Depends(get_db)):
    return get_symlink_stats(db)


@router.get("/grouped")
def list_symlinks_grouped(
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    db: Session = Depends(get_db),
):
    return list_symlink_groups(db, folder_id, status, keyword)


@router.delete("/all")
def clear_all(db: Session = Depends(get_db)):
    return clear_all_symlinks(db)


@router.delete("/{record_id}")
def delete_symlink(
    record_id: int,
    delete_files: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    return delete_symlink_record(db, record_id, delete_files)


class BatchDeleteBody(BaseModel):
    ids: List[int]
    delete_files: bool = False


@router.post("/batch-delete")
def batch_delete(body: BatchDeleteBody, db: Session = Depends(get_db)):
    return batch_delete_symlink_records(db, body.ids, body.delete_files)


@router.post("/delete-group")
def delete_group(body: GroupDeleteBody, db: Session = Depends(get_db)):
    return delete_symlink_group(db, body.ids, body.group_dir)


@router.post("/clear-failed")
def clear_failed(db: Session = Depends(get_db)):
    return clear_failed_symlinks(db)


@router.post("/{record_id}/retry")
def retry_symlink(record_id: int, db: Session = Depends(get_db)):
    return retry_symlink_record(db, record_id)


@router.post("/retry-failed")
def retry_all_failed(db: Session = Depends(get_db)):
    return retry_all_failed_symlinks(db)
