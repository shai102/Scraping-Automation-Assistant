"""Monitor folder CRUD + scan trigger."""

import os

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from db.database import get_db
from db.scrape_models import MonitorFolder

router = APIRouter(prefix="/api/monitor", tags=["monitor"])


class FolderCreate(BaseModel):
    path: str
    target_root: str = ""
    media_type: str = "auto"
    data_source: str = "siliconflow_tmdb"
    organize_mode: str = "move"  # move / copy / symlink / hardlink / rename
    symlink_source: str = ""  # STRM source dir (rename mode only)
    enabled: bool = True


class FolderUpdate(BaseModel):
    path: Optional[str] = None
    target_root: Optional[str] = None
    media_type: Optional[str] = None
    data_source: Optional[str] = None
    organize_mode: Optional[str] = None
    symlink_source: Optional[str] = None
    enabled: Optional[bool] = None


class FolderOut(BaseModel):
    id: int
    path: str
    target_root: str
    media_type: str
    data_source: str
    organize_mode: str = "move"
    symlink_source: str = ""
    enabled: bool
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


@router.get("/folders", response_model=list[FolderOut])
def list_folders(db: Session = Depends(get_db)):
    rows = db.query(MonitorFolder).order_by(MonitorFolder.id).all()
    result = []
    for r in rows:
        result.append(FolderOut(
            id=r.id, path=r.path, target_root=r.target_root,
            media_type=r.media_type, data_source=r.data_source,
            organize_mode=getattr(r, 'organize_mode', 'move') or 'move',
            symlink_source=getattr(r, 'symlink_source', '') or '',
            enabled=r.enabled,
            created_at=r.created_at.isoformat() if r.created_at else None,
        ))
    return result


@router.post("/folders", response_model=FolderOut)
def create_folder(body: FolderCreate, db: Session = Depends(get_db)):
    if not os.path.isdir(body.path):
        raise HTTPException(400, detail="目录不存在")
    existing = db.query(MonitorFolder).filter(MonitorFolder.path == body.path).first()
    if existing:
        raise HTTPException(400, detail="该目录已添加")
    row = MonitorFolder(
        path=body.path,
        target_root=body.target_root,
        media_type=body.media_type,
        data_source=body.data_source,
        organize_mode=body.organize_mode,
        symlink_source=body.symlink_source,
        enabled=body.enabled,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    # Notify watcher to pick up the new folder
    from server import get_watcher
    w = get_watcher()
    if w:
        w.refresh()

    return FolderOut(
        id=row.id, path=row.path, target_root=row.target_root,
        media_type=row.media_type, data_source=row.data_source,
        organize_mode=getattr(row, 'organize_mode', 'move') or 'move',
        symlink_source=getattr(row, 'symlink_source', '') or '',
        enabled=row.enabled,
        created_at=row.created_at.isoformat() if row.created_at else None,
    )


@router.put("/folders/{folder_id}", response_model=FolderOut)
def update_folder(folder_id: int, body: FolderUpdate, db: Session = Depends(get_db)):
    row = db.query(MonitorFolder).get(folder_id)
    if not row:
        raise HTTPException(404, detail="目录不存在")
    if body.path is not None:
        if not os.path.isdir(body.path):
            raise HTTPException(400, detail="目录不存在")
        row.path = body.path
    if body.target_root is not None:
        row.target_root = body.target_root
    if body.media_type is not None:
        row.media_type = body.media_type
    if body.data_source is not None:
        row.data_source = body.data_source
    if body.organize_mode is not None:
        row.organize_mode = body.organize_mode
    if body.symlink_source is not None:
        row.symlink_source = body.symlink_source
    if body.enabled is not None:
        row.enabled = body.enabled
    db.commit()
    db.refresh(row)

    from server import get_watcher
    w = get_watcher()
    if w:
        w.refresh()

    return FolderOut(
        id=row.id, path=row.path, target_root=row.target_root,
        media_type=row.media_type, data_source=row.data_source,
        organize_mode=getattr(row, 'organize_mode', 'move') or 'move',
        symlink_source=getattr(row, 'symlink_source', '') or '',
        enabled=row.enabled,
        created_at=row.created_at.isoformat() if row.created_at else None,
    )


@router.delete("/folders/{folder_id}")
def delete_folder(folder_id: int, db: Session = Depends(get_db)):
    row = db.query(MonitorFolder).get(folder_id)
    if not row:
        raise HTTPException(404, detail="目录不存在")
    db.delete(row)
    db.commit()

    from server import get_watcher
    w = get_watcher()
    if w:
        w.refresh()

    return {"ok": True}


@router.post("/folders/{folder_id}/scan")
def scan_folder(folder_id: int, db: Session = Depends(get_db)):
    row = db.query(MonitorFolder).get(folder_id)
    if not row:
        raise HTTPException(404, detail="目录不存在")
    if not os.path.isdir(row.path):
        raise HTTPException(400, detail="目录不可访问")

    from server import get_watcher
    w = get_watcher()
    if w:
        import threading
        threading.Thread(target=w.scan_folder, args=(folder_id,), daemon=True).start()

    return {"ok": True, "message": "扫描已启动"}


class BrowseRequest(BaseModel):
    path: str = ""


@router.post("/browse")
def browse_directory(body: BrowseRequest):
    """List drives (when path is empty) or subdirectories of given path."""
    import string

    if not body.path:
        # Return available drive letters on Windows, or / on Unix
        if os.name == "nt":
            drives = []
            for letter in string.ascii_uppercase:
                dp = f"{letter}:\\"
                if os.path.isdir(dp):
                    drives.append({"name": f"{letter}:\\", "path": dp})
            return {"current": "", "parent": "", "dirs": drives}
        else:
            return {"current": "/", "parent": "/", "dirs": _list_subdirs("/")}

    target = os.path.abspath(body.path)
    if not os.path.isdir(target):
        raise HTTPException(400, detail="路径不存在或无法访问")

    parent = os.path.dirname(target)
    if parent == target:
        parent = ""  # at root, go back to drive list

    return {"current": target, "parent": parent, "dirs": _list_subdirs(target)}


def _list_subdirs(base: str):
    result = []
    try:
        for entry in sorted(os.scandir(base), key=lambda e: e.name.lower()):
            if entry.is_dir() and not entry.name.startswith("."):
                result.append({"name": entry.name, "path": entry.path})
    except PermissionError:
        pass
    return result
