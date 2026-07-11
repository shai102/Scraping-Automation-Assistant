from fastapi import APIRouter, Depends

from api.routes.ws import manager
from core.services.runtime_status import build_runtime_status
from db.database import get_db

router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/status")
def runtime_status(db=Depends(get_db)):
    from server import get_watcher

    return build_runtime_status(db, get_watcher(), len(manager.active))
