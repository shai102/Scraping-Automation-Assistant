"""Scrape record queries, manual match, retry, delete."""

import logging

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from core.records.delete_service import (
    batch_delete_records,
    clear_all_records,
    clear_failed_records,
    delete_group_records,
    delete_record_by_id,
    record_output_group_dir as _record_output_group_dir,
)
from core.records.manual_service import (
    batch_update_metadata_from_hub_records,
    batch_refresh_metadata_records,
    batch_retry_records,
    manual_match_record,
    refresh_metadata_for_record,
    retry_record_async,
    update_metadata_from_hub_for_record,
)
from core.records.query_service import (
    list_records_grouped_payload,
    list_records_payload,
    search_candidates_payload,
)
from db.database import get_db, vacuum_db
from db.scrape_models import ScrapeRecord

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/records", tags=["records"])


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

@router.get("", response_model=dict)
def list_records(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    parse_source: Optional[str] = None,
    dir: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    return list_records_payload(
        db,
        status=status,
        keyword=keyword,
        media_type=media_type,
        parse_source=parse_source,
        dir_path=dir,
        page=page,
        page_size=page_size,
    )


@router.get("/grouped")
def list_records_grouped(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    parse_source: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return records grouped by output directory when available."""
    return list_records_grouped_payload(
        db,
        record_output_group_dir=_record_output_group_dir,
        status=status,
        keyword=keyword,
        media_type=media_type,
        parse_source=parse_source,
    )


@router.delete("/{record_id}")
def delete_record(
    record_id: int,
    delete_files: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    return delete_record_by_id(record_id, delete_files, db)


class BatchDeleteBody(BaseModel):
    ids: list[int]
    delete_files: bool = False


class GroupDeleteBody(BaseModel):
    ids: list[int]
    group_dir: str


@router.post("/batch-delete")
def batch_delete(body: BatchDeleteBody, db: Session = Depends(get_db)):
    """Delete multiple records by IDs, optionally also deleting local files."""
    return batch_delete_records(body.ids, body.delete_files, db)


@router.post("/delete-group")
def delete_group(body: GroupDeleteBody, db: Session = Depends(get_db)):
    return delete_group_records(body.ids, body.group_dir, db)


@router.post("/clear-failed")
def clear_failed(db: Session = Depends(get_db)):
    """Delete all failed records."""
    return clear_failed_records(db)


@router.post("/clear-all")
def clear_all(db: Session = Depends(get_db)):
    """Delete all records."""
    return clear_all_records(db, vacuum_db)


@router.post("/batch-retry")
def batch_retry(body: BatchDeleteBody, db: Session = Depends(get_db)):
    """Retry multiple records by deleting them and re-enqueuing the original files."""
    return batch_retry_records(body.ids, db)


@router.post("/search-candidates")
def search_candidates(body: SearchCandidatesBody, db: Session = Depends(get_db)):
    """Search TMDB/BGM candidates for manual matching."""
    return search_candidates_payload(body.query, body.year, body.is_tv, body.source)

@router.post("/{record_id}/manual-match")
def manual_match(record_id: int, body: ManualMatchBody, db: Session = Depends(get_db)):
    """Apply a manually chosen candidate to a pending record, then archive."""
    return manual_match_record(record_id, body, db)


@router.post("/{record_id}/retry")
def retry_record(record_id: int, db: Session = Depends(get_db)):
    """Re-run automatic recognition on a failed/pending record."""
    return retry_record_async(record_id, db)


# ------------------------------------------------------------------
# Metadata refresh — re-fetch from TMDB/BGM without full re-recognition
# ------------------------------------------------------------------

@router.post("/{record_id}/refresh-metadata")
def refresh_metadata(record_id: int, db: Session = Depends(get_db)):
    """Re-fetch metadata for a successful record and update NFO + images."""
    return refresh_metadata_for_record(record_id, db)


class BatchRefreshBody(BaseModel):
    ids: list[int]


@router.post("/batch-refresh-metadata")
def batch_refresh_metadata(body: BatchRefreshBody, db: Session = Depends(get_db)):
    """Re-fetch metadata for multiple successful records."""
    return batch_refresh_metadata_records(body.ids, db)


@router.post("/{record_id}/update-from-metadata-hub")
def update_from_metadata_hub(record_id: int, db: Session = Depends(get_db)):
    """Manually replace NFO and images from the read-only local Metadata Hub."""
    return update_metadata_from_hub_for_record(record_id, db)


@router.post("/batch-update-from-metadata-hub")
def batch_update_from_metadata_hub(body: BatchRefreshBody, db: Session = Depends(get_db)):
    """Apply local Metadata Hub sidecars to selected successful TMDB records."""
    return batch_update_metadata_from_hub_records(body.ids, db)
