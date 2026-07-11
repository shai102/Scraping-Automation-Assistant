"""Query and search services for scrape records."""

import json
import os
from typing import Optional

from sqlalchemy import or_

from core.services.worker_context import WorkerContext
from db.scrape_models import MonitorFolder, ScrapeRecord
from db.tmdb_api import fetch_bgm_candidates, fetch_tmdb_candidates
from utils.value_utils import normalize_parse_source


def apply_parse_source_filter(query, parse_source: Optional[str]):
    if not parse_source:
        return query
    parse_source = str(parse_source).strip().lower()
    patterns = ("ai", "hybrid") if parse_source == "ai" else (parse_source,)

    clauses = []
    for value in patterns:
        clauses.append(ScrapeRecord.metadata_json.like(f'%\"parse_source\": \"{value}\"%'))
        clauses.append(ScrapeRecord.metadata_json.like(f'%\"parse_source\":\"{value}\"%'))
    return query.filter(or_(*clauses))


def row_to_out_dict(row: ScrapeRecord) -> dict:
    media_type = None
    parse_source = None
    confidence = None
    confidence_level = None
    recognition_trace = []
    recognition_warnings = []
    if row.metadata_json:
        try:
            meta = json.loads(row.metadata_json)
            media_type = meta.get("type")
            parse_source = normalize_parse_source(meta.get("parse_source"))
            confidence = meta.get("confidence")
            confidence_level = meta.get("confidence_level")
            recognition_trace = meta.get("recognition_trace") or []
            recognition_warnings = meta.get("recognition_warnings") or []
        except Exception:
            pass
    return {
        "id": row.id,
        "folder_id": row.folder_id,
        "original_path": row.original_path,
        "original_name": row.original_name,
        "status": row.status,
        "matched_title": row.matched_title,
        "matched_id": row.matched_id,
        "matched_provider": row.matched_provider,
        "target_path": row.target_path,
        "media_type": media_type,
        "parse_source": parse_source,
        "confidence": confidence,
        "confidence_level": confidence_level,
        "recognition_trace": recognition_trace,
        "recognition_warnings": recognition_warnings,
        "error_msg": row.error_msg,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def list_records_payload(
    db,
    *,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    parse_source: Optional[str] = None,
    dir_path: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    query = db.query(ScrapeRecord)
    if status:
        query = query.filter(ScrapeRecord.status == status)
    if keyword:
        query = query.filter(ScrapeRecord.original_name.ilike(f"%{keyword}%"))
    if media_type:
        query = query.join(MonitorFolder, ScrapeRecord.folder_id == MonitorFolder.id, isouter=True)
        query = query.filter(MonitorFolder.media_type == media_type)
    query = apply_parse_source_filter(query, parse_source)
    if dir_path:
        norm_dir = os.path.normpath(dir_path)
        query = query.filter(
            or_(
                ScrapeRecord.target_path.like(norm_dir.replace("\\", "/") + "/%"),
                ScrapeRecord.target_path.like(norm_dir + os.sep + "%"),
                ScrapeRecord.original_path.like(norm_dir.replace("\\", "/") + "/%"),
                ScrapeRecord.original_path.like(norm_dir + os.sep + "%"),
            )
        )
    total = query.count()
    rows = (
        query.order_by(ScrapeRecord.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_out_dict(row) for row in rows],
    }


def list_records_grouped_payload(
    db,
    *,
    record_output_group_dir,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    parse_source: Optional[str] = None,
) -> dict:
    query = db.query(ScrapeRecord)
    if status:
        query = query.filter(ScrapeRecord.status == status)
    if keyword:
        query = query.filter(ScrapeRecord.original_name.ilike(f"%{keyword}%"))
    if media_type:
        query = query.join(MonitorFolder, ScrapeRecord.folder_id == MonitorFolder.id, isouter=True)
        query = query.filter(MonitorFolder.media_type == media_type)
    query = apply_parse_source_filter(query, parse_source)
    rows = query.order_by(ScrapeRecord.id.desc()).all()

    groups: dict[str, dict] = {}
    for row in rows:
        group_dir = record_output_group_dir(row)
        if group_dir not in groups:
            groups[group_dir] = {
                "dir_path": group_dir,
                "dir_name": os.path.basename(group_dir),
                "folder_id": row.folder_id,
                "total": 0,
                "success": 0,
                "failed": 0,
                "pending": 0,
                "ids": [],
            }
        group = groups[group_dir]
        group["total"] += 1
        group["ids"].append(row.id)
        if row.status == "success":
            group["success"] += 1
        elif row.status == "failed":
            group["failed"] += 1
        elif row.status == "pending_manual":
            group["pending"] += 1

    return {"groups": list(groups.values())}


def search_candidates_payload(query: str, year: Optional[int], is_tv: bool, source: str) -> dict:
    ctx = WorkerContext()
    api_key = ctx.tmdb_api_key.get() if source == "siliconflow_tmdb" else ctx.bgm_api_key.get()

    if source == "siliconflow_tmdb":
        results = fetch_tmdb_candidates(query, year, is_tv, api_key)
    else:
        results = fetch_bgm_candidates(query, year, api_key)

    return {"candidates": results or []}
