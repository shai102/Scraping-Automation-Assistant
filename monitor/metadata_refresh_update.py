import json
import logging
import os
import re

from core.services.worker_context import WorkerContext
from utils.cache import invalidate_cache_prefix


logger = logging.getLogger(__name__)


def refresh_record_metadata(record, db, worker_ctx, broadcast_fn=None) -> bool:
    from db.tmdb_api import (
        fetch_bgm_by_id,
        fetch_hybrid_episode_meta,
        fetch_tmdb_by_id,
        fetch_tmdb_credits,
        fetch_tmdb_episode_meta,
        fetch_tmdb_season_poster,
    )

    if not record.metadata_json or not record.matched_id:
        return False

    try:
        old_meta = json.loads(record.metadata_json)
    except Exception:
        return False

    media_id = str(record.matched_id or "None")
    if media_id == "None":
        return False

    provider = str(record.matched_provider or old_meta.get("provider") or "tmdb").strip().lower()
    is_tmdb = provider == "tmdb"
    media_type = str(old_meta.get("type") or "episode").strip().lower()
    is_tv = media_type == "episode"

    api_tmdb = worker_ctx.tmdb_api_key.get().strip() if worker_ctx else ""
    api_bgm = worker_ctx.bgm_api_key.get().strip() if worker_ctx else ""

    if is_tmdb:
        invalidate_cache_prefix(f"tmdb_detail:{media_id}_")
        invalidate_cache_prefix(f"tmdb_credits:{media_id}_")
        if is_tv:
            invalidate_cache_prefix(f"tmdb_ep_v3:{media_id}_")
            invalidate_cache_prefix(f"tmdb_season_poster:{media_id}_")
    else:
        invalidate_cache_prefix(f"bgm_detail:{media_id}_")

    if is_tmdb:
        _, resolved_id, _, detail_meta = fetch_tmdb_by_id(media_id, is_tv, api_tmdb)
    else:
        _, resolved_id, _, detail_meta = fetch_bgm_by_id(media_id, api_bgm)

    if not detail_meta or resolved_id == "None":
        return False

    new_meta = dict(old_meta)
    for key in ("overview", "poster", "fanart", "release", "original_title", "status"):
        old_val = str(new_meta.get(key) or "").strip()
        new_val = str(detail_meta.get(key) or "").strip()
        if not old_val and new_val:
            new_meta[key] = new_val

    for key in ("genres", "studios"):
        if not new_meta.get(key) and detail_meta.get(key):
            new_meta[key] = detail_meta[key]

    for key in ("rating", "votes", "runtime"):
        try:
            old_value = float(new_meta.get(key) or 0)
        except (TypeError, ValueError):
            old_value = 0
        try:
            new_value = float(detail_meta.get(key) or 0)
        except (TypeError, ValueError):
            new_value = 0
        if old_value == 0 and new_value > 0:
            new_meta[key] = detail_meta[key]

    if is_tmdb and not new_meta.get("actors"):
        actors, directors = fetch_tmdb_credits(media_id, is_tv=is_tv, api_key=api_tmdb)
        if actors:
            new_meta["actors"] = actors
        if directors and not new_meta.get("directors"):
            new_meta["directors"] = directors

    if is_tv:
        season_num = new_meta.get("s", 1)
        episode_num = new_meta.get("e", 1)
        ep_title, ep_plot, ep_still, season_poster = "", "", "", ""

        if is_tmdb:
            title_for_ep = new_meta.get("title") or record.matched_title or ""
            ep_title, ep_plot, ep_still = fetch_tmdb_episode_meta(
                media_id, season_num, episode_num, api_tmdb, title_for_ep, api_bgm
            )
            season_poster = fetch_tmdb_season_poster(media_id, season_num, api_tmdb)
        else:
            title_for_ep = new_meta.get("title") or record.matched_title or ""
            year_for_ep = new_meta.get("year")
            ep_title, ep_plot, ep_still, season_poster = fetch_hybrid_episode_meta(
                title_for_ep, media_id, season_num, episode_num, api_bgm, api_tmdb, year_for_ep
            )

        generic_ep_re = re.compile(r"^第\s*\d+\s*集$")
        old_ep_title = str(new_meta.get("ep_title") or "").strip()
        if ep_title and (not old_ep_title or generic_ep_re.match(old_ep_title)):
            new_meta["ep_title"] = ep_title
        if ep_plot and not str(new_meta.get("ep_plot") or "").strip():
            new_meta["ep_plot"] = ep_plot
        if ep_still and not str(new_meta.get("still") or "").strip():
            new_meta["still"] = ep_still
        if season_poster and not str(new_meta.get("s_poster") or "").strip():
            new_meta["s_poster"] = season_poster

    updated_fields = sorted(
        key
        for key in set(old_meta.keys()) | set(new_meta.keys())
        if old_meta.get(key) != new_meta.get(key)
    )
    if not updated_fields:
        return False

    target_path = str(record.target_path or "").strip()
    if not target_path or not os.path.exists(target_path):
        record.metadata_json = json.dumps(new_meta, ensure_ascii=False)
        db.commit()
        logger.info(
            "元数据刷新: "
            f"record_id={record.id} | title={new_meta.get('title') or '-'} | "
            f"id={media_id} | provider={provider} | "
            f"target_path={target_path or '-'} | "
            f"updated_fields={','.join(updated_fields) or '-'}"
        )
        return True

    ctx = WorkerContext(config=dict(worker_ctx._cfg))
    updated = ctx._refresh_sidecar_files(target_path, old_meta, new_meta)
    record.metadata_json = json.dumps(new_meta, ensure_ascii=False)
    db.commit()
    if updated:
        logger.info(
            "元数据刷新: "
            f"record_id={record.id} | title={new_meta.get('title') or '-'} | "
            f"id={media_id} | provider={provider} | "
            f"target_path={target_path or '-'} | "
            f"updated_fields={','.join(updated_fields) or '-'}"
        )
    return True
