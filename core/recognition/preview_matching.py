import logging
import os
import re
import threading

from ai.ollama_ai import is_ai_rate_limited_error
from core.recognition.preview_helpers import (
    fetch_ai_parse as _fetch_ai_parse,
    mark_ai_rate_limited as _mark_ai_rate_limited,
    merge_assist_parse as _merge_assist_parse,
    store_dir_parse_cache as _store_dir_parse_cache,
)
from db.tmdb_api import (
    fetch_bgm_by_id,
    fetch_bgm_candidates,
    fetch_tmdb_by_id,
    fetch_tmdb_zh_alternative_title,
)
from utils.library_paths import extract_db_id_from_path
from utils.title_parsing import derive_title_from_filename
from utils.value_utils import extract_year_from_release, safe_str


logger = logging.getLogger(__name__)


def resolve_preview_match(gui, item, state):
    folder_id_title_hints = [
        state["title"],
        state["guess_title"],
        derive_title_from_filename(state["pure"]),
        os.path.basename(os.path.dirname(state["dir_p"] or "")),
        os.path.basename(state["dir_p"] or ""),
    ]
    folder_id_for_cache = (
        extract_db_id_from_path(item.path, state["mode"], folder_id_title_hints) or ""
    )
    cache_key = (
        f"{state['title']}_{safe_str(state['year'])}_{state['is_tv']}_"
        f"{state['mode']}_{folder_id_for_cache}"
    )

    with gui.cache_lock:
        db_c = gui.manual_locks.get(state["path_key"]) or gui.db_cache.get(cache_key)
        pending_event = gui.db_resolution_events.get(cache_key)
        is_resolver = False
        if not db_c and pending_event is None:
            pending_event = threading.Event()
            gui.db_resolution_events[cache_key] = pending_event
            is_resolver = True

    if not db_c:
        if is_resolver:
            try:
                folder_id = extract_db_id_from_path(
                    item.path, state["mode"], folder_id_title_hints
                )
                if folder_id:
                    if state["mode"] == "siliconflow_tmdb":
                        ft, fid, fm, fmeta = fetch_tmdb_by_id(
                            folder_id, state["is_tv"], gui.tmdb_api_key.get()
                        )
                        if fid == "None":
                            ft, fid, fm, fmeta = fetch_tmdb_by_id(
                                folder_id, not state["is_tv"], gui.tmdb_api_key.get()
                            )
                    else:
                        ft, fid, fm, fmeta = fetch_bgm_by_id(
                            folder_id, gui.bgm_api_key.get()
                        )
                    if fid != "None":
                        db_c = (ft, fid, "文件夹ID锁定", fmeta)
                if not db_c:
                    db_c = gui._resolve_db_match(
                        item,
                        state["title"],
                        state["year"],
                        state["is_tv"],
                        state["mode"],
                        state["ai_data"],
                        state["g"],
                    )
                if (
                    state["ai_mode_val"] == "assist"
                    and (not db_c or (len(db_c) >= 2 and db_c[1] == "None"))
                ):
                    if not state["ai_data"]:
                        ai_data, retry_ai_msg = _fetch_ai_parse(
                            gui, state["pure_for_parse"]
                        )
                        if not ai_data and is_ai_rate_limited_error(retry_ai_msg):
                            _mark_ai_rate_limited(item)
                            return None
                        if ai_data:
                            state["ai_data"] = ai_data
                            (
                                state["title"],
                                state["year"],
                                state["season"],
                                state["episode"],
                                state["parse_source"],
                            ) = _merge_assist_parse(
                                gui,
                                state["pure"],
                                state["dir_p"],
                                state["g"],
                                state["guess_title"],
                                state["guess_year"],
                                state["guess_season"],
                                state["guess_episode"],
                                state["extracted_ep"],
                                ai_data,
                            )
                            if state["parse_source"] == "hybrid":
                                state["ai_msg"] = "AI辅助"
                            elif state["parse_source"] == "ai":
                                state["ai_msg"] = "AI识别"
                            with gui.cache_lock:
                                _store_dir_parse_cache(
                                    gui,
                                    state["dir_cache_key"],
                                    ai_data,
                                    state["title"],
                                    state["year"],
                                    state["season"],
                                    state["episode"],
                                    state["parse_source"],
                                    state["cache_title_aliases"],
                                    cache_season=state["guess_season"],
                                )
                    if state["ai_data"]:
                        retry_result = gui._resolve_db_match(
                            item,
                            state["title"],
                            state["year"],
                            state["is_tv"],
                            state["mode"],
                            state["ai_data"],
                            state["g"],
                        )
                        if retry_result and len(retry_result) >= 2 and retry_result[1] != "None":
                            db_c = retry_result
                with gui.cache_lock:
                    if db_c and len(db_c) >= 2 and db_c[1] != "None":
                        gui.db_cache[cache_key] = db_c
                        final_cache_key = (
                            f"{state['title']}_{safe_str(state['year'])}_"
                            f"{state['is_tv']}_{state['mode']}_{folder_id_for_cache}"
                        )
                        if final_cache_key != cache_key:
                            gui.db_cache[final_cache_key] = db_c
            finally:
                with gui.cache_lock:
                    waiter = gui.db_resolution_events.pop(cache_key, None)
                if waiter:
                    waiter.set()
        else:
            if pending_event and not pending_event.wait(timeout=240):
                logger.warning("等待数据库候选解析超时，已跳过缓存复用")
            with gui.cache_lock:
                db_c = gui.manual_locks.get(state["path_key"]) or gui.db_cache.get(
                    cache_key
                )

    if not db_c:
        db_c = (state["title"], "None", "待手动确认", {})

    std_t, tid, db_m, meta = db_c
    provider_name = meta.get("_provider") or (
        "tmdb" if state["mode"] == "siliconflow_tmdb" else "bgm"
    )
    if tid and tid != "None":
        logger.info(
            "资料库匹配: parsed_title=%s | query_title=%s | matched_title=%s | matched_id=%s | provider=%s | result=%s | path=%s",
            state["guess_title"] or "",
            state["title"] or "",
            std_t or "",
            tid,
            provider_name,
            db_m or "",
            item.path,
        )
    else:
        logger.warning(
            "资料库匹配失败: parsed_title=%s | query_title=%s | provider=%s | reason=%s | path=%s",
            state["guess_title"] or "",
            state["title"] or "",
            provider_name,
            db_m or "未命中",
            item.path,
        )

    is_bgm_fallback = meta.get("_provider") == "bgm"
    eff_tmdb = state["mode"] == "siliconflow_tmdb" and not is_bgm_fallback

    if eff_tmdb and tid and tid != "None" and not meta.get("genres"):
        _, _, _, detail_meta = fetch_tmdb_by_id(
            tid, state["is_tv"], gui.tmdb_api_key.get()
        )
        if detail_meta:
            meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}

    if (
        eff_tmdb
        and tid != "None"
        and std_t
        and re.search(r"[A-Za-z]", std_t)
        and not re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", std_t)
    ):
        zh_title = ""
        resolved_is_tv = state["is_tv"]
        detail_t, detail_id, _, detail_meta = fetch_tmdb_by_id(
            tid, resolved_is_tv, gui.tmdb_api_key.get()
        )
        if detail_id == "None":
            resolved_is_tv = not state["is_tv"]
            detail_t, detail_id, _, detail_meta = fetch_tmdb_by_id(
                tid, resolved_is_tv, gui.tmdb_api_key.get()
            )
        if (
            detail_id != "None"
            and detail_t
            and re.search(r"[\u4e00-\u9fff]", str(detail_t))
        ):
            zh_title = str(detail_t)
            if detail_meta and not meta.get("genres"):
                meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}
        if not zh_title:
            # zh-CN \u8be6\u60c5\u6807\u9898\u7f3a\u5931\uff08TMDB \u7ffb\u8bd1\u672a\u586b\u5199\uff09\u65f6\uff0c\u67e5\u522b\u540d\u63a5\u53e3\u515c\u5e95
            zh_title = fetch_tmdb_zh_alternative_title(
                tid, resolved_is_tv, gui.tmdb_api_key.get()
            )
        if zh_title:
            std_t = zh_title
        else:
            release_year = extract_year_from_release(meta.get("release", ""))
            if release_year:
                bgm_cands = fetch_bgm_candidates(
                    std_t, year=release_year, api_key=gui.bgm_api_key.get()
                )
                if bgm_cands:
                    bgm_cn_title = bgm_cands[0].get("title", "")
                    if bgm_cn_title and re.search(r"[\u4e00-\u9fff]", bgm_cn_title):
                        std_t = bgm_cn_title

    return {
        "std_title": std_t,
        "tid": tid,
        "db_message": db_m,
        "meta": meta,
        "provider_name": provider_name,
        "eff_tmdb": eff_tmdb,
    }
