"""Preview UI helpers extracted from task_runner."""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from db.tmdb_api import (
    fetch_hybrid_episode_meta,
    fetch_tmdb_by_id,
    fetch_tmdb_credits,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from utils.error_utils import ERROR_CODE_UNKNOWN, format_error_message
from utils.value_utils import extract_year_from_release, safe_filename, safe_int, safe_str

from .preview_helpers import notify_error, prefer_existing_library_target, render_media_filename


def async_batch_runner(gui, indices, title, t_id, msg, meta):
    with ThreadPoolExecutor(max_workers=gui._get_sync_workers()) as executor:
        futures = [
            executor.submit(gui._bg_update_single_ui, idx, title, t_id, msg, meta)
            for idx in indices
        ]
        for _future in as_completed(futures):
            gui.root.after(0, lambda: gui.pbar.step(1))
    gui.root.after(0, lambda: gui.status.config(text="同步完成！"))


def bg_update_single_ui(gui, idx, title, t_id, msg, meta):
    from guessit import guessit
    from utils.title_parsing import derive_title_from_filename, extract_episode_number
    from utils.library_paths import extract_db_id_from_path

    item = None
    try:
        mode = gui.source_var.get()
        is_bgm_fallback = meta.get("_provider") == "bgm"
        eff_tmdb = mode == "siliconflow_tmdb" and not is_bgm_fallback
        if eff_tmdb and t_id and t_id != "None" and not meta.get("genres"):
            _, _, _, detail_meta = fetch_tmdb_by_id(t_id, True, gui.tmdb_api_key.get())
            if not detail_meta:
                _, _, _, detail_meta = fetch_tmdb_by_id(t_id, False, gui.tmdb_api_key.get())
            if detail_meta:
                meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}
        item = gui.file_list[idx]
        pure, ext = gui.extract_lang_and_ext(item.old_name)
        guessed = guessit(pure)
        current_meta = item.metadata or {}
        path_key = item.path

        forced_s = gui.forced_seasons.get(path_key)
        season = forced_s if forced_s is not None else gui._pick_season(pure, guessed, current_meta.get("s", 1))

        extracted_ep = extract_episode_number(pure, guessed)
        raw_e = extracted_ep if extracted_ep is not None else guessed.get("episode")
        if raw_e in (None, ""):
            raw_e = current_meta.get("e", 1)
        if isinstance(raw_e, list):
            raw_e = raw_e[0]

        forced_o = gui.forced_offsets.get(path_key, 0)
        e_calc = raw_e
        if forced_o != 0 and str(raw_e).isdigit():
            e_calc = max(1, int(raw_e) + forced_o)

        year = guessed.get("year") or current_meta.get("year")
        media_type = gui._resolve_media_type({"type": current_meta.get("type", "episode")})
        is_tv = media_type == "episode"

        ep_n, ep_p, ep_s, s_p = "", "", "", ""
        if is_tv and t_id != "None" and title:
            if eff_tmdb:
                ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                    t_id, season, e_calc, gui.tmdb_api_key.get(), title, gui.bgm_api_key.get()
                )
                s_p = fetch_tmdb_season_poster(t_id, season, gui.tmdb_api_key.get())
            else:
                ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                    title, t_id, season, e_calc, gui.bgm_api_key.get(), gui.tmdb_api_key.get(), year
                )

        fallback_ep_title = guessed.get("episode_title") or ""
        ep_n_final = ep_n or fallback_ep_title

        season = safe_int(season, 1)
        e_calc = safe_int(e_calc, 1)
        s_fmt = f"{int(season):02d}"
        e_fmt = f"{int(e_calc):02d}"

        v_tag = gui._get_version_tag(item.path)
        safe_title = safe_filename(title)
        safe_ep_name = safe_filename(ep_n_final)

        if is_tv:
            new_fn, media_suffix = render_media_filename(
                gui, gui.tv_format.get(), title=safe_title, year=year, season=s_fmt,
                episode=e_fmt, ep_name=safe_ep_name, ext=v_tag + ext,
                source_filename=item.old_name, pure_name=pure,
                parse_source=item.parse_source or "", source_provider="tmdb" if eff_tmdb else "bgm",
                media_id=t_id, is_tv=is_tv, original_title=meta.get("original_title", ""),
                rating=meta.get("rating") or 0, genres=meta.get("genres") or [],
                studios=meta.get("studios") or [], overview=meta.get("overview", ""),
                ep_plot=ep_p, release=meta.get("release", ""),
            )
        else:
            new_fn, media_suffix = render_media_filename(
                gui, gui.movie_format.get(), title=safe_title, year=year, ext=v_tag + ext,
                source_filename=item.old_name, pure_name=pure,
                parse_source=item.parse_source or "", source_provider="tmdb" if eff_tmdb else "bgm",
                media_id=t_id, is_tv=is_tv, original_title=meta.get("original_title", ""),
                rating=meta.get("rating") or 0, genres=meta.get("genres") or [],
                studios=meta.get("studios") or [], overview=meta.get("overview", ""),
                release=meta.get("release", ""),
            )

        actors, directors = [], []
        if eff_tmdb and t_id and t_id != "None":
            actors, directors = fetch_tmdb_credits(t_id, is_tv=is_tv, api_key=gui.tmdb_api_key.get())

        item.metadata = {
            "id": t_id,
            "provider": "tmdb" if eff_tmdb else "bgm",
            "title": safe_title,
            "year": year,
            "ep_title": ep_n_final or f"第 {e_calc} 集",
            "overview": meta.get("overview", ""),
            "ep_plot": ep_p,
            "s": season,
            "e": e_calc,
            "poster": meta.get("poster"),
            "fanart": meta.get("fanart"),
            "still": ep_s,
            "s_poster": s_p,
            "type": media_type,
            "actors": actors,
            "directors": directors,
            "genres": meta.get("genres") or [],
            "studios": meta.get("studios") or [],
            "runtime": meta.get("runtime"),
            "status": meta.get("status", ""),
            "rating": meta.get("rating", 0),
            "votes": meta.get("votes", 0),
            "release": meta.get("release", ""),
            "original_title": meta.get("original_title", ""),
            "parse_source": "guessit",
            "media_suffix": media_suffix,
        }
        item.media_suffix = media_suffix
        item.new_name_only = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            preserved_target = prefer_existing_library_target(gui, item, new_fn, item.metadata)
            if preserved_target:
                item.full_target = preserved_target
            else:
                id_tag = f"tmdbid={t_id}" if eff_tmdb else f"bgmid={t_id}"
                folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                season_folder = f"Season {season}"
                if is_tv:
                    item.full_target = os.path.join(root_d, folder_name, season_folder, new_fn)
                else:
                    year_text = safe_str(year)
                    if year_text:
                        folder_name = safe_filename(f"{safe_title} ({year_text}) [{id_tag}]")
                    else:
                        folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                    item.full_target = os.path.join(root_d, folder_name, new_fn)
        else:
            item.full_target = ""

        gui.root.after(
            0,
            lambda: gui.tree.item(
                item.id,
                values=(item.old_name, safe_title, t_id, item.full_target or new_fn, msg),
            ),
        )
    except Exception as err:
        logging.error(f"更新UI失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"更新失败: {str(err)[:30]}")
        if item and item.id:
            gui.root.after(
                0,
                lambda id_val=item.id, msg=err_msg: gui.tree.set(
                    id_val, "st", gui._friendly_status_text(msg)
                ),
            )
        else:
            gui.root.after(
                0,
                lambda msg=err_msg: gui.status.config(text=gui._friendly_status_text(msg)),
            )


def run_preview_pool(gui):
    total = len(gui.file_list)
    gui.root.after(0, lambda max_v=total: gui.pbar.config(maximum=max_v))
    try:
        with ThreadPoolExecutor(max_workers=gui._get_preview_workers()) as executor:
            list(executor.map(gui.process_task, range(total)))
    except Exception as err:
        logging.error(f"预览处理失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"处理失败: {str(err)[:30]}")
        gui.root.after(0, lambda msg=err_msg: notify_error(gui, "错误", msg))

    def _finish_preview_ui():
        gui.btn_pre.config(state="normal")
        if gui.preview_skip_all_event.is_set():
            gui.status.config(text="已终止本轮剩余识别")
        else:
            gui.status.config(text="预览完成")

    gui.root.after(0, _finish_preview_ui)
