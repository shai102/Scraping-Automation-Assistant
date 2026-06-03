import os

from core.recognition.preview_helpers import (
    prefer_existing_library_target as _prefer_existing_library_target,
    render_media_filename as _render_media_filename,
    retry_rate_limited_siblings as _retry_rate_limited_siblings,
)
from db.tmdb_api import (
    fetch_hybrid_episode_meta,
    fetch_tmdb_credits,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from utils.value_utils import safe_filename, safe_int, safe_str


def populate_preview_item(gui, item, state, match_state, index):
    ep_n, ep_p, ep_s, s_p = "", "", "", ""

    if state["is_tv"] and match_state["tid"] != "None":
        if match_state["eff_tmdb"]:
            ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                match_state["tid"],
                state["season"],
                state["episode_calc"],
                gui.tmdb_api_key.get(),
                match_state["std_title"],
                gui.bgm_api_key.get(),
            )
            s_p = fetch_tmdb_season_poster(
                match_state["tid"], state["season"], gui.tmdb_api_key.get()
            )
        else:
            ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                match_state["std_title"],
                match_state["tid"],
                state["season"],
                state["episode_calc"],
                gui.bgm_api_key.get(),
                gui.tmdb_api_key.get(),
                state["year"],
            )

    fallback_ep_title = state["g"].get("episode_title") or ""
    ep_n_final = ep_n or fallback_ep_title

    season = safe_int(state["season"], 1)
    episode_calc = safe_int(state["episode_calc"], 1)
    s_fmt = f"{int(season):02d}"
    e_fmt = f"{int(episode_calc):02d}"

    v_tag = gui._get_version_tag(item.path)

    safe_std_t = safe_filename(match_state["std_title"])
    safe_ep_name = safe_filename(ep_n_final)

    if state["is_tv"]:
        new_fn, media_suffix = _render_media_filename(
            gui,
            gui.tv_format.get(),
            title=safe_std_t,
            year=state["year"],
            season=s_fmt,
            episode=e_fmt,
            ep_name=safe_ep_name,
            ext=v_tag + state["ext"],
            source_filename=item.old_name,
            pure_name=state["pure"],
            parse_source=item.parse_source or "",
            source_provider="tmdb" if match_state["eff_tmdb"] else "bgm",
            media_id=match_state["tid"],
            is_tv=state["is_tv"],
            original_title=match_state["meta"].get("original_title", ""),
            rating=match_state["meta"].get("rating") or 0,
            genres=match_state["meta"].get("genres") or [],
            studios=match_state["meta"].get("studios") or [],
            overview=match_state["meta"].get("overview", ""),
            ep_plot=ep_p,
            release=match_state["meta"].get("release", ""),
        )
    else:
        new_fn, media_suffix = _render_media_filename(
            gui,
            gui.movie_format.get(),
            title=safe_std_t,
            year=state["year"],
            ext=v_tag + state["ext"],
            source_filename=item.old_name,
            pure_name=state["pure"],
            parse_source=item.parse_source or "",
            source_provider="tmdb" if match_state["eff_tmdb"] else "bgm",
            media_id=match_state["tid"],
            is_tv=state["is_tv"],
            original_title=match_state["meta"].get("original_title", ""),
            rating=match_state["meta"].get("rating") or 0,
            genres=match_state["meta"].get("genres") or [],
            studios=match_state["meta"].get("studios") or [],
            overview=match_state["meta"].get("overview", ""),
            release=match_state["meta"].get("release", ""),
        )

    actors, directors = [], []
    if match_state["eff_tmdb"] and match_state["tid"] and match_state["tid"] != "None":
        actors, directors = fetch_tmdb_credits(
            match_state["tid"], is_tv=state["is_tv"], api_key=gui.tmdb_api_key.get()
        )

    item.metadata = {
        "id": match_state["tid"],
        "provider": "tmdb" if match_state["eff_tmdb"] else "bgm",
        "title": safe_std_t,
        "year": state["year"],
        "ep_title": ep_n_final or f"第 {episode_calc} 集",
        "overview": match_state["meta"].get("overview", ""),
        "ep_plot": ep_p,
        "s": season,
        "e": episode_calc,
        "poster": match_state["meta"].get("poster"),
        "fanart": match_state["meta"].get("fanart"),
        "still": ep_s,
        "s_poster": s_p,
        "type": state["media_type"],
        "actors": actors,
        "directors": directors,
        "genres": match_state["meta"].get("genres") or [],
        "studios": match_state["meta"].get("studios") or [],
        "runtime": match_state["meta"].get("runtime"),
        "status": match_state["meta"].get("status", ""),
        "rating": match_state["meta"].get("rating", 0),
        "votes": match_state["meta"].get("votes", 0),
        "release": match_state["meta"].get("release", ""),
        "original_title": match_state["meta"].get("original_title", ""),
        "parse_source": state["parse_source"],
        "query_title": state["title"],
        "media_suffix": media_suffix,
        "pending_reason": match_state["db_message"],
    }
    item.parse_source = state["parse_source"]
    item.media_suffix = media_suffix
    item.new_name_only = new_fn

    root_d = gui.target_root.get().strip()
    if root_d:
        preserved_target = _prefer_existing_library_target(gui, item, new_fn, item.metadata)
        if preserved_target:
            item.full_target = preserved_target
        else:
            id_tag = (
                f"tmdbid={match_state['tid']}"
                if match_state["eff_tmdb"]
                else f"bgmid={match_state['tid']}"
            )
            folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
            season_folder = f"Season {season}"

            if state["is_tv"]:
                item.full_target = os.path.join(
                    root_d, folder_name, season_folder, new_fn
                )
            else:
                year_text = safe_str(state["year"])
                if year_text:
                    folder_name = safe_filename(
                        f"{safe_std_t} ({year_text}) [{id_tag}]"
                    )
                else:
                    folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
                item.full_target = os.path.join(root_d, folder_name, new_fn)
    else:
        item.full_target = ""

    gui.root.after(
        0,
        lambda: gui.tree.item(
            item.id,
            values=(
                item.old_name,
                safe_std_t,
                match_state["tid"],
                item.full_target or new_fn,
                gui._build_status_text(
                    state["ai_msg"], state["recap_status"], match_state["db_message"]
                ),
            ),
        ),
    )
    if str(item.metadata.get("id") or "None") != "None":
        _retry_rate_limited_siblings(gui, index, state["dir_p"])
