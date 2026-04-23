import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from guessit import guessit

from ai.ollama_ai import fetch_siliconflow_info
from db.tmdb_api import (
    fetch_bgm_by_id,
    fetch_hybrid_episode_meta,
    fetch_tmdb_by_id,
    fetch_tmdb_credits,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from core.workers.execution_runner import (
    process_one_file as execution_process_one_file,
    process_one_file_scrape as execution_process_one_file_scrape,
    run_execution as execution_run_execution,
    run_scrape_execution as execution_run_scrape_execution,
)
from core.services.naming_service import extract_season_from_dir
from utils.helpers import (
    ERROR_CODE_UNKNOWN,
    derive_title_from_filename,
    extract_db_id_from_path,
    extract_episode_number,
    format_error_message,
    normalize_compare_text,
    safe_filename,
    safe_int,
    safe_str,
)


SPECIAL_TAG_RE = re.compile(
    r"(?i)(?<![A-Z0-9])(?:PROLOGUE|OVA|OAD|SP|SPECIAL|NC\.VER|EXTRA)(?![A-Z0-9])"
)
SPECIAL_EPISODE_RE = re.compile(
    r"(?i)(?<![A-Z0-9])(?:SP|OVA|OAD|SPECIAL|EXTRA)(?![A-Z0-9])\s*(?:BD)?\s*0*(\d+)"
)
PROLOGUE_RE = re.compile(r"(?i)(?<![A-Z0-9])PROLOGUE(?![A-Z0-9])")
GROUP_RELEASE_RE = re.compile(r"^(?:\[[^\]]+\]\s*){2,}")
GENERIC_TITLE_RE = re.compile(
    r"(?i)^(?:unknown|none|null|untitled|na|nan|未知|season\s*\d{1,2}|s\s*\d{1,2}|第\s*\d{1,2}\s*季)$"
)
GENERIC_SEASON_DIR_RE = re.compile(r"(?i)^(?:season\s*\d{1,2}|s\s*\d{1,2}|第\s*\d{1,2}\s*季)$")


def _is_meaningful_title(title):
    raw = str(title or "").strip()
    if not raw:
        return False
    if GENERIC_TITLE_RE.match(raw):
        return False
    return bool(normalize_compare_text(raw))


def _notify_error(gui, title, message):
    """Report a worker error without depending on Tkinter."""
    handler = getattr(gui, "show_error", None)
    if callable(handler):
        try:
            handler(title, message)
            return
        except Exception:
            pass
    logging.error("%s: %s", title, message)


def _fetch_ai_parse(gui, pure_for_parse):
    """Fetch parse result from the configured AI backend."""
    if gui.prefer_ollama.get():
        if gui.ollama_url.get().strip() and gui.ollama_model.get().strip():
            ai_data, ai_msg = gui._parse_with_ollama(pure_for_parse)
            if ai_data is None and gui.sf_api_key.get().strip():
                ai_data, ai_msg = fetch_siliconflow_info(
                    pure_for_parse,
                    gui.sf_api_key.get(),
                    gui.sf_api_url.get(),
                    gui.sf_model.get(),
                    gui._get_ai_temperature(),
                    gui._get_ai_top_p(),
                )
            return ai_data, ai_msg
        if gui.sf_api_key.get().strip():
            return fetch_siliconflow_info(
                pure_for_parse,
                gui.sf_api_key.get(),
                gui.sf_api_url.get(),
                gui.sf_model.get(),
                gui._get_ai_temperature(),
                gui._get_ai_top_p(),
            )
        return None, ""

    if gui.sf_api_key.get().strip():
        return fetch_siliconflow_info(
            pure_for_parse,
            gui.sf_api_key.get(),
            gui.sf_api_url.get(),
            gui.sf_model.get(),
            gui._get_ai_temperature(),
            gui._get_ai_top_p(),
        )
    return None, ""


def _derive_guessit_fields(gui, pure, dir_p, g, extracted_ep):
    """Build the baseline parse result from guessit and directory hints."""
    title = g.get("title") or derive_title_from_filename(pure) or "未知"
    year = g.get("year")
    if not year:
        dir_for_year = dir_p
        for _ in range(3):
            folder_name = os.path.basename(dir_for_year)
            year_match = re.search(r"\b((?:19|20)\d{2})\b", folder_name)
            if year_match:
                year = int(year_match.group(1))
                break
            parent_dir = os.path.dirname(dir_for_year)
            if not parent_dir or parent_dir == dir_for_year:
                break
            dir_for_year = parent_dir
    dir_season = extract_season_from_dir(dir_p)
    season = gui._pick_season(pure, g, dir_season if dir_season is not None else 1)
    episode = extracted_ep or 1
    return title, year, season, episode


def _guessit_needs_assist(pure, dir_p, g, title, extracted_ep):
    """Heuristics for deciding whether assist mode should invoke AI early."""
    title_norm = normalize_compare_text(title)
    if not _is_meaningful_title(title):
        return True
    if len(title_norm) <= 2:
        return True
    if extracted_ep is None:
        return True
    if GROUP_RELEASE_RE.search(str(pure or "")):
        return True

    guess_title = str(g.get("title") or "").strip()
    derived_title = derive_title_from_filename(pure)
    if (
        guess_title
        and _is_meaningful_title(derived_title)
        and normalize_compare_text(guess_title) != normalize_compare_text(derived_title)
    ):
        return True

    dir_name = os.path.basename(dir_p or "").strip()
    if GENERIC_SEASON_DIR_RE.match(dir_name):
        parent_title = os.path.basename(os.path.dirname(dir_p or "")).strip()
        if _is_meaningful_title(parent_title):
            if normalize_compare_text(parent_title) != title_norm:
                return True

    return False


def _build_dir_cache_entry(ai_data, title, year, season, episode, parse_source):
    cache_data = dict(ai_data or {})
    cache_data.update(
        {
            "title": title,
            "year": year,
            "season": season,
            "episode": episode,
            "parse_source": parse_source,
        }
    )
    return cache_data


def _merge_assist_parse(
    gui,
    pure,
    dir_p,
    g,
    guess_title,
    guess_year,
    guess_season,
    guess_episode,
    extracted_ep,
    ai_data,
):
    """Merge guessit baseline with AI output in assist mode."""
    title = guess_title
    year = guess_year
    season = guess_season
    episode = guess_episode
    used_fields = set()

    ai_title = str((ai_data or {}).get("title") or "").strip()
    ai_year = (ai_data or {}).get("year")
    ai_season = safe_int((ai_data or {}).get("season"), 1)
    ai_episode = extract_episode_number(pure, None, ai_data) or safe_int(
        (ai_data or {}).get("episode"), 1
    )

    if _is_meaningful_title(ai_title):
        if not _is_meaningful_title(title):
            title = ai_title
            used_fields.add("title")
        elif normalize_compare_text(ai_title) != normalize_compare_text(title):
            title = ai_title
            used_fields.add("title")

    if ai_year and (not year or "title" in used_fields):
        year = ai_year
        used_fields.add("year")

    explicit_season = gui._extract_explicit_season(pure)
    dir_season = extract_season_from_dir(dir_p)
    if explicit_season is None and dir_season is None and ai_season >= 1:
        if ai_season != safe_int(season, 1):
            season = gui._pick_season(pure, g, ai_season)
            used_fields.add("season")

    if extracted_ep is None and ai_episode:
        if ai_episode != safe_int(episode, 1):
            episode = ai_episode
            used_fields.add("episode")

    guessit_reliable = _is_meaningful_title(guess_title) and extracted_ep is not None
    if used_fields:
        parse_source = "hybrid" if guessit_reliable else "ai"
    else:
        parse_source = "guessit"

    return title, year, season, episode, parse_source


def async_batch_runner(gui, indices, title, t_id, msg, meta):
    """Run background sync updates for selected files."""
    with ThreadPoolExecutor(max_workers=gui._get_sync_workers()) as executor:
        futures = [
            executor.submit(gui._bg_update_single_ui, idx, title, t_id, msg, meta)
            for idx in indices
        ]
        for _future in as_completed(futures):
            gui.root.after(0, lambda: gui.pbar.step(1))

    gui.root.after(0, lambda: gui.status.config(text="同步完成！"))


def bg_update_single_ui(gui, idx, title, t_id, msg, meta):
    """Update single row metadata and naming in background sync flow."""
    item = None
    try:
        # 当 TMDb 无结果回退到 BGM 时，meta 中会有 _provider="bgm" 标记
        mode = gui.source_var.get()
        _is_bgm_fallback = (meta.get("_provider") == "bgm")
        _eff_tmdb = (mode == "siliconflow_tmdb" and not _is_bgm_fallback)
        if _eff_tmdb and t_id and t_id != "None" and not meta.get("genres"):
            _, _, _, detail_meta = fetch_tmdb_by_id(t_id, True, gui.tmdb_api_key.get())
            if not detail_meta:
                _, _, _, detail_meta = fetch_tmdb_by_id(t_id, False, gui.tmdb_api_key.get())
            if detail_meta:
                meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}
        item = gui.file_list[idx]
        pure, ext = gui.extract_lang_and_ext(item.old_name)
        g = guessit(pure)
        m = item.metadata or {}
        path_key = item.path

        forced_s = gui.forced_seasons.get(path_key)
        s = (
            forced_s
            if forced_s is not None
            else gui._pick_season(pure, g, m.get("s", 1))
        )

        raw_e = g.get("episode") or m.get("e", 1)
        if isinstance(raw_e, list):
            raw_e = raw_e[0]

        forced_o = gui.forced_offsets.get(path_key, 0)
        e_calc = raw_e
        if forced_o != 0 and str(raw_e).isdigit():
            e_calc = max(1, int(raw_e) + forced_o)

        y = g.get("year") or m.get("year")
        media_type = gui._resolve_media_type({"type": m.get("type", "episode")})
        is_tv = media_type == "episode"

        ep_n, ep_p, ep_s, s_p = "", "", "", ""
        if is_tv and t_id != "None" and title:
            if _eff_tmdb:
                ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                    t_id,
                    s,
                    e_calc,
                    gui.tmdb_api_key.get(),
                    title,
                    gui.bgm_api_key.get(),
                )
                s_p = fetch_tmdb_season_poster(t_id, s, gui.tmdb_api_key.get())
            else:
                ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                    title,
                    t_id,
                    s,
                    e_calc,
                    gui.bgm_api_key.get(),
                    gui.tmdb_api_key.get(),
                )

        fallback_ep_title = g.get("episode_title") or ""
        ep_n_final = ep_n or fallback_ep_title

        s = safe_int(s, 1)
        e_calc = safe_int(e_calc, 1)
        s_fmt = f"{int(s):02d}"
        e_fmt = f"{int(e_calc):02d}"

        v_tag = gui._get_version_tag(item.path)
        safe_title = safe_filename(title)
        safe_ep_name = safe_filename(ep_n_final)

        if is_tv:
            new_fn = (
                gui.tv_format.get()
                .replace("{title}", safe_title)
                .replace("{year}", safe_str(y))
                .replace("{s:02d}", s_fmt)
                .replace("{s}", s_fmt)
                .replace("{e:02d}", e_fmt)
                .replace("{e}", e_fmt)
                .replace("{ep_name}", safe_ep_name)
                .replace("{ext}", v_tag + ext)
            )
        else:
            new_fn = (
                gui.movie_format.get()
                .replace("{title}", safe_title)
                .replace("{year}", safe_str(y))
                .replace("{ext}", v_tag + ext)
            )

        new_fn = re.sub(r"\s*\(\s*\)", "", new_fn)
        new_fn = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
        new_fn = re.sub(r"\s+(?=\.)", "", new_fn).strip()

        actors, directors = [], []
        if _eff_tmdb and t_id and t_id != "None":
            actors, directors = fetch_tmdb_credits(
                t_id, is_tv=is_tv, api_key=gui.tmdb_api_key.get()
            )

        item.metadata = {
            "id": t_id,
            "provider": "tmdb" if _eff_tmdb else "bgm",
            "title": safe_title,
            "year": y,
            "ep_title": ep_n_final or f"第 {e_calc} 集",
            "overview": meta.get("overview", ""),
            "ep_plot": ep_p,
            "s": s,
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
        }
        item.new_name_only = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            id_tag = f"tmdbid={t_id}" if _eff_tmdb else f"bgmid={t_id}"
            folder_name = safe_filename(f"{safe_title} [{id_tag}]")
            season_folder = f"Season {s}"
            if is_tv:
                item.full_target = os.path.join(
                    root_d, folder_name, season_folder, new_fn
                )
            else:
                year_text = safe_str(y)
                if year_text:
                    folder_name = safe_filename(
                        f"{safe_title} ({year_text}) [{id_tag}]"
                    )
                else:
                    folder_name = safe_filename(f"{safe_title} [{id_tag}]")
                item.full_target = os.path.join(root_d, folder_name, new_fn)
        else:
            item.full_target = ""

        gui.root.after(
            0,
            lambda: gui.tree.item(
                item.id,
                values=(
                    item.old_name,
                    safe_title,
                    t_id,
                    item.full_target or new_fn,
                    msg,
                ),
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
                lambda msg=err_msg: gui.status.config(
                    text=gui._friendly_status_text(msg)
                ),
            )


def run_preview_pool(gui):
    """Run preview recognition tasks with configured worker count."""
    total = len(gui.file_list)
    gui.root.after(0, lambda max_v=total: gui.pbar.config(maximum=max_v))

    try:
        with ThreadPoolExecutor(max_workers=gui._get_preview_workers()) as executor:
            list(executor.map(gui.process_task, range(total)))
    except Exception as err:
        logging.error(f"预览处理失败: {err}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"处理失败: {str(err)[:30]}")
        gui.root.after(0, lambda msg=err_msg: _notify_error(gui, "错误", msg))

    def _finish_preview_ui():
        gui.btn_pre.config(state="normal")
        if gui.preview_skip_all_event.is_set():
            gui.status.config(text="已终止本轮剩余识别")
        else:
            gui.status.config(text="预览完成")

    gui.root.after(0, _finish_preview_ui)


def process_task(gui, i):
    """Process a single preview task."""
    item = gui.file_list[i]

    try:
        if gui.preview_skip_all_event.is_set() or item.dir in gui.preview_skip_dirs:
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "已跳过")
            )
            return

        gui.root.after(
            0, lambda id_val=item.id: gui.tree.set(id_val, "st", "识别中")
        )

        if gui.preview_skip_all_event.is_set() or item.dir in gui.preview_skip_dirs:
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "已跳过")
            )
            return

        pure, ext = gui.extract_lang_and_ext(item.old_name)
        dir_p = item.dir
        mode = gui.source_var.get()

        # Apply strip_keywords: remove user-defined keywords before recognition
        strip_kw = getattr(gui, 'strip_keywords', None) or []
        pure_for_parse = pure
        if strip_kw:
            for kw in strip_kw:
                if kw:
                    pure_for_parse = re.sub(re.escape(kw), ' ', pure_for_parse, flags=re.IGNORECASE)
            pure_for_parse = re.sub(r'\s+', ' ', pure_for_parse).strip()

        g = guessit(pure_for_parse)

        extracted_ep = extract_episode_number(pure, g)
        guess_title, guess_year, guess_season, guess_episode = _derive_guessit_fields(
            gui, pure, dir_p, g, extracted_ep
        )
        guessit_needs_assist = _guessit_needs_assist(
            pure, dir_p, g, guess_title, extracted_ep
        )
        guessit_confident = not guessit_needs_assist

        # 读取 AI 模式（在缓存判断前确定，后续 is_resolver 块也可访问）
        _ai_mode_obj = getattr(gui, "ai_mode", None)
        ai_mode_val = _ai_mode_obj.get() if _ai_mode_obj else "assist"

        with gui.cache_lock:
            cached_ai = gui.dir_cache.get(dir_p)

        parse_source = "guessit"  # default; overridden when AI is actually used

        if cached_ai and gui._can_reuse_dir_ai(cached_ai, pure, g):
            t = cached_ai["title"]
            y = cached_ai.get("year")
            s = gui._pick_season(pure, g, cached_ai.get("season") or 1)
            e = extracted_ep or cached_ai.get("episode") or 1
            ai_msg = "复用"
            ai_data = cached_ai
            parse_source = cached_ai.get("parse_source", "guessit")
        else:
            ai_data = None
            t = guess_title
            y = guess_year
            s = guess_season
            e = guess_episode
            ai_msg = "猜测"

            if ai_mode_val == "force":
                # 强制使用 AI：只调 AI，失败则直接待手动
                ai_data, ai_msg = _fetch_ai_parse(gui, pure_for_parse)
                if ai_data:
                    t = ai_data.get("title", "未知")
                    y = ai_data.get("year")
                    ai_season = safe_int(ai_data.get("season"), 1)
                    if ai_season < 1:
                        ai_season = 1
                    s = gui._pick_season(pure, g, ai_season)
                    e = (
                        extracted_ep
                        or extract_episode_number(pure, None, ai_data)
                        or safe_int(ai_data.get("episode"), 1)
                    )
                    parse_source = "ai"
                    with gui.cache_lock:
                        gui.dir_cache[dir_p] = _build_dir_cache_entry(
                            ai_data, t, y, s, e, "ai"
                        )
                else:
                    # AI 失败 → 直接待手动，不猜测
                    item.metadata = {"id": "None"}
                    item.new_name_only = ""
                    return
            else:
                if ai_mode_val == "assist" and guessit_needs_assist:
                    ai_data, ai_msg = _fetch_ai_parse(gui, pure_for_parse)
                    if ai_data:
                        t, y, s, e, parse_source = _merge_assist_parse(
                            gui,
                            pure,
                            dir_p,
                            g,
                            guess_title,
                            guess_year,
                            guess_season,
                            guess_episode,
                            extracted_ep,
                            ai_data,
                        )
                        if parse_source == "hybrid":
                            ai_msg = "AI辅助"
                        elif parse_source == "ai":
                            ai_msg = "AI识别"
                if parse_source == "guessit" and guessit_confident:
                    with gui.cache_lock:
                        if dir_p not in gui.dir_cache:
                            gui.dir_cache[dir_p] = _build_dir_cache_entry(
                                None, t, y, s, e, "guessit"
                            )
                elif parse_source != "guessit":
                    with gui.cache_lock:
                        gui.dir_cache[dir_p] = _build_dir_cache_entry(
                            ai_data, t, y, s, e, parse_source
                        )

        if SPECIAL_TAG_RE.search(pure):
            # 若文件名已有显式 S\d+E\d+ 标记（如 S01E01），尊重该标记，
            # 不强制覆盖为 Season 0，避免把 OVA 系列误归入特别篇。
            explicit_s_in_name = gui._extract_explicit_season(pure)
            if explicit_s_in_name is None:
                s = 0
                sp_match = SPECIAL_EPISODE_RE.search(pure)
                if sp_match:
                    e = int(sp_match.group(1))
                elif PROLOGUE_RE.search(pure):
                    e = 0

        media_type = gui._resolve_media_type(g)
        is_tv = media_type == "episode"
        path_key = item.path

        forced_s = gui.forced_seasons.get(path_key)
        if forced_s is not None:
            s = forced_s

        forced_o = gui.forced_offsets.get(path_key, 0)
        e_calc = e

        if isinstance(e, list):
            e = e[0]
            e_calc = e

        if forced_o != 0:
            e_calc = max(1, safe_int(e, 1) + forced_o)

        _folder_id_for_cache = extract_db_id_from_path(item.path, mode) or ""
        cache_key = f"{t}_{safe_str(y)}_{is_tv}_{mode}_{_folder_id_for_cache}"

        with gui.cache_lock:
            db_c = gui.manual_locks.get(path_key) or gui.db_cache.get(cache_key)
            pending_event = gui.db_resolution_events.get(cache_key)
            is_resolver = False
            if not db_c and pending_event is None:
                import threading

                pending_event = threading.Event()
                gui.db_resolution_events[cache_key] = pending_event
                is_resolver = True

        if not db_c:
            if is_resolver:
                try:
                    folder_id = extract_db_id_from_path(item.path, mode)
                    if folder_id:
                        if mode == "siliconflow_tmdb":
                            _ft, _fid, _fm, _fmeta = fetch_tmdb_by_id(
                                folder_id, is_tv, gui.tmdb_api_key.get()
                            )
                            if _fid == "None":
                                _ft, _fid, _fm, _fmeta = fetch_tmdb_by_id(
                                    folder_id, not is_tv, gui.tmdb_api_key.get()
                                )
                        else:
                            _ft, _fid, _fm, _fmeta = fetch_bgm_by_id(
                                folder_id, gui.bgm_api_key.get()
                            )
                        if _fid != "None":
                            db_c = (_ft, _fid, "文件夹ID锁定", _fmeta)
                    if not db_c:
                        db_c = gui._resolve_db_match(item, t, y, is_tv, mode, ai_data, g)
                    # 辅助识别模式：数据库无结果时，再让 AI 参与一次重试。
                    if (ai_mode_val == "assist"
                            and (not db_c or (len(db_c) >= 2 and db_c[1] == "None"))):
                        if not ai_data:
                            ai_data, _ = _fetch_ai_parse(gui, pure_for_parse)
                            if ai_data:
                                t, y, s, e, parse_source = _merge_assist_parse(
                                    gui,
                                    pure,
                                    dir_p,
                                    g,
                                    guess_title,
                                    guess_year,
                                    guess_season,
                                    guess_episode,
                                    extracted_ep,
                                    ai_data,
                                )
                                if parse_source == "hybrid":
                                    ai_msg = "AI辅助"
                                elif parse_source == "ai":
                                    ai_msg = "AI识别"
                                with gui.cache_lock:
                                    gui.dir_cache[dir_p] = _build_dir_cache_entry(
                                        ai_data, t, y, s, e, parse_source
                                    )
                        if ai_data:
                            _db_retry = gui._resolve_db_match(item, t, y, is_tv, mode, ai_data, g)
                            if _db_retry and len(_db_retry) >= 2 and _db_retry[1] != "None":
                                db_c = _db_retry
                    with gui.cache_lock:
                        if db_c and len(db_c) >= 2 and db_c[1] != "None":
                            gui.db_cache[cache_key] = db_c
                            final_cache_key = f"{t}_{safe_str(y)}_{is_tv}_{mode}_{_folder_id_for_cache}"
                            if final_cache_key != cache_key:
                                gui.db_cache[final_cache_key] = db_c
                finally:
                    with gui.cache_lock:
                        waiter = gui.db_resolution_events.pop(cache_key, None)
                    if waiter:
                        waiter.set()
            else:
                if pending_event and not pending_event.wait(timeout=240):
                    logging.warning("等待数据库候选解析超时，已跳过缓存复用")
                with gui.cache_lock:
                    db_c = gui.manual_locks.get(path_key) or gui.db_cache.get(cache_key)

        if not db_c:
            db_c = (t, "None", "待手动确认", {})

        std_t, tid, db_m, meta = db_c

        # 当 TMDb 无结果回退到 BGM 时，meta 中会有 _provider="bgm" 标记
        _is_bgm_fallback = (meta.get("_provider") == "bgm")
        _eff_tmdb = (mode == "siliconflow_tmdb" and not _is_bgm_fallback)

        # 搜索路径返回的 meta 缺少 genres/runtime/status/studios，用 detail 接口补全
        if _eff_tmdb and tid and tid != "None" and not meta.get("genres"):
            _, _, _, detail_meta = fetch_tmdb_by_id(tid, is_tv, gui.tmdb_api_key.get())
            if detail_meta:
                meta = {**detail_meta, **{k: v for k, v in meta.items() if v}}

        ep_n, ep_p, ep_s, s_p = "", "", "", ""

        if is_tv and tid != "None":
            if _eff_tmdb:
                ep_n, ep_p, ep_s = fetch_tmdb_episode_meta(
                    tid,
                    s,
                    e_calc,
                    gui.tmdb_api_key.get(),
                    std_t,
                    gui.bgm_api_key.get(),
                )
                s_p = fetch_tmdb_season_poster(tid, s, gui.tmdb_api_key.get())
            else:
                ep_n, ep_p, ep_s, s_p = fetch_hybrid_episode_meta(
                    std_t,
                    tid,
                    s,
                    e_calc,
                    gui.bgm_api_key.get(),
                    gui.tmdb_api_key.get(),
                    y,
                )

        fallback_ep_title = g.get("episode_title") or ""
        ep_n_final = ep_n or fallback_ep_title

        s = safe_int(s, 1)
        e_calc = safe_int(e_calc, 1)
        s_fmt = f"{int(s):02d}"
        e_fmt = f"{int(e_calc):02d}"

        v_tag = gui._get_version_tag(item.path)

        safe_std_t = safe_filename(std_t)
        safe_ep_name = safe_filename(ep_n_final)

        if is_tv:
            new_fn = (
                gui.tv_format.get()
                .replace("{title}", safe_std_t)
                .replace("{year}", safe_str(y))
                .replace("{s:02d}", s_fmt)
                .replace("{s}", s_fmt)
                .replace("{e:02d}", e_fmt)
                .replace("{e}", e_fmt)
                .replace("{ep_name}", safe_ep_name)
                .replace("{ext}", v_tag + ext)
            )
        else:
            new_fn = (
                gui.movie_format.get()
                .replace("{title}", safe_std_t)
                .replace("{year}", safe_str(y))
                .replace("{ext}", v_tag + ext)
            )

        new_fn = re.sub(r"\s*\(\s*\)", "", new_fn)
        new_fn = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", new_fn)
        new_fn = re.sub(r"\s+(?=\.)", "", new_fn).strip()

        actors, directors = [], []
        if _eff_tmdb and tid and tid != "None":
            actors, directors = fetch_tmdb_credits(
                tid, is_tv=is_tv, api_key=gui.tmdb_api_key.get()
            )

        item.metadata = {
            "id": tid,
            "provider": "tmdb" if _eff_tmdb else "bgm",
            "title": safe_std_t,
            "year": y,
            "ep_title": ep_n_final or f"第 {e_calc} 集",
            "overview": meta.get("overview", ""),
            "ep_plot": ep_p,
            "s": s,
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
            "parse_source": parse_source,
        }
        item.parse_source = parse_source

        item.new_name_only = new_fn

        root_d = gui.target_root.get().strip()
        if root_d:
            id_tag = f"tmdbid={tid}" if _eff_tmdb else f"bgmid={tid}"
            folder_name = safe_filename(f"{safe_std_t} [{id_tag}]")
            season_folder = f"Season {s}"

            if is_tv:
                item.full_target = os.path.join(
                    root_d, folder_name, season_folder, new_fn
                )
            else:
                year_text = safe_str(y)
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
                    tid,
                    item.full_target or new_fn,
                    gui._build_status_text(ai_msg, db_m),
                ),
            ),
        )
    except Exception as ex:
        logging.error(f"处理文件 {item.old_name} 时出错: {ex}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"异常: {str(ex)[:50]}")
        gui.root.after(
            0,
            lambda id_val=item.id,
            old_name=item.old_name,
            msg=err_msg: gui.tree.item(
                id_val,
                values=(
                    old_name,
                    "错误",
                    "None",
                    gui._friendly_status_text(msg),
                    "崩溃",
                ),
            ),
        )
    finally:
        gui.root.after(0, lambda: gui.pbar.step(1))


def run_execution(gui, is_archive):
    """Run rename/archive execution with background worker pool."""
    return execution_run_execution(gui, is_archive)


def process_one_file(gui, item, is_archive):
    """Process single file move/rename and sidecar writing."""
    return execution_process_one_file(gui, item, is_archive)


def run_scrape_execution(gui):
    """Run scrape-only execution with background worker pool."""
    return execution_run_scrape_execution(gui)


def process_one_file_scrape(gui, item):
    """Process single file scrape-only (write NFO and download images)."""
    return execution_process_one_file_scrape(gui, item)

