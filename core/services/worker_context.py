"""WorkerContext — A GUI-free runtime context that task_runner / execution_runner
can consume directly. It reads configuration from renamer_config.json and exposes
the same attribute/method surface that the worker modules access on the ``gui`` parameter.

All UI-bound callbacks (tree updates, progress bar, messagebox) are replaced with
no-ops or optional callback hooks that the web layer can subscribe to.
"""

import json
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ai.ollama_ai import fetch_siliconflow_info, is_ai_rate_limited_error
from core.services.matcher_service import (
    get_embedding,
    get_online_embedding,
    pick_candidate_with_openai_compatible,
    parse_with_ollama,
    pick_candidate_with_ollama,
    rerank_candidates_with_embedding,
)
from core.services.naming_service import (
    build_status_text,
    can_reuse_dir_ai,
    extract_explicit_season,
    extract_lang_and_ext,
    extract_media_suffix,
    friendly_status_text,
    get_version_tag,
    pick_season,
    render_filename_template,
)
from db.tmdb_api import (
    fetch_bgm_by_id,
    fetch_bgm_candidates,
    fetch_hybrid_episode_meta,
    fetch_tmdb_by_id,
    fetch_tmdb_candidates,
    fetch_tmdb_credits,
    fetch_tmdb_episode_meta,
    fetch_tmdb_season_poster,
)
from utils.helpers import (
    CONFIG_FILE,
    DEFAULT_LANG_TAGS,
    DEFAULT_MOVIE_FORMAT,
    DEFAULT_SUB_AUDIO_EXTS,
    DEFAULT_TV_FORMAT,
    DEFAULT_VIDEO_EXTS,
    build_db_query_plan,
    build_query_titles,
    candidate_to_result,
    candidate_looks_like_extra_title,
    candidate_looks_like_unrequested_variant,
    derive_title_from_filename,
    extract_year_from_release,
    GROUP_RELEASE_BRACKET_RE,
    normalize_parse_source,
    normalize_compare_text,
    safe_filename,
    safe_int,
    text_mentions_extra_title,
    write_nfo,
    save_image,
    _nfo_has_empty_plot,
)


class _SimpleVar:
    """Minimal replacement for tkinter StringVar / BooleanVar."""

    def __init__(self, value: Any = ""):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value


class WorkerContext:
    """Runtime context for the worker modules.

    All tkinter-specific concepts (root.after, tree, pbar, etc.) are replaced with
    no-ops or callback hooks.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        config: Optional[dict] = None,
        on_status: Optional[Callable[[int, str, dict], None]] = None,
    ):
        """
        Parameters
        ----------
        config : dict, optional
            Overrides for renamer_config.json.  If *None*, the file is loaded from disk.
        on_status : callable(record_id, status_text, extra_dict), optional
            Hook invoked whenever a file's processing status changes.
        """
        if config is None:
            config = self._load_config_from_disk()
        self._cfg = config
        self._on_status = on_status

        # --- Locks & caches expected by the worker modules ---
        self.cache_lock = threading.Lock()
        self.file_write_lock = threading.Lock()
        self.popup_lock = threading.Lock()
        self.preview_skip_all_event = threading.Event()
        self.preview_skip_dirs: set = set()
        self.dir_cache: Dict[str, Any] = {}
        self.db_cache: Dict[str, Any] = {}
        self.manual_locks: Dict[str, Any] = {}
        self.forced_seasons: Dict[str, int] = {}
        self.forced_offsets: Dict[str, int] = {}
        self.db_resolution_events: Dict[str, threading.Event] = {}
        self.embedding_cache: Dict[str, Any] = {}
        self.ollama_embed_endpoint: Optional[str] = None

        # --- Config vars (expose via SimpleVar.get()) ---
        self.sf_api_key = _SimpleVar(config.get("sf_api_key", ""))
        self.sf_api_url = _SimpleVar(config.get("sf_api_url", "https://api.siliconflow.cn/v1"))
        self.sf_model = _SimpleVar(config.get("sf_model", "deepseek-ai/DeepSeek-V3"))
        self.ai_temperature = _SimpleVar(f"{self._clamp_temperature(config.get('ai_temperature'), 0.2):.2f}")
        self.ai_top_p = _SimpleVar(f"{self._clamp_top_p(config.get('ai_top_p'), 0.85):.2f}")
        self.bgm_api_key = _SimpleVar(config.get("bgm_api_key", ""))
        self.tmdb_api_key = _SimpleVar(config.get("tmdb_api_key", ""))
        self.tv_format = _SimpleVar(config.get("tv_format", DEFAULT_TV_FORMAT))
        self.movie_format = _SimpleVar(config.get("movie_format", DEFAULT_MOVIE_FORMAT))
        self.video_exts = _SimpleVar(config.get("video_exts", DEFAULT_VIDEO_EXTS))
        self.sub_audio_exts = _SimpleVar(config.get("sub_audio_exts", DEFAULT_SUB_AUDIO_EXTS))
        self.lang_tags = _SimpleVar(config.get("lang_tags", DEFAULT_LANG_TAGS))
        self.preserve_media_suffix = _SimpleVar(config.get("preserve_media_suffix", False))
        self.ollama_url = _SimpleVar(config.get("ollama_url", "http://localhost:11434"))
        self.ollama_model = _SimpleVar(config.get("ollama_model", ""))
        self.embedding_model = _SimpleVar(config.get("embedding_model", ""))
        self.embedding_source = _SimpleVar(config.get("embedding_source", "local"))
        self.online_embedding_model = _SimpleVar(config.get("online_embedding_model", ""))
        self.prefer_ollama = _SimpleVar(config.get("prefer_ollama", False))
        self.use_embedding_rank = _SimpleVar(config.get("use_embedding_rank", True))
        self.ai_mode = _SimpleVar(config.get("ai_mode", "assist"))  # disabled / assist / force
        self.preview_workers = _SimpleVar(str(self._clamp_workers(config.get("preview_workers"), 1)))
        self.sync_workers = _SimpleVar(str(self._clamp_workers(config.get("sync_workers"), 5)))
        self.execution_workers = _SimpleVar(str(self._clamp_workers(config.get("execution_workers"), 5)))
        self.media_type_override = _SimpleVar(config.get("media_type_override", "自动判断"))
        self.target_root = _SimpleVar(config.get("target_root", ""))
        self.source_var = _SimpleVar(config.get("data_source", "siliconflow_tmdb"))
        self.strip_keywords = config.get("strip_keywords", [])

        # Sync runtime cache expiry from config
        from utils.helpers import set_cache_expiry_days
        set_cache_expiry_days(config.get("cache_expiry_days", 7))

        # --- File list (populated externally) ---
        self.file_list: list = []

        # --- Dummy UI stubs (task_runner calls gui.root.after, gui.tree, etc.) ---
        self.root = _DummyRoot(self)
        self.tree = _DummyTree()
        self.pbar = _DummyProgressbar()
        self.status = _DummyLabel()
        self.btn_pre = _DummyButton()

    # ------------------------------------------------------------------
    # Config helpers used by the worker modules
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config_from_disk() -> dict:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as err:
                logging.error(f"WorkerContext: 加载配置失败: {err}")
        return {}

    def reload_config(self):
        cfg = self._load_config_from_disk()
        self._cfg = cfg
        for attr in ("sf_api_key", "sf_api_url", "sf_model", "bgm_api_key",
                      "tmdb_api_key", "ollama_url", "ollama_model",
                      "embedding_model", "embedding_source", "online_embedding_model",
                      "tv_format", "movie_format",
                      "video_exts", "sub_audio_exts", "lang_tags"):
            var = getattr(self, attr, None)
            if var is not None:
                var.set(cfg.get(attr, var.get()))
        self.preserve_media_suffix.set(cfg.get("preserve_media_suffix", False))
        self.prefer_ollama.set(cfg.get("prefer_ollama", False))
        self.use_embedding_rank.set(cfg.get("use_embedding_rank", True))
        self.ai_mode.set(cfg.get("ai_mode", "assist"))
        self.target_root.set(cfg.get("target_root", self.target_root.get()))
        self.source_var.set(cfg.get("data_source", self.source_var.get()))
        self.strip_keywords = cfg.get("strip_keywords", [])
        from utils.helpers import set_cache_expiry_days
        set_cache_expiry_days(cfg.get("cache_expiry_days", 7))

    @staticmethod
    def _clamp_workers(value, default):
        num = safe_int(value, default)
        return max(1, min(10, num))

    @staticmethod
    def _clamp_temperature(value, default=0.2):
        try:
            return max(0.0, min(2.0, float(value)))
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _clamp_top_p(value, default=0.9):
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            return float(default)

    def _get_ai_temperature(self):
        return self._clamp_temperature(self.ai_temperature.get(), 0.2)

    def _get_ai_top_p(self):
        return self._clamp_top_p(self.ai_top_p.get(), 0.85)

    def _get_preview_workers(self):
        return self._clamp_workers(self.preview_workers.get(), 1)

    def _get_sync_workers(self):
        return self._clamp_workers(self.sync_workers.get(), 5)

    def _get_execution_workers(self):
        return self._clamp_workers(self.execution_workers.get(), 5)

    # ------------------------------------------------------------------
    # Media helpers  (same signatures as app.py methods)
    # ------------------------------------------------------------------

    def get_media_exts(self):
        v = [e.strip().lower() for e in self.video_exts.get().split(",") if e.strip()]
        s = [e.strip().lower() for e in self.sub_audio_exts.get().split(",") if e.strip()]
        return tuple(v + s)

    def get_sub_audio_exts(self):
        return tuple(e.strip().lower() for e in self.sub_audio_exts.get().split(",") if e.strip())

    def extract_lang_and_ext(self, filename):
        return extract_lang_and_ext(filename, self.lang_tags.get())

    def _extract_explicit_season(self, pure_name):
        return extract_explicit_season(pure_name)

    def _pick_season(self, pure_name, guess_data=None, fallback=1):
        return pick_season(pure_name, guess_data, fallback)

    def _can_reuse_dir_ai(self, cached_ai, pure_name, guess_data=None):
        return can_reuse_dir_ai(cached_ai, pure_name, guess_data)

    def _get_version_tag(self, path):
        return get_version_tag(path)

    def _extract_media_suffix(self, filename, pure_name=None):
        return extract_media_suffix(filename, pure_name)

    def _render_media_filename(
        self,
        template,
        *,
        title="",
        year="",
        season="",
        episode="",
        ep_name="",
        ext="",
        source_filename="",
        pure_name="",
        parse_source="",
        source_provider="",
        media_id="",
        is_tv=True,
        original_title="",
        rating=0,
        genres=None,
        studios=None,
        overview="",
        ep_plot="",
        release="",
    ):
        preserve = bool(self.preserve_media_suffix.get())
        media_suffix = ""
        if preserve:
            media_suffix = safe_filename(
                self._extract_media_suffix(source_filename, pure_name)
            )
        context = {
            "title": title,
            "year": year,
            "season": season,
            "episode": episode,
            "ep_name": ep_name,
            "ext": ext,
            "media_suffix": media_suffix,
            "parse_source": parse_source,
            "source_provider": source_provider,
            "media_id": media_id,
            "is_tv": is_tv,
            "original_title": original_title,
            "rating": rating or 0,
            "genres": genres or [],
            "studios": studios or [],
            "overview": overview,
            "ep_plot": ep_plot,
            "release": release,
        }
        return render_filename_template(template, context, preserve), media_suffix

    def _friendly_status_text(self, message):
        return friendly_status_text(message)

    def _build_status_text(self, *messages):
        return build_status_text(*messages)

    def _resolve_media_type(self, guess_data=None):
        override = str(self.media_type_override.get() or "").strip()
        if override == "电影":
            return "movie"
        if override == "电视剧":
            return "episode"
        guessed_type = str((guess_data or {}).get("type") or "episode").strip().lower()
        if guessed_type in ("movie", "film"):
            return "movie"
        return "episode"

    # ------------------------------------------------------------------
    # AI helpers
    # ------------------------------------------------------------------

    def _parse_with_ollama(self, filename):
        return parse_with_ollama(
            self.ollama_url.get().strip(),
            self.ollama_model.get().strip(),
            filename,
            self._get_ai_temperature(),
            self._get_ai_top_p(),
        )

    def _can_use_ollama_for_pick(self):
        return bool(self.ollama_url.get().strip() and self.ollama_model.get().strip())

    def _can_use_online_model_for_pick(self):
        return bool(
            self.sf_api_url.get().strip()
            and self.sf_api_key.get().strip()
            and self.sf_model.get().strip()
        )

    def _can_use_embedding_rank(self):
        if not self.use_embedding_rank.get():
            return False
        if str(self.embedding_source.get() or "local").strip().lower() == "online":
            return bool(
                self.sf_api_url.get().strip()
                and self.sf_api_key.get().strip()
                and self.online_embedding_model.get().strip()
            )
        return bool(self.ollama_url.get().strip() and self.embedding_model.get().strip())

    def _get_embedding(self, text):
        if not self._can_use_embedding_rank():
            return None
        if str(self.embedding_source.get() or "local").strip().lower() == "online":
            return get_online_embedding(
                self.sf_api_url.get().strip(),
                self.sf_api_key.get().strip(),
                self.online_embedding_model.get().strip(),
                text,
                self.embedding_cache,
                self.cache_lock,
            )
        emb, endpoint = get_embedding(
            self.ollama_url.get().strip(),
            self.embedding_model.get().strip(),
            text,
            self.embedding_cache,
            self.cache_lock,
            self.ollama_embed_endpoint,
        )
        self.ollama_embed_endpoint = endpoint
        return emb

    # ------------------------------------------------------------------
    # DB matching (headless: auto-pick only, no manual popup)
    # ------------------------------------------------------------------

    def _resolve_db_match(self, item, query_title, year, is_tv, mode, ai_data, g):
        source_name = "TMDb" if mode == "siliconflow_tmdb" else "BGM"
        query_groups = build_db_query_plan(item, query_title, ai_data, g)
        merged, seen_ids = [], set()
        used_query, _first_hit = query_title, False

        def _search_queries(query_titles, fetch_func, limit=10):
            nonlocal used_query, _first_hit
            found = []
            for q in query_titles:
                cur = fetch_func(q)
                if not cur:
                    continue
                if not _first_hit:
                    used_query = q
                    _first_hit = True
                for cand in cur:
                    cid = str(cand.get("id") or "")
                    if not cid or cid in seen_ids:
                        continue
                    seen_ids.add(cid)
                    found.append(cand)
                if len(found) >= limit:
                    break
            return found

        for query_titles in query_groups:
            if mode == "siliconflow_tmdb":
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_tmdb_candidates(
                        q, year, is_tv, self.tmdb_api_key.get()
                    ),
                )
            else:
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_bgm_candidates(q, year, self.bgm_api_key.get()),
                )
            if current:
                merged.extend(current)
                break

        # TMDb 无结果时，以 BGM 作为回退数据源
        bgm_fallback = False
        if not merged and mode == "siliconflow_tmdb":
            for query_titles in query_groups:
                current = _search_queries(
                    query_titles,
                    lambda q: fetch_bgm_candidates(q, year, self.bgm_api_key.get()),
                )
                if current:
                    merged.extend(current)
                    break
            if merged:
                bgm_fallback = True
                source_name = "BGM(回退)"

        if merged:
            t_hit, tid_hit, msg_hit, meta_hit = self._select_best_db_match(
                item, used_query, year, is_tv, source_name, merged,
                recognized_title=query_title,
            )
            if tid_hit != "None" and normalize_compare_text(used_query) != normalize_compare_text(query_title):
                msg_hit += " (备选标题)"
            if bgm_fallback and tid_hit != "None":
                meta_hit["_provider"] = "bgm"
                # 封面/背景图仍从 TMDb 获取
                if self.tmdb_api_key.get().strip():
                    _tmdb_cands = fetch_tmdb_candidates(
                        t_hit or used_query, year, is_tv, self.tmdb_api_key.get()
                    )
                    if _tmdb_cands:
                        _tc_meta = _tmdb_cands[0].get("meta") or {}
                        if _tc_meta.get("poster"):
                            meta_hit["poster"] = _tc_meta["poster"]
                        if _tc_meta.get("fanart"):
                            meta_hit["fanart"] = _tc_meta["fanart"]
            return t_hit, tid_hit, msg_hit, meta_hit

        return query_title, "None", f"{source_name}无结果", {}

    def _select_best_db_match(self, item, query_title, year, is_tv, source_name,
                              candidates, recognized_title=None):
        if not candidates:
            return query_title, "None", f"{source_name}无结果", {}
        rank_pick_allowed = True
        raw_name = ""
        if isinstance(item, dict):
            raw_name = str(item.get("old_name") or "")
        else:
            raw_name = str(getattr(item, "old_name", "") or "")
        if GROUP_RELEASE_BRACKET_RE.match(raw_name):
            derived_query = derive_title_from_filename(raw_name)
            if (
                derived_query
                and normalize_compare_text(derived_query) != normalize_compare_text(query_title)
            ):
                rank_pick_allowed = False

        if source_name.startswith("TMDb") and not text_mentions_extra_title(
            f"{raw_name} {query_title}"
        ):
            regular_candidates = [
                c for c in candidates if not candidate_looks_like_extra_title(c)
            ]
            if regular_candidates:
                candidates = regular_candidates
            elif candidates:
                return query_title, "None", "TMDb候选疑似总集篇/特别篇，需手动确认", {}

        if source_name.startswith("TMDb"):
            source_text = f"{raw_name} {query_title}"
            regular_candidates = [
                c
                for c in candidates
                if not candidate_looks_like_unrequested_variant(c, source_text)
            ]
            if regular_candidates:
                candidates = regular_candidates
            elif candidates:
                return query_title, "None", "TMDb候选疑似外传/衍生剧，需手动确认", {}

        if len(candidates) == 1 and (
            not source_name.startswith("TMDb") or rank_pick_allowed
        ):
            return candidate_to_result(candidates[0], f"{source_name}命中")

        # Year pre-sort
        if year:
            year_str = str(year).strip()
            candidates = sorted(
                candidates,
                key=lambda c: 0 if extract_year_from_release(c.get("release") or "") == year_str else 1,
            )

        # Title exact / high-confidence
        import difflib as _difflib
        _q_norm = re.sub(r"[\W_]+", "", str(query_title or "").lower())
        if _q_norm:
            _exact = None
            _scores = []
            for _c in candidates:
                _ct = re.sub(r"[\W_]+", "", str(_c.get("title") or "").lower())
                _ca = re.sub(r"[\W_]+", "", str(_c.get("alt_title") or "").lower())
                _s = max(
                    _difflib.SequenceMatcher(None, _q_norm, _ct).ratio() if _ct else 0.0,
                    _difflib.SequenceMatcher(None, _q_norm, _ca).ratio() if _ca else 0.0,
                )
                _scores.append((_s, _c))
                if _ct == _q_norm or _ca == _q_norm:
                    _exact = _c
                    break
            if _exact is None and _scores:
                _scores.sort(key=lambda x: x[0], reverse=True)
                _top_s, _top_c = _scores[0]
                _second_s = _scores[1][0] if len(_scores) > 1 else 0.0
                if _top_s >= 0.90 and (_top_s - _second_s) >= 0.20:
                    _exact = _top_c
            if _exact is not None:
                return candidate_to_result(_exact, f"标题匹配/{source_name}命中")

        prefer_ollama = bool(self.prefer_ollama.get())
        online_ready = self._can_use_online_model_for_pick()
        ollama_ready = self._can_use_ollama_for_pick()

        # Embedding rerank. When a chat model is available, embedding only
        # changes candidate order; the chat model remains the final judge.
        ranked, emb_pick, emb_msg = self._rerank_candidates_with_embedding(
            item, query_title, year, is_tv, source_name, candidates
        )
        if emb_pick and not (online_ready or ollama_ready):
            hit_msg = f"Embedding判定/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            return candidate_to_result(emb_pick, hit_msg)

        def _candidate_result_from_model(label, chosen, reason):
            hit_msg = f"{label}/{source_name}命中"
            if emb_msg:
                hit_msg += f" ({emb_msg})"
            if reason:
                hit_msg += f" ({reason})"
            return candidate_to_result(chosen, hit_msg)

        if prefer_ollama and ollama_ready:
            chosen, reason = self._pick_candidate_with_ollama(
                item, query_title, year, is_tv, source_name, ranked
            )
            if chosen:
                return _candidate_result_from_model("Ollama判定", chosen, reason)
            online_ready = False

        if online_ready:
            chosen, reason = self._pick_candidate_with_online_model(
                item, query_title, year, is_tv, source_name, ranked
            )
            if chosen:
                return _candidate_result_from_model("在线模型判定", chosen, reason)

        if (not prefer_ollama) and (not online_ready) and ollama_ready:
            chosen, reason = self._pick_candidate_with_ollama(
                item, query_title, year, is_tv, source_name, ranked
            )
            if chosen:
                return _candidate_result_from_model("Ollama判定", chosen, reason)

        # TMDb can return the correct anime by romanized alias while the visible
        # zh-CN/original titles are Chinese/Japanese. Use this only as a final
        # fallback after embedding/model judgement has had a chance to decide.
        if (
            rank_pick_allowed
            and source_name.startswith("TMDb")
            and not online_ready
            and _q_norm
            and len(_q_norm) >= 6
        ):
            primary_hits = []
            for _c in ranked:
                _meta = _c.get("meta") or {}
                _sq_norm = re.sub(
                    r"[\W_]+",
                    "",
                    str(_meta.get("search_query") or "").lower(),
                )
                if _sq_norm == _q_norm:
                    primary_hits.append(_c)
            primary_hits.sort(
                key=lambda c: safe_int((c.get("meta") or {}).get("search_rank"), 999)
            )
            if primary_hits:
                _top = primary_hits[0]
                _top_meta = _top.get("meta") or {}
                _top_rank = safe_int(_top_meta.get("search_rank"), 999)
                _top_year = extract_year_from_release(_top.get("release") or "")
                _year_ok = not year or not _top_year or _top_year == str(year).strip()
                if _top_rank == 1 and _year_ok:
                    hit_msg = f"TMDb首位候选/{source_name}命中"
                    if emb_msg:
                        hit_msg += f" ({emb_msg})"
                    return candidate_to_result(_top, hit_msg)

        # --- Headless mode: no manual popup, return pending ---
        return query_title, "None", "待手动确认", {}

    def _rerank_candidates_with_embedding(self, item, query_title, year, is_tv,
                                          source_name, candidates):
        if not self._can_use_embedding_rank() or not candidates:
            return candidates, None, ""
        return rerank_candidates_with_embedding(
            item, query_title, year, is_tv, source_name, candidates, self._get_embedding,
        )

    def _pick_candidate_with_ollama(self, item, query_title, year, is_tv,
                                    source_name, candidates):
        return pick_candidate_with_ollama(
            self.ollama_url.get().strip(),
            self.ollama_model.get().strip(),
            item, query_title, year, is_tv, source_name, candidates,
            self._get_ai_temperature(),
        )

    def _pick_candidate_with_online_model(self, item, query_title, year, is_tv,
                                          source_name, candidates):
        return pick_candidate_with_openai_compatible(
            self.sf_api_url.get().strip(),
            self.sf_api_key.get().strip(),
            self.sf_model.get().strip(),
            item, query_title, year, is_tv, source_name, candidates,
            self._get_ai_temperature(),
        )

    def _request_manual_candidate_choice(self, item, query_title, source_name,
                                         candidates, recognized_title=None):
        # Headless: no popup, always decline
        return None

    def _show_candidate_picker_dialog(self, item, query_title, source_name,
                                      candidates, result_holder, done_event):
        result_holder["selected"] = None
        done_event.set()

    # ------------------------------------------------------------------
    # Sidecar writing (NFO + images)
    # ------------------------------------------------------------------

    def _write_sidecar_files(self, item, target_path):
        target_dir = os.path.dirname(target_path)
        m = item.metadata or {}
        media_type = m.get("type", "episode")
        is_tv = media_type == "episode"
        is_sub_audio = item.old_name.lower().endswith(self.get_sub_audio_exts())

        image_tasks = []

        with self.file_write_lock:
            if is_tv:
                if not is_sub_audio:
                    ep_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(ep_nfo):
                        write_nfo(ep_nfo, m, "episodedetails")
                    thumb_source = m.get("still") or m.get("s_poster") or m.get("poster")
                    if thumb_source:
                        thumb_path = os.path.splitext(target_path)[0] + "-thumb.jpg"
                        if not os.path.exists(thumb_path):
                            image_tasks.append((thumb_path, thumb_source))

                cur_dir = target_dir
                dir_name = os.path.basename(cur_dir)
                is_season_folder = bool(re.match(r"^(Season\s*\d+|S\d+)$", dir_name, re.I))
                root_d = os.path.dirname(cur_dir) if (is_season_folder and os.path.dirname(cur_dir)) else cur_dir

                s_num = m.get("s", 1)
                try:
                    s_fmt = f"{int(s_num):02d}"
                except Exception:
                    s_fmt = str(s_num)

                s_nfo_root = os.path.join(root_d, f"season{s_fmt}.nfo")
                s_poster_root = os.path.join(root_d, f"season{s_fmt}-poster.jpg")
                if not os.path.exists(s_nfo_root):
                    write_nfo(s_nfo_root, m, "season")
                if m.get("s_poster") and not os.path.exists(s_poster_root):
                    image_tasks.append((s_poster_root, m["s_poster"]))

                if is_season_folder:
                    season_nfo_local = os.path.join(cur_dir, "season.nfo")
                    folder_jpg_local = os.path.join(cur_dir, "folder.jpg")
                    if not os.path.exists(season_nfo_local):
                        write_nfo(season_nfo_local, m, "season")
                    if m.get("s_poster") and not os.path.exists(folder_jpg_local):
                        image_tasks.append((folder_jpg_local, m["s_poster"]))

                tvshow_nfo = os.path.join(root_d, "tvshow.nfo")
                poster_path = os.path.join(root_d, "poster.jpg")
                if not os.path.exists(tvshow_nfo) or _nfo_has_empty_plot(tvshow_nfo):
                    write_nfo(tvshow_nfo, m, "tvshow")
                if m.get("poster") and not os.path.exists(poster_path):
                    image_tasks.append((poster_path, m["poster"]))
            else:
                if not is_sub_audio:
                    movie_nfo = os.path.splitext(target_path)[0] + ".nfo"
                    if not os.path.exists(movie_nfo):
                        write_nfo(movie_nfo, m, "movie")
                poster_path = os.path.join(target_dir, "poster.jpg")
                if m.get("poster") and not os.path.exists(poster_path):
                    image_tasks.append((poster_path, m["poster"]))
                fanart_path = os.path.join(target_dir, "fanart.jpg")
                if m.get("fanart") and not os.path.exists(fanart_path):
                    image_tasks.append((fanart_path, m["fanart"]))

        for img_path, img_url in image_tasks:
            save_image(img_path, img_url)

    # ------------------------------------------------------------------
    # process_task / process_one_file delegates  (called by task_runner)
    # ------------------------------------------------------------------

    def process_task(self, i):
        from core.workers.task_runner import process_task
        process_task(self, i)

    def process_one_file(self, item, is_archive):
        from core.workers.execution_runner import process_one_file
        process_one_file(self, item, is_archive)

    def process_one_file_scrape(self, item):
        from core.workers.execution_runner import process_one_file_scrape
        process_one_file_scrape(self, item)


# ======================================================================
# Dummy UI stubs — absorb all Tkinter calls silently
# ======================================================================

class _DummyRoot:
    """Absorbs gui.root.after(0, fn) by just calling fn immediately in-thread."""

    def __init__(self, ctx: WorkerContext):
        self._ctx = ctx

    def after(self, delay_ms, fn=None, *args):
        if fn is not None:
            try:
                fn()
            except Exception:
                pass


class _DummyTree:
    """Absorbs gui.tree.set() and gui.tree.item() calls."""

    def set(self, *args, **kwargs):
        pass

    def item(self, *args, **kwargs):
        pass


class _DummyProgressbar:
    def step(self, *args):
        pass

    def config(self, **kwargs):
        pass

    def configure(self, **kwargs):
        pass


class _DummyLabel:
    def config(self, **kwargs):
        pass


class _DummyButton:
    def config(self, **kwargs):
        pass
