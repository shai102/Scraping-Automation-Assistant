"""Recognition test service helpers."""

import logging
import os
import re
from typing import Any, Optional

from guessit import guessit

from core.models.media_item import MediaItem
from core.recognition.preview_helpers import (
    derive_guessit_fields as _derive_guessit_fields,
    guessit_needs_assist as _guessit_needs_assist,
)
from core.services.worker_context import WorkerContext
from core.workers.task_runner import process_task as _process_task
from utils.cache import bypass_api_cache
from utils.title_parsing import build_db_query_plan, extract_episode_number
from utils.value_utils import normalize_compare_text, normalize_parse_source, safe_int, safe_str


logger = logging.getLogger(__name__)


def jsonable(value: Any):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    return str(value)


def clean_filename(value: str) -> str:
    name = str(value or "").strip().strip('"').strip("'")
    if not name:
        raise ValueError("请输入测试文件名")
    if "/" in name or "\\" in name:
        raise ValueError("识别测试仅支持文件名，不支持文件夹路径")
    if name in {".", ".."}:
        raise ValueError("文件名无效")
    return name


def has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def clean_expected(case: dict) -> dict:
    return {
        "title": str(case.get("expected_title") or "").strip(),
        "year": safe_str(case.get("expected_year")).strip(),
        "season": safe_int(case.get("expected_season"), 0)
        if has_value(case.get("expected_season"))
        else None,
        "episode": safe_int(case.get("expected_episode"), 0)
        if has_value(case.get("expected_episode"))
        else None,
        "provider": str(case.get("expected_provider") or "").strip().lower(),
        "id": str(case.get("expected_id") or "").strip(),
    }


def build_guessit_summary(ctx: WorkerContext, filename: str) -> tuple[dict, dict, str]:
    pure, ext = ctx.extract_lang_and_ext(filename)
    pure_for_parse = pure
    for kw in getattr(ctx, "strip_keywords", None) or []:
        if kw:
            pure_for_parse = re.sub(re.escape(kw), " ", pure_for_parse, flags=re.I)
    pure_for_parse = re.sub(r"\s+", " ", pure_for_parse).strip()

    guessed = guessit(pure_for_parse)
    extracted_ep = extract_episode_number(pure, guessed)
    title, year, season, episode = _derive_guessit_fields(
        ctx, pure, "", guessed, extracted_ep
    )
    needs_assist = _guessit_needs_assist(pure, "", guessed, title, extracted_ep)

    summary = {
        "pure_name": pure,
        "parsed_name": pure_for_parse,
        "ext": ext,
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
        "type": guessed.get("type"),
        "needs_ai_assist": needs_assist,
        "raw": jsonable(dict(guessed)),
    }
    return summary, guessed, pure


def poster_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return f"https://image.tmdb.org/t/p/w500{text}"


def ai_status(enabled: bool, parse_source: str, needs_assist: bool) -> str:
    if not enabled:
        return "未启用 AI，本次只使用 guessit + 资料库匹配"
    if parse_source in {"ai", "hybrid"}:
        return "AI 已参与本次识别"
    if needs_assist:
        return "已尝试 AI 辅助，但未得到可用结果，最终回退 guessit"
    return "guessit 判断足够稳定，未触发 AI 辅助"


def mode_config(base_cfg: dict, mode: str) -> dict:
    cfg = dict(base_cfg)
    if mode == "guessit":
        cfg["ai_mode"] = "disabled"
        cfg["use_embedding_rank"] = False
        cfg["ollama_model"] = ""
        cfg["embedding_model"] = ""
        cfg["sf_api_key"] = ""
        cfg["online_embedding_model"] = ""
    elif mode == "local_ai":
        cfg["ai_mode"] = "force"
        cfg["prefer_ollama"] = True
        cfg["embedding_source"] = "local"
        cfg["sf_api_key"] = ""
        cfg["online_embedding_model"] = ""
    elif mode == "online_ai":
        cfg["ai_mode"] = "force"
        cfg["prefer_ollama"] = False
        cfg["ollama_model"] = ""
        cfg["embedding_source"] = "local"
        cfg["online_embedding_model"] = ""
    else:
        current_mode = str(cfg.get("ai_mode") or "assist").strip().lower()
        cfg["ai_mode"] = current_mode if current_mode != "disabled" else "assist"
    return cfg


def set_media_type(ctx: WorkerContext, media_type: str):
    if media_type == "movie":
        ctx.media_type_override.set("电影")
    elif media_type == "tv":
        ctx.media_type_override.set("电视剧")
    else:
        ctx.media_type_override.set("自动判断")


def build_search_plan(filename: str, guessed: dict, query_title: str, ai_data=None) -> list[list[str]]:
    try:
        return build_db_query_plan(
            {"old_name": filename, "dir": ""},
            query_title,
            ai_data,
            guessed or {},
        )
    except Exception:
        return []


def compare_text(actual: Any, expected: Any) -> Optional[bool]:
    if not has_value(expected):
        return None
    return normalize_compare_text(actual) == normalize_compare_text(expected)


def compare_scalar(actual: Any, expected: Any) -> Optional[bool]:
    if not has_value(expected):
        return None
    return str(actual or "").strip().lower() == str(expected or "").strip().lower()


def compare_int(actual: Any, expected: Any) -> Optional[bool]:
    if expected is None:
        return None
    return safe_int(actual, -999999) == safe_int(expected, -888888)


def build_failure_reasons(result: dict, expected: Optional[dict] = None) -> list[str]:
    reasons = []
    if result.get("status") == "failed":
        reasons.append(result.get("message") or "识别执行失败")

    guessit_title = ((result.get("guessit") or {}).get("title") or "").strip()
    if not guessit_title:
        reasons.append("guessit 未能解析出有效标题")

    match = result.get("match") or {}
    if not match.get("id"):
        reasons.append("资料库未命中，或候选需要手动确认")

    if expected:
        title_ok = compare_text(match.get("title"), expected.get("title"))
        year_ok = compare_scalar(match.get("year"), expected.get("year"))
        season_ok = compare_int(match.get("season"), expected.get("season"))
        episode_ok = compare_int(match.get("episode"), expected.get("episode"))
        id_ok = compare_scalar(match.get("id"), expected.get("id"))
        provider_ok = compare_text(match.get("provider"), expected.get("provider"))
        if has_value(expected.get("title")) and not title_ok:
            reasons.append("标题与预期不一致")
        if has_value(expected.get("year")) and not year_ok:
            reasons.append("年份与预期不一致")
        if expected.get("season") is not None and not season_ok:
            reasons.append("季数与预期不一致")
        if expected.get("episode") is not None and not episode_ok:
            reasons.append("集数与预期不一致")
        if has_value(expected.get("id")) and not id_ok:
            reasons.append("资料库 ID 与预期不一致")
        if has_value(expected.get("provider")) and not provider_ok:
            reasons.append("资料库来源与预期不一致")

    if not reasons and result.get("ok"):
        reasons.append("识别链路完成，未发现明显异常")
    return reasons


def run_mode(
    filename: str,
    base_cfg: dict,
    source: str,
    media_type: str,
    mode: str,
    bypass_cache: bool = False,
) -> dict:
    clean_name = clean_filename(filename)
    cfg = mode_config(base_cfg, mode)
    ctx = WorkerContext(config=cfg)
    ctx.source_var.set(source)
    ctx.target_root.set("识别测试预览")
    set_media_type(ctx, media_type)

    guessit_summary, guessed, _pure = build_guessit_summary(ctx, clean_name)

    item = MediaItem(
        id="recognition-test",
        path=clean_name,
        dir="",
        old_name=clean_name,
        ext=os.path.splitext(clean_name)[1],
    )
    ctx.file_list = [item]

    with bypass_api_cache(bypass_cache):
        try:
            _process_task(ctx, 0)
        except Exception as err:
            logger.error("Recognition test failed for %s/%s: %s", mode, clean_name, err)
            item.metadata = {"id": "None", "error_msg": str(err)[:500]}

    metadata = item.metadata or {}
    tid = str(metadata.get("id") or "None")
    parse_source_raw = str(
        getattr(item, "parse_source", "") or metadata.get("parse_source") or ""
    )
    parse_source = normalize_parse_source(parse_source_raw)
    matched = tid != "None" and bool(item.new_name_only)
    status = "success" if matched else "pending_manual"
    if metadata.get("error_code"):
        status = "failed"

    message = "识别成功" if matched else metadata.get("error_msg") or "未匹配到资料库结果"
    search_title = metadata.get("query_title") or guessit_summary.get("title") or ""
    search_plan = build_search_plan(clean_name, guessed, search_title)

    result = {
        "ok": matched,
        "mode": mode,
        "status": status,
        "message": message,
        "input": clean_name,
        "source": source,
        "guessit": guessit_summary,
        "ai": {
            "enabled": mode != "guessit",
            "parse_source": parse_source or parse_source_raw,
            "parse_source_raw": parse_source_raw,
            "status": ai_status(
                mode != "guessit",
                parse_source_raw,
                bool(guessit_summary.get("needs_ai_assist")),
            ),
        },
        "match": {
            "id": tid if tid != "None" else "",
            "provider": metadata.get("provider") or "",
            "title": metadata.get("title") or "",
            "year": safe_str(metadata.get("year")),
            "type": metadata.get("type") or "",
            "season": metadata.get("s"),
            "episode": metadata.get("e"),
            "episode_title": metadata.get("ep_title") or "",
            "overview": metadata.get("overview") or "",
            "episode_plot": metadata.get("ep_plot") or "",
        },
        "preview": {
            "new_name": item.new_name_only or "",
            "target_path": item.full_target or "",
            "poster": poster_url(metadata.get("poster") or metadata.get("s_poster") or ""),
            "fanart": poster_url(metadata.get("fanart") or ""),
            "still": poster_url(metadata.get("still") or ""),
        },
        "diagnostics": {
            "mode": mode,
            "bypass_cache": bool(bypass_cache),
            "parsed_name": guessit_summary.get("parsed_name") or "",
            "search_plan": search_plan,
            "config": {
                "ai_mode": cfg.get("ai_mode"),
                "prefer_ollama": bool(cfg.get("prefer_ollama")),
                "embedding_source": cfg.get("embedding_source") or "local",
                "has_local_model": bool(str(cfg.get("ollama_model") or "").strip()),
                "has_local_embedding_model": bool(str(cfg.get("embedding_model") or "").strip()),
                "has_online_key": bool(str(cfg.get("sf_api_key") or "").strip()),
                "has_online_embedding_model": bool(
                    str(cfg.get("online_embedding_model") or "").strip()
                ),
            },
        },
        "metadata": jsonable(metadata),
    }
    result["reasons"] = build_failure_reasons(result)
    return result


def score_result(result: dict, expected: dict) -> dict:
    match = result.get("match") or {}
    metrics = {
        "title": compare_text(match.get("title"), expected.get("title")),
        "year": compare_scalar(match.get("year"), expected.get("year")),
        "season": compare_int(match.get("season"), expected.get("season")),
        "episode": compare_int(match.get("episode"), expected.get("episode")),
        "provider": compare_text(match.get("provider"), expected.get("provider")),
        "id": compare_scalar(match.get("id"), expected.get("id")),
    }
    evaluated = [value for value in metrics.values() if value is not None]
    full = bool(evaluated) and all(evaluated)
    wrong_match = (
        has_value(expected.get("id"))
        and has_value(match.get("id"))
        and metrics["id"] is False
    )
    return {
        "metrics": metrics,
        "full_match": full,
        "evaluated": bool(evaluated),
        "wrong_match": wrong_match,
        "pending_manual": result.get("status") == "pending_manual",
        "failed": result.get("status") == "failed",
    }


def empty_mode_stats(total: int) -> dict:
    metric_names = ("title", "year", "season", "episode", "provider", "id")
    return {
        "total": total,
        "metrics": {
            name: {"evaluated": 0, "correct": 0, "rate": None}
            for name in metric_names
        },
        "full": {"evaluated": 0, "correct": 0, "rate": None},
        "pending_manual": 0,
        "failed": 0,
        "wrong_match": 0,
    }


def finalize_mode_stats(stats: dict):
    for metric in stats["metrics"].values():
        if metric["evaluated"]:
            metric["rate"] = round(metric["correct"] * 100 / metric["evaluated"], 1)
    if stats["full"]["evaluated"]:
        stats["full"]["rate"] = round(
            stats["full"]["correct"] * 100 / stats["full"]["evaluated"], 1
        )


def build_batch_stats(rows: list[dict], modes: list[str]) -> dict:
    stats = {"total": len(rows), "modes": {mode: empty_mode_stats(len(rows)) for mode in modes}}
    for row in rows:
        for mode in modes:
            score = ((row.get("results") or {}).get(mode) or {}).get("score") or {}
            mode_stats = stats["modes"][mode]
            for name, value in (score.get("metrics") or {}).items():
                if value is None or name not in mode_stats["metrics"]:
                    continue
                mode_stats["metrics"][name]["evaluated"] += 1
                if value:
                    mode_stats["metrics"][name]["correct"] += 1
            if score.get("evaluated"):
                mode_stats["full"]["evaluated"] += 1
                if score.get("full_match"):
                    mode_stats["full"]["correct"] += 1
            if score.get("pending_manual"):
                mode_stats["pending_manual"] += 1
            if score.get("failed"):
                mode_stats["failed"] += 1
            if score.get("wrong_match"):
                mode_stats["wrong_match"] += 1
    for mode in modes:
        finalize_mode_stats(stats["modes"][mode])
    return stats


def run_single_test(body: dict) -> dict:
    filename = clean_filename(body.get("filename"))
    cfg_ctx = WorkerContext()
    cfg = dict(cfg_ctx._cfg)
    source = body.get("data_source") or cfg.get("data_source") or "siliconflow_tmdb"
    mode = "online_ai" if body.get("use_ai") else "guessit"
    return run_mode(
        filename,
        cfg,
        source,
        body.get("media_type") or "auto",
        mode,
        bool(body.get("bypass_cache")),
    )


def run_batch_test(body: dict) -> dict:
    cases = body.get("cases") or []
    if not cases:
        raise ValueError("请至少提供 1 条测试数据")
    if len(cases) > 100:
        raise ValueError("单次最多支持 100 条测试数据")

    cfg_ctx = WorkerContext()
    cfg = dict(cfg_ctx._cfg)
    source = body.get("data_source") or cfg.get("data_source") or "siliconflow_tmdb"
    modes = ["guessit", "local_ai", "online_ai"]
    rows = []

    for idx, case in enumerate(cases, 1):
        expected = clean_expected(case)
        row = {
            "index": idx,
            "filename": str(case.get("filename") or "").strip(),
            "expected": expected,
            "results": {},
        }
        for mode in modes:
            try:
                result = run_mode(
                    case.get("filename"),
                    cfg,
                    source,
                    case.get("media_type") or "auto",
                    mode,
                    bool(body.get("bypass_cache")),
                )
            except ValueError as err:
                result = {
                    "ok": False,
                    "mode": mode,
                    "status": "failed",
                    "message": str(err),
                    "input": case.get("filename"),
                    "source": source,
                    "guessit": {},
                    "ai": {"enabled": mode != "guessit", "parse_source": "", "status": ""},
                    "match": {},
                    "preview": {},
                    "diagnostics": {},
                    "metadata": {},
                }
            score = score_result(result, expected)
            result["score"] = score
            result["reasons"] = build_failure_reasons(result, expected)
            row["results"][mode] = result
        rows.append(row)

    return {
        "ok": True,
        "source": source,
        "bypass_cache": bool(body.get("bypass_cache")),
        "modes": modes,
        "rows": rows,
        "stats": build_batch_stats(rows, modes),
    }
