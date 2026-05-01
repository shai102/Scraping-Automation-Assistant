"""Recognition test API — run the current parser chain without touching files."""

import os
import re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from guessit import guessit
from pydantic import BaseModel

from core.models.media_item import MediaItem
from core.services.worker_context import WorkerContext
from core.workers.task_runner import (
    _derive_guessit_fields,
    _guessit_needs_assist,
    process_task as _process_task,
)
from utils.helpers import extract_episode_number, normalize_parse_source, safe_str

router = APIRouter(prefix="/api/recognition-test", tags=["recognition-test"])


class RecognitionTestBody(BaseModel):
    filename: str
    use_ai: bool = False
    media_type: str = "auto"  # auto / movie / tv
    data_source: Optional[str] = None


def _jsonable(value: Any):
    """Convert guessit/babelfish values into JSON-safe primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _clean_filename(value: str) -> str:
    name = str(value or "").strip().strip('"').strip("'")
    if not name:
        raise HTTPException(400, detail="请输入测试文件名")
    if "/" in name or "\\" in name:
        raise HTTPException(400, detail="识别测试仅支持文件名，不支持文件夹路径")
    if name in {".", ".."}:
        raise HTTPException(400, detail="文件名无效")
    return name


def _build_guessit_summary(ctx: WorkerContext, filename: str) -> tuple[dict, dict, str]:
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
        "raw": _jsonable(dict(guessed)),
    }
    return summary, guessed, pure


def _poster_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return f"https://image.tmdb.org/t/p/w500{text}"


def _ai_status(enabled: bool, parse_source: str, needs_assist: bool) -> str:
    if not enabled:
        return "未启用 AI，本次只使用 guessit + 资料库匹配"
    if parse_source in {"ai", "hybrid"}:
        return "AI 已参与本次识别"
    if needs_assist:
        return "已尝试 AI 辅助，但未得到可用结果，最终回退 guessit"
    return "guessit 判断足够稳定，未触发 AI 辅助"


@router.post("", response_model=dict)
def run_recognition_test(body: RecognitionTestBody):
    filename = _clean_filename(body.filename)

    cfg_ctx = WorkerContext()
    cfg = dict(cfg_ctx._cfg)
    cfg["ai_mode"] = "assist" if body.use_ai else "disabled"

    ctx = WorkerContext(config=cfg)
    source = body.data_source or cfg.get("data_source") or "siliconflow_tmdb"
    ctx.source_var.set(source)
    ctx.target_root.set("识别测试预览")
    if body.media_type == "movie":
        ctx.media_type_override.set("电影")
    elif body.media_type == "tv":
        ctx.media_type_override.set("电视剧")
    else:
        ctx.media_type_override.set("自动判断")

    guessit_summary, _guessed, _pure = _build_guessit_summary(ctx, filename)

    item = MediaItem(
        id="recognition-test",
        path=filename,
        dir="",
        old_name=filename,
        ext=os.path.splitext(filename)[1],
    )
    ctx.file_list = [item]

    _process_task(ctx, 0)

    metadata = item.metadata or {}
    tid = str(metadata.get("id") or "None")
    parse_source_raw = str(getattr(item, "parse_source", "") or metadata.get("parse_source") or "")
    parse_source = normalize_parse_source(parse_source_raw)
    matched = tid != "None" and bool(item.new_name_only)
    status = "success" if matched else "pending_manual"
    if metadata.get("error_code"):
        status = "failed"

    message = "识别成功" if matched else metadata.get("error_msg") or "未匹配到资料库结果"

    return {
        "ok": matched,
        "status": status,
        "message": message,
        "input": filename,
        "source": source,
        "guessit": guessit_summary,
        "ai": {
            "enabled": body.use_ai,
            "parse_source": parse_source or parse_source_raw,
            "parse_source_raw": parse_source_raw,
            "status": _ai_status(
                body.use_ai,
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
            "poster": _poster_url(metadata.get("poster") or metadata.get("s_poster") or ""),
            "fanart": _poster_url(metadata.get("fanart") or ""),
            "still": _poster_url(metadata.get("still") or ""),
        },
        "metadata": _jsonable(metadata),
    }
