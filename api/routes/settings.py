"""Settings API — read/write renamer_config.json + test connections."""

import json
import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from utils.helpers import CONFIG_FILE
from ai.ollama_ai import test_silicon_api

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/settings", tags=["settings"])


class SettingsModel(BaseModel):
    sf_api_key: Optional[str] = None
    sf_api_url: Optional[str] = None
    sf_model: Optional[str] = None
    ai_temperature: Optional[float] = None
    ai_top_p: Optional[float] = None
    bgm_api_key: Optional[str] = None
    tmdb_api_key: Optional[str] = None
    tv_format: Optional[str] = None
    movie_format: Optional[str] = None
    video_exts: Optional[str] = None
    sub_audio_exts: Optional[str] = None
    lang_tags: Optional[str] = None
    ollama_url: Optional[str] = None
    ollama_model: Optional[str] = None
    embedding_model: Optional[str] = None
    prefer_ollama: Optional[bool] = None
    use_embedding_rank: Optional[bool] = None
    ai_mode: Optional[str] = None  # disabled / assist / force
    preview_workers: Optional[int] = None
    sync_workers: Optional[int] = None
    execution_workers: Optional[int] = None
    media_type_override: Optional[str] = None
    tg_bot_token: Optional[str] = None
    tg_chat_id: Optional[str] = None
    tg_notify_enabled: Optional[bool] = None
    tg_notify_delay: Optional[int] = None
    strip_keywords: Optional[List[str]] = None
    cache_expiry_days: Optional[int] = None


def _load() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save(data: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


@router.get("")
def get_settings():
    cfg = _load()
    # Mask API keys for display (show last 4 chars)
    safe = dict(cfg)
    for key in ("sf_api_key", "bgm_api_key", "tmdb_api_key", "tg_bot_token"):
        val = safe.get(key, "")
        if val and len(val) > 4:
            safe[key] = "*" * (len(val) - 4) + val[-4:]
    return safe


@router.get("/raw")
def get_settings_raw():
    """Full settings including unmasked keys (for form pre-fill)."""
    cfg = _load()
    cfg.setdefault("ai_temperature", 0.20)
    cfg.setdefault("ai_top_p", 0.85)
    return cfg


@router.put("")
def update_settings(body: SettingsModel):
    cfg = _load()
    updates = body.model_dump(exclude_none=True)
    cfg.update(updates)
    _save(cfg)

    # Apply cache expiry setting immediately
    if 'cache_expiry_days' in updates:
        from utils.helpers import set_cache_expiry_days
        set_cache_expiry_days(updates['cache_expiry_days'])

    # Reload worker context if watcher is running
    from server import get_watcher
    w = get_watcher()
    if w and w._worker_ctx:
        w.reload_runtime_config()
        # 清空目录缓存，确保新配置（AI key/模型等）立即生效
        w._worker_ctx.dir_cache.clear()

    return {"ok": True}


@router.post("/test-tmdb")
def test_tmdb():
    cfg = _load()
    api_key = cfg.get("tmdb_api_key", "")
    if not api_key:
        raise HTTPException(400, detail="TMDB API Key 未配置")
    import requests as req
    try:
        resp = req.get(
            "https://api.themoviedb.org/3/configuration",
            params={"api_key": api_key},
            timeout=10,
        )
        if resp.status_code == 200:
            return {"ok": True, "message": "TMDB 连接成功"}
        return {"ok": False, "message": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "message": str(e)[:200]}


@router.post("/test-ai")
def test_ai():
    cfg = _load()
    prefer_ollama = cfg.get("prefer_ollama", False)

    if prefer_ollama:
        ollama_url = cfg.get("ollama_url", "http://localhost:11434")
        import requests as req
        try:
            resp = req.get(f"{ollama_url}/api/tags", timeout=8)
            if resp.status_code == 200:
                models = [m.get("name", "") for m in resp.json().get("models", [])]
                return {"ok": True, "message": f"Ollama 连接成功，{len(models)} 个模型可用", "models": models}
            return {"ok": False, "message": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"ok": False, "message": str(e)[:200]}
    else:
        api_key = cfg.get("sf_api_key", "")
        api_url = cfg.get("sf_api_url", "https://api.siliconflow.cn/v1")
        model_name = cfg.get("sf_model", "deepseek-ai/DeepSeek-V3")
        if not api_key:
            raise HTTPException(400, detail="AI API Key 未配置")

        success, message = test_silicon_api(api_url, api_key, model_name)
        return {"ok": success, "message": message}


@router.get("/ollama-models")
def list_ollama_models():
    cfg = _load()
    ollama_url = cfg.get("ollama_url", "http://localhost:11434")
    import requests as req
    try:
        resp = req.get(f"{ollama_url}/api/tags", timeout=8)
        if resp.status_code == 200:
            models = [m.get("name", "") for m in resp.json().get("models", [])]
            return {"models": models}
        return {"models": []}
    except Exception:
        return {"models": []}


@router.post("/test-telegram")
def test_telegram():
    cfg = _load()
    token = (cfg.get("tg_bot_token") or "").strip()
    chat_id = (cfg.get("tg_chat_id") or "").strip()
    if not token:
        raise HTTPException(400, detail="Telegram Bot Token 未配置")
    if not chat_id:
        raise HTTPException(400, detail="Telegram Chat ID 未配置")
    try:
        from utils.telegram_notify import send_test_message
        result = send_test_message(token, chat_id)
        if result.get("ok"):
            return {"ok": True, "message": "Telegram 测试消息发送成功"}
        return {"ok": False, "message": result.get("description", "发送失败")}
    except Exception as e:
        return {"ok": False, "message": str(e)[:200]}


@router.post("/clear-cache")
def clear_cache():
    """Wipe the API response cache (api_cache.json)."""
    from utils.helpers import clear_api_cache_file, CACHE_FILE
    clear_api_cache_file()
    return {"ok": True, "message": "缓存已清除，下次识别将重新向 API 请求"}
