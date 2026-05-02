"""Settings API — read/write renamer_config.json + test connections."""

import json
import logging
import os
import time
import urllib.parse

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional

from utils.helpers import (
    CONFIG_FILE,
    DEFAULT_NO_PROXY,
    apply_proxy_environment,
    normalize_proxy_url,
    override_proxy_config,
    proxy_summary,
    request_get,
)
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
    preserve_media_suffix: Optional[bool] = None
    ollama_url: Optional[str] = None
    ollama_model: Optional[str] = None
    embedding_model: Optional[str] = None
    embedding_source: Optional[str] = None  # local / online
    online_embedding_model: Optional[str] = None
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
    proxy_enabled: Optional[bool] = None
    proxy_url: Optional[str] = None
    proxy_no_proxy: Optional[str] = None


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


def _extract_local_model_names(payload: dict) -> list[str]:
    """Support both Ollama /api/tags and OpenAI-compatible /v1/models."""
    if not isinstance(payload, dict):
        return []

    names = []
    for key, fields in (("models", ("name", "model", "id")), ("data", ("id", "name"))):
        items = payload.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = ""
            for field in fields:
                name = str(item.get(field) or "").strip()
                if name:
                    break
            if name and name not in names:
                names.append(name)
        if names:
            return names
    return names


def _list_local_ai_models(base_url: str) -> tuple[list[str], str]:
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return [], "本地 AI 地址未配置"

    endpoints = ["/api/tags"]
    endpoints.append("/models" if normalized.endswith("/v1") else "/v1/models")

    errors = []
    for endpoint in endpoints:
        try:
            resp = request_get(normalized + endpoint, timeout=8)
            if resp.status_code != 200:
                errors.append(f"{endpoint}: HTTP {resp.status_code}")
                continue
            try:
                models = _extract_local_model_names(resp.json())
            except ValueError:
                errors.append(f"{endpoint}: 返回内容不是 JSON")
                continue
            if models:
                return models, "已获取本地模型列表"
            errors.append(f"{endpoint}: 未发现模型")
        except Exception as e:
            errors.append(f"{endpoint}: {str(e)[:120]}")

    return [], "；".join(errors) or "读取本地模型失败"


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
    cfg.setdefault("proxy_enabled", False)
    cfg.setdefault("proxy_url", "")
    cfg.setdefault("proxy_no_proxy", DEFAULT_NO_PROXY)
    cfg.setdefault("preserve_media_suffix", False)
    return cfg


@router.put("")
def update_settings(body: SettingsModel):
    cfg = _load()
    updates = body.model_dump(exclude_none=True)
    if "proxy_url" in updates:
        updates["proxy_url"] = normalize_proxy_url(updates["proxy_url"])
    if "proxy_no_proxy" in updates and not str(updates.get("proxy_no_proxy") or "").strip():
        updates["proxy_no_proxy"] = DEFAULT_NO_PROXY
    cfg.update(updates)
    _save(cfg)

    # Apply cache expiry setting immediately
    if 'cache_expiry_days' in updates:
        from utils.helpers import set_cache_expiry_days
        set_cache_expiry_days(updates['cache_expiry_days'])
    if {"proxy_enabled", "proxy_url", "proxy_no_proxy"} & set(updates):
        apply_proxy_environment(cfg)

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
    try:
        resp = request_get(
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
        models, message = _list_local_ai_models(ollama_url)
        if models:
            return {"ok": True, "message": f"本地 AI 连接成功，{len(models)} 个模型可用", "models": models}
        return {"ok": False, "message": message[:300], "models": []}
    else:
        api_key = cfg.get("sf_api_key", "")
        api_url = cfg.get("sf_api_url", "https://api.siliconflow.cn/v1")
        model_name = cfg.get("sf_model", "deepseek-ai/DeepSeek-V3")
        if not api_key:
            raise HTTPException(400, detail="AI API Key 未配置")

        success, message = test_silicon_api(api_url, api_key, model_name)
        return {"ok": success, "message": message}


@router.get("/ollama-models")
def list_ollama_models(ollama_url: Optional[str] = Query(default=None)):
    cfg = _load()
    effective_url = ollama_url if ollama_url is not None else cfg.get("ollama_url", "http://localhost:11434")
    models, message = _list_local_ai_models(effective_url)
    return {"models": models, "message": message}


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


def _service_url_label(url: str) -> str:
    host = urllib.parse.urlparse(url).netloc or url
    return host.replace(":443", "").replace(":80", "")


def _build_proxy_test_targets(cfg: dict) -> list[dict]:
    targets = [
        {"name": "api.themoviedb.org", "url": "https://api.themoviedb.org/3/configuration"},
        {"name": "www.themoviedb.org", "url": "https://www.themoviedb.org/"},
        {
            "name": "image.tmdb.org",
            "url": "https://image.tmdb.org/t/p/w92/wwemzKWzjKYJFfCeiB57q3r4Bcm.png",
        },
        {"name": "api.bgm.tv", "url": "https://api.bgm.tv/v0/subjects/1"},
        {"name": "api.telegram.org", "url": "https://api.telegram.org/"},
        {"name": "t.me", "url": "https://t.me/"},
        {"name": "github.com", "url": "https://github.com/"},
    ]
    api_url = str(cfg.get("sf_api_url") or "").strip().rstrip("/")
    if api_url:
        model_url = api_url if api_url.endswith("/models") else api_url + "/models"
        targets.insert(0, {"name": _service_url_label(api_url), "url": model_url, "auth": "ai"})
    return targets


@router.post("/test-proxy")
def test_proxy(body: Optional[SettingsModel] = None):
    cfg = _load()
    if body is not None:
        updates = body.model_dump(exclude_none=True)
        if "proxy_url" in updates:
            updates["proxy_url"] = normalize_proxy_url(updates["proxy_url"])
        if "proxy_no_proxy" in updates and not str(updates.get("proxy_no_proxy") or "").strip():
            updates["proxy_no_proxy"] = DEFAULT_NO_PROXY
        cfg.update(updates)
    cfg.setdefault("proxy_enabled", False)
    cfg.setdefault("proxy_url", "")
    cfg.setdefault("proxy_no_proxy", DEFAULT_NO_PROXY)

    results = []
    ok_count = 0
    latencies = []
    with override_proxy_config(cfg):
        summary = proxy_summary()
        for target in _build_proxy_test_targets(cfg):
            headers = {"User-Agent": "MyMediaRenamer/ProxyTest"}
            if target.get("auth") == "ai" and str(cfg.get("sf_api_key") or "").strip():
                headers["Authorization"] = f"Bearer {str(cfg.get('sf_api_key')).strip()}"
            started = time.perf_counter()
            try:
                resp = request_get(target["url"], headers=headers, timeout=(5, 12))
                latency = int((time.perf_counter() - started) * 1000)
                status = int(resp.status_code)
                connected = status < 500
                ok_count += 1 if connected else 0
                latencies.append(latency)
                results.append(
                    {
                        "name": target["name"],
                        "url": target["url"],
                        "ok": connected,
                        "status": f"HTTP {status}",
                        "latency_ms": latency,
                        "message": "连通" if connected else "服务端错误",
                    }
                )
            except Exception as err:
                latency = int((time.perf_counter() - started) * 1000)
                results.append(
                    {
                        "name": target["name"],
                        "url": target["url"],
                        "ok": False,
                        "status": "FAILED",
                        "latency_ms": latency,
                        "message": str(err)[:180],
                    }
                )

    avg_latency = round(sum(latencies) / len(latencies)) if latencies else None
    return {
        "ok": ok_count > 0,
        "summary": {
            "total": len(results),
            "success": ok_count,
            "failed": len(results) - ok_count,
            "avg_latency_ms": avg_latency,
        },
        "proxy": summary,
        "results": results,
    }


@router.post("/clear-cache")
def clear_cache():
    """Wipe the API response cache (api_cache.json)."""
    from utils.helpers import clear_api_cache_file, CACHE_FILE
    clear_api_cache_file()
    return {"ok": True, "message": "缓存已清除，下次识别将重新向 API 请求"}
