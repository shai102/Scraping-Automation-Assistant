"""Settings API — read/write renamer_config.json + test connections."""

import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional

from core.settings.config_service import (
    get_settings_for_display,
    get_metadata_hub_root,
    get_settings_raw_defaults,
    load_settings,
    merge_settings_update,
    persist_settings_updates,
    reload_watcher_runtime_config,
)
from core.settings.connection_service import (
    list_local_ai_models,
    run_proxy_test,
    test_ai_connection,
    test_emby_server,
    test_telegram_connection,
    test_tmdb_connection,
)
from core.metadata.local_hub_service import MetadataHubError, inspect_metadata_hub
from core.settings.preview_service import build_filename_preview_payload
from utils.cache import clear_api_cache_file

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
    symlink_export_workers: Optional[int] = None
    sync_workers: Optional[int] = None
    execution_workers: Optional[int] = None
    media_type_override: Optional[str] = None
    tg_bot_token: Optional[str] = None
    tg_chat_id: Optional[str] = None
    tg_notify_enabled: Optional[bool] = None
    tg_notify_delay: Optional[int] = None
    emby_url: Optional[str] = None
    emby_api_key: Optional[str] = None
    emby_notify_enabled: Optional[bool] = None
    emby_notify_delay: Optional[int] = None
    strip_keywords: Optional[List[str]] = None
    cache_expiry_days: Optional[int] = None
    proxy_enabled: Optional[bool] = None
    proxy_url: Optional[str] = None
    proxy_no_proxy: Optional[str] = None
    metadata_refresh_enabled: Optional[bool] = None
    metadata_refresh_interval_hours: Optional[int] = None
    metadata_refresh_lookback_days: Optional[int] = None
    metadata_refresh_ignore_episode_title_rules: Optional[str] = None
    metadata_refresh_skip_rules: Optional[str] = None
    metadata_hub_root: Optional[str] = None


class FilenamePreviewModel(BaseModel):
    template: str
    is_tv: bool = True
    preserve_media_suffix: bool = False


@router.get("")
def get_settings():
    return get_settings_for_display()


@router.get("/raw")
def get_settings_raw():
    """Full settings including unmasked keys (for form pre-fill)."""
    return get_settings_raw_defaults()


@router.put("")
def update_settings(body: SettingsModel):
    updates = body.model_dump(exclude_none=True)
    persist_settings_updates(updates)
    reload_watcher_runtime_config()
    return {"ok": True}


@router.post("/test-tmdb")
def test_tmdb():
    cfg = load_settings()
    api_key = cfg.get("tmdb_api_key", "")
    if not api_key:
        raise HTTPException(400, detail="TMDB API Key 未配置")
    ok, message = test_tmdb_connection(api_key)
    return {"ok": ok, "message": message}


@router.post("/test-metadata-hub")
def test_metadata_hub():
    try:
        result = inspect_metadata_hub(get_metadata_hub_root())
    except MetadataHubError as err:
        raise HTTPException(400, detail=str(err)) from err
    return {
        "ok": True,
        "message": (
            f"Metadata Hub 目录可用，识别到 "
            f"{result['indexed_titles']}/{result['title_dirs']} 个带 TMDB ID 的作品"
        ),
        **result,
    }


@router.post("/test-ai")
def test_ai():
    cfg = load_settings()
    if not cfg.get("prefer_ollama", False) and not cfg.get("sf_api_key", ""):
        raise HTTPException(400, detail="AI API Key 未配置")
    ok, message, models = test_ai_connection(cfg)
    if models:
        return {"ok": ok, "message": f"本地 AI 连接成功，{len(models)} 个模型可用", "models": models}
    return {"ok": ok, "message": message, "models": models}


@router.get("/ollama-models")
def list_ollama_models(ollama_url: Optional[str] = Query(default=None)):
    cfg = load_settings()
    effective_url = ollama_url if ollama_url is not None else cfg.get("ollama_url", "http://localhost:11434")
    models, message = list_local_ai_models(effective_url)
    return {"models": models, "message": message}


@router.post("/test-telegram")
def test_telegram():
    cfg = load_settings()
    token = (cfg.get("tg_bot_token") or "").strip()
    chat_id = (cfg.get("tg_chat_id") or "").strip()
    if not token:
        raise HTTPException(400, detail="Telegram Bot Token 未配置")
    if not chat_id:
        raise HTTPException(400, detail="Telegram Chat ID 未配置")
    try:
        ok, message = test_telegram_connection(token, chat_id)
        return {"ok": ok, "message": message}
    except Exception as e:
        return {"ok": False, "message": str(e)[:200]}


@router.post("/test-proxy")
def test_proxy(body: Optional[SettingsModel] = None):
    cfg = load_settings()
    if body is not None:
        cfg = merge_settings_update(cfg, body.model_dump(exclude_none=True))
    return run_proxy_test(cfg)


@router.post("/test-emby")
def test_emby():
    cfg = load_settings()
    url = (cfg.get("emby_url") or "").strip()
    api_key = (cfg.get("emby_api_key") or "").strip()
    if not url:
        raise HTTPException(400, detail="Emby/Jellyfin 地址未配置")
    if not api_key:
        raise HTTPException(400, detail="Emby/Jellyfin API Key 未配置")
    ok, message = test_emby_server(url, api_key)
    return {"ok": ok, "message": message}


@router.post("/clear-cache")
def clear_cache():
    """Wipe the API response cache (api_cache.json)."""
    clear_api_cache_file()
    return {"ok": True, "message": "缓存已清除，下次识别将重新向 API 请求"}


@router.post("/preview-filename")
def preview_filename(body: FilenamePreviewModel):
    template = str(body.template or "").strip()
    if not template:
        raise HTTPException(400, detail="模板不能为空")

    try:
        payload = build_filename_preview_payload(
            template=template,
            is_tv=bool(body.is_tv),
            preserve_media_suffix=bool(body.preserve_media_suffix),
        )
    except Exception as err:
        raise HTTPException(400, detail=f"模板预览失败: {err}") from err

    return {
        "ok": True,
        "template": template,
        "is_tv": bool(body.is_tv),
        "preserve_media_suffix": bool(body.preserve_media_suffix),
        **payload,
    }
