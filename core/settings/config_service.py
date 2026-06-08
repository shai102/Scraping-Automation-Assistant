import json
import os

from utils.app_runtime import CONFIG_FILE
from utils.cache import set_cache_expiry_days
from utils.proxy import DEFAULT_NO_PROXY, apply_proxy_environment, normalize_proxy_url


def load_settings() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            pass
    return {}


def get_metadata_hub_root(cfg: dict | None = None) -> str:
    settings = cfg if cfg is not None else load_settings()
    return str(
        settings.get("metadata_hub_root")
        or os.environ.get("METADATA_HUB_ROOT")
        or "/media/metadata hub"
    ).strip()


def save_settings(data: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=4, ensure_ascii=False)


def get_settings_for_display() -> dict:
    cfg = load_settings()
    safe = dict(cfg)
    for key in ("sf_api_key", "bgm_api_key", "tmdb_api_key", "tg_bot_token"):
        val = safe.get(key, "")
        if val and len(val) > 4:
            safe[key] = "*" * (len(val) - 4) + val[-4:]
    return safe


def get_settings_raw_defaults() -> dict:
    cfg = load_settings()
    cfg.setdefault("ai_temperature", 0.20)
    cfg.setdefault("ai_top_p", 0.85)
    cfg.setdefault("proxy_enabled", False)
    cfg.setdefault("proxy_url", "")
    cfg.setdefault("proxy_no_proxy", DEFAULT_NO_PROXY)
    cfg.setdefault("preserve_media_suffix", False)
    cfg.setdefault("symlink_export_workers", 3)
    cfg.setdefault("metadata_refresh_enabled", True)
    cfg.setdefault("metadata_refresh_interval_hours", 12)
    cfg.setdefault("metadata_refresh_lookback_days", 14)
    cfg.setdefault("metadata_refresh_ignore_episode_title_rules", "")
    cfg.setdefault("metadata_refresh_skip_rules", "")
    cfg.setdefault("metadata_hub_root", get_metadata_hub_root(cfg))
    return cfg


def merge_settings_update(cfg: dict, updates: dict) -> dict:
    merged = dict(cfg or {})
    normalized = dict(updates or {})
    if "proxy_url" in normalized:
        normalized["proxy_url"] = normalize_proxy_url(normalized["proxy_url"])
    if "proxy_no_proxy" in normalized and not str(normalized.get("proxy_no_proxy") or "").strip():
        normalized["proxy_no_proxy"] = DEFAULT_NO_PROXY
    merged.update(normalized)
    return merged


def persist_settings_updates(updates: dict) -> dict:
    cfg = merge_settings_update(load_settings(), updates)
    save_settings(cfg)

    if "cache_expiry_days" in updates:
        set_cache_expiry_days(updates["cache_expiry_days"])
    if {"proxy_enabled", "proxy_url", "proxy_no_proxy"} & set(updates):
        apply_proxy_environment(cfg)
    return cfg


def reload_watcher_runtime_config():
    from server import get_watcher

    watcher = get_watcher()
    if watcher and watcher._worker_ctx:
        watcher.reload_runtime_config()
        watcher._worker_ctx.dir_cache.clear()
