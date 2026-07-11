import json
import os
import shutil
import tempfile
from datetime import datetime

from utils.app_runtime import CONFIG_FILE
from utils.cache import set_cache_expiry_days
from utils.proxy import DEFAULT_NO_PROXY, apply_proxy_environment, normalize_proxy_url

CONFIG_VERSION = 2
RUNTIME_DEFAULTS = {
    "config_version": CONFIG_VERSION,
    "file_stability_enabled": True,
    "file_stability_checks": 2,
    "file_stability_interval_seconds": 1.0,
    "retry_base_seconds": 30,
    "retry_max_seconds": 1800,
    "retry_max_attempts": 5,
    "task_retention_days": 30,
    "log_retention_days": 30,
    "recognition_confidence_gate_enabled": False,
    "recognition_confidence_threshold": 0.60,
}


def migrate_settings(cfg: dict) -> tuple[dict, bool]:
    migrated = dict(cfg or {})
    try:
        old_version = int(migrated.get("config_version") or 0)
    except (TypeError, ValueError):
        old_version = 0
    for key, value in RUNTIME_DEFAULTS.items():
        migrated.setdefault(key, value)
    migrated["config_version"] = CONFIG_VERSION
    return migrated, old_version < CONFIG_VERSION


def load_settings() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as handle:
                cfg, changed = migrate_settings(json.load(handle))
                if changed:
                    save_settings(cfg, backup_existing=True)
                return cfg
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


def save_settings(data: dict, *, backup_existing: bool = False):
    os.makedirs(os.path.dirname(CONFIG_FILE) or ".", exist_ok=True)
    if backup_existing and os.path.isfile(CONFIG_FILE):
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(CONFIG_FILE, f"{CONFIG_FILE}.bak-{stamp}")
    fd, temp_path = tempfile.mkstemp(prefix="renamer_config.", suffix=".tmp", dir=os.path.dirname(CONFIG_FILE) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=4, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, CONFIG_FILE)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


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
    for key, value in RUNTIME_DEFAULTS.items():
        cfg.setdefault(key, value)
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
