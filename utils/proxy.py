import json
import os
import re
import sys
import urllib.parse
import urllib.request
from contextlib import contextmanager
from contextvars import ContextVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_NO_PROXY = (
    "localhost,127.0.0.1,::1,0.0.0.0,host.docker.internal,*.local,"
    "10.*,192.168.*,172.16.*,172.17.*,172.18.*,172.19.*,172.20.*,172.21.*,172.22.*,172.23.*,172.24.*,172.25.*,172.26.*,172.27.*,172.28.*,172.29.*,172.30.*,172.31.*"
)

_proxy_config_override = ContextVar("proxy_config_override", default=None)


def _resolve_config_file() -> str:
    if getattr(sys, "frozen", False):
        cfg_dir = os.path.dirname(sys.executable)
    else:
        cfg_dir = os.environ.get("DATA_DIR") or os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    return os.path.join(cfg_dir, "renamer_config.json")


def normalize_proxy_url(value):
    """Normalize single proxy address, accepting host:port shorthand."""
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" not in text:
        text = "http://" + text
    return text.rstrip("/")


def _load_proxy_config():
    override = _proxy_config_override.get()
    if isinstance(override, dict):
        return dict(override)
    try:
        with open(_resolve_config_file(), "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _split_no_proxy(value):
    text = str(value or DEFAULT_NO_PROXY)
    return [part.strip() for part in re.split(r"[,;\s]+", text) if part.strip()]


def _strip_no_proxy_host(pattern):
    value = str(pattern or "").strip().lower()
    if "://" in value:
        value = urllib.parse.urlparse(value).hostname or value
    if value.startswith("[") and "]" in value:
        return value.strip("[]")
    if ":" in value and value.count(":") == 1:
        value = value.split(":", 1)[0]
    return value


def proxy_bypass_url(url, no_proxy=None):
    """Return True when the URL host should bypass configured proxies."""
    parsed = urllib.parse.urlparse(str(url or ""))
    host = (parsed.hostname or "").strip().lower().strip("[]")
    if not host:
        return False
    for raw_pattern in _split_no_proxy(no_proxy):
        pattern = _strip_no_proxy_host(raw_pattern)
        if not pattern:
            continue
        if pattern == "*":
            return True
        if "*" in pattern:
            regex = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
            if re.match(regex, host):
                return True
            continue
        if pattern.startswith(".") and host.endswith(pattern):
            return True
        if host == pattern or host.endswith("." + pattern):
            return True
    return False


def _proxy_config_from_settings():
    cfg = _load_proxy_config()
    no_proxy = str(cfg.get("proxy_no_proxy") or DEFAULT_NO_PROXY).strip()
    proxy_url = normalize_proxy_url(cfg.get("proxy_url"))
    return {
        "enabled": bool(cfg.get("proxy_enabled")),
        "proxy_url": proxy_url,
        "no_proxy": no_proxy,
    }


def apply_proxy_environment(cfg=None):
    """Apply NO_PROXY so env/system proxies do not capture local services."""
    data = dict(cfg or _load_proxy_config())
    no_proxy = str(data.get("proxy_no_proxy") or DEFAULT_NO_PROXY).strip()
    if no_proxy:
        os.environ["NO_PROXY"] = no_proxy
        os.environ["no_proxy"] = no_proxy
    return no_proxy


def request_proxy_kwargs(url):
    cfg = _proxy_config_from_settings()
    no_proxy = apply_proxy_environment(cfg)
    if proxy_bypass_url(url, no_proxy):
        return {"proxies": {"http": None, "https": None, "all": None}}
    if cfg["enabled"] and cfg["proxy_url"]:
        return {
            "proxies": {
                "http": cfg["proxy_url"],
                "https": cfg["proxy_url"],
                "all": cfg["proxy_url"],
            }
        }
    return {}


def proxy_summary():
    cfg = _proxy_config_from_settings()
    env_proxies = urllib.request.getproxies()
    if cfg["enabled"] and cfg["proxy_url"]:
        mode = "manual"
        source = "配置代理"
        proxy_url = cfg["proxy_url"]
    elif env_proxies.get("https") or env_proxies.get("http"):
        mode = "environment"
        source = "环境/系统代理"
        proxy_url = env_proxies.get("https") or env_proxies.get("http") or ""
    else:
        mode = "direct"
        source = "直连"
        proxy_url = ""
    return {
        "mode": mode,
        "source": source,
        "proxy_url": proxy_url,
        "no_proxy": cfg["no_proxy"],
    }


@contextmanager
def override_proxy_config(cfg):
    """Temporarily test requests with unsaved proxy settings."""
    token = _proxy_config_override.set(dict(cfg or {}))
    try:
        yield
    finally:
        _proxy_config_override.reset(token)


def request_get(url, **kwargs):
    kwargs = dict(kwargs)
    kwargs.update({k: v for k, v in request_proxy_kwargs(url).items() if k not in kwargs})
    return requests.get(url, **kwargs)


def request_post(url, **kwargs):
    kwargs = dict(kwargs)
    kwargs.update({k: v for k, v in request_proxy_kwargs(url).items() if k not in kwargs})
    return requests.post(url, **kwargs)


class ProxyAwareSession(requests.Session):
    def request(self, method, url, **kwargs):
        kwargs = dict(kwargs)
        kwargs.update({k: v for k, v in request_proxy_kwargs(url).items() if k not in kwargs})
        return super().request(method, url, **kwargs)


def create_retry_session(
    retries=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504]
):
    """Create a requests session with retry policy."""
    req_session = ProxyAwareSession()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    req_session.mount("https://", adapter)
    req_session.mount("http://", adapter)
    return req_session


session = create_retry_session()
