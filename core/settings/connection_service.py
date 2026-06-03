import time
import urllib.parse

from ai.ollama_ai import test_silicon_api
from utils.proxy import DEFAULT_NO_PROXY, override_proxy_config, proxy_summary, request_get


def extract_local_model_names(payload: dict) -> list[str]:
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


def list_local_ai_models(base_url: str) -> tuple[list[str], str]:
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return [], "本地 AI 地址未配置"

    endpoints = ["/api/tags", "/models" if normalized.endswith("/v1") else "/v1/models"]
    errors = []
    for endpoint in endpoints:
        try:
            resp = request_get(normalized + endpoint, timeout=8)
            if resp.status_code != 200:
                errors.append(f"{endpoint}: HTTP {resp.status_code}")
                continue
            try:
                models = extract_local_model_names(resp.json())
            except ValueError:
                errors.append(f"{endpoint}: 返回内容不是 JSON")
                continue
            if models:
                return models, "已获取本地模型列表"
            errors.append(f"{endpoint}: 未发现模型")
        except Exception as err:
            errors.append(f"{endpoint}: {str(err)[:120]}")

    return [], "；".join(errors) or "读取本地模型失败"


def test_tmdb_connection(api_key: str) -> tuple[bool, str]:
    try:
        resp = request_get(
            "https://api.themoviedb.org/3/configuration",
            params={"api_key": api_key},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, "TMDB 连接成功"
        return False, f"HTTP {resp.status_code}"
    except Exception as err:
        return False, str(err)[:200]


def test_ai_connection(cfg: dict) -> tuple[bool, str, list[str]]:
    if cfg.get("prefer_ollama", False):
        models, message = list_local_ai_models(cfg.get("ollama_url", "http://localhost:11434"))
        return bool(models), message[:300], models

    success, message = test_silicon_api(
        cfg.get("sf_api_url", "https://api.siliconflow.cn/v1"),
        cfg.get("sf_api_key", ""),
        cfg.get("sf_model", "deepseek-ai/DeepSeek-V3"),
    )
    return success, message, []


def test_telegram_connection(token: str, chat_id: str) -> tuple[bool, str]:
    from utils.telegram_notify import send_test_message

    result = send_test_message(token, chat_id)
    if result.get("ok"):
        return True, "Telegram 测试消息发送成功"
    return False, result.get("description", "发送失败")


def test_emby_server(url: str, api_key: str) -> tuple[bool, str]:
    from utils.emby_notify import test_emby_connection

    return test_emby_connection(url, api_key)


def service_url_label(url: str) -> str:
    host = urllib.parse.urlparse(url).netloc or url
    return host.replace(":443", "").replace(":80", "")


def build_proxy_test_targets(cfg: dict) -> list[dict]:
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
        targets.insert(0, {"name": service_url_label(api_url), "url": model_url, "auth": "ai"})
    return targets


def run_proxy_test(cfg: dict) -> dict:
    cfg = dict(cfg or {})
    cfg.setdefault("proxy_enabled", False)
    cfg.setdefault("proxy_url", "")
    cfg.setdefault("proxy_no_proxy", DEFAULT_NO_PROXY)

    results = []
    ok_count = 0
    latencies = []
    with override_proxy_config(cfg):
        summary = proxy_summary()
        for target in build_proxy_test_targets(cfg):
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
