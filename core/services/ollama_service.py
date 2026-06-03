import json
import re

import requests

from utils.app_runtime import TIMEOUT_OLLAMA_CHAT, TIMEOUT_OLLAMA_TAGS
from utils.error_utils import (
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    format_error_message,
)
from utils.proxy import request_get, request_post


def ollama_post_json(base_url, endpoint, payload, timeout):
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        raise ValueError("Ollama URL 未配置")
    return request_post(normalized + endpoint, json=payload, timeout=timeout)


def extract_ollama_model_names(payload):
    if not isinstance(payload, dict):
        raise ValueError("Ollama响应不是JSON对象")

    models = payload.get("models")
    if not isinstance(models, list):
        raise ValueError("Ollama响应缺少models列表")

    names = []
    for item in models:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def list_ollama_models(base_url):
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return [], "Ollama URL 未配置"

    try:
        response = request_get(normalized + "/api/tags", timeout=TIMEOUT_OLLAMA_TAGS)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError:
            return [], "Ollama返回非JSON响应"

        names = extract_ollama_model_names(payload)
        if not names:
            return [], "未发现本地已安装模型"
        return names, "已获取本地模型列表"
    except requests.exceptions.Timeout:
        return [], "读取本地模型超时"
    except Exception as err:
        return [], f"读取本地模型失败: {err}"


def normalize_top_p(value, default=0.9):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(1.0, number))


def normalize_temperature(value, default=0.2):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(2.0, number))


def normalize_ollama_parse_result(data):
    title = str((data or {}).get("title") or "").strip()

    year = (data or {}).get("year")
    try:
        year = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        year = None

    season_raw = (data or {}).get("season")
    try:
        season = int(season_raw) if season_raw not in (None, "") else 1
    except (TypeError, ValueError):
        season = 1
    season = max(0, season)

    episode_raw = (data or {}).get("episode")
    try:
        episode = int(episode_raw) if episode_raw not in (None, "") else 1
    except (TypeError, ValueError):
        episode = 1
    episode = max(0, episode)

    return {
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
    }


def parse_with_ollama(base_url, model, filename, temperature=0.2, top_p=0.9):
    model = str(model or "").strip()
    if not str(base_url or "").strip() or not model:
        return None, "Ollama URL 或模型未配置"

    prompt = r"""
你是动漫/影视文件名解析助手。

任务：
从文件名中提取作品标题、年份、季数、集数。

硬性规则：
1. 只输出 JSON，不要解释，不要 markdown。
2. title 必须是文件名里真实存在的作品名，不允许联想、不允许猜测其他作品。
3. 当文件名同时包含中文和英文标题时（如"迷宫饭.Dungeon.Meshi"），只保留其中一个：
   - 优先保留英文标题（如 "Dungeon Meshi"）
   - 如果英文部分不是完整标题，则保留中文标题
4. 遇到番组文件名时，优先保留原标题，如 Violet_Evergarden -> Violet Evergarden。
5. 删除字幕组、分辨率、编码、语言标签、发布信息，如 KTXP、1080p、BDrip、GB、x264。
6. season 默认 1。
7. episode 必须是数字；像 [01] 这种优先识别为 episode。
8. 如果无法确定 year，填 null。
9. 如果文件名里没有明确作品名，title 设为空字符串，不要猜。

示例：
输入: [KTXP][Dungeon Meshi][01][CHS][1080P][AVC].mkv
输出: {"title": "Dungeon Meshi", "year": null, "season": 1, "episode": 1}

输入: 蜡笔小新.2024.S01E05.1080p.mkv
输出: {"title": "蜡笔小新", "year": 2024, "season": 1, "episode": 5}

输入: 迷宫饭.Dungeon.Meshi.2024.第01话.简繁内封.1080p.mkv
输出: {"title": "Dungeon Meshi", "year": 2024, "season": 1, "episode": 1}

输入: The.Mandalorian.S03E04.2023.WEB-DL.mkv
输出: {"title": "The Mandalorian", "year": 2023, "season": 3, "episode": 4}

输入: [UHA-WINGS][Violet Evergarden][06][CHT][1080p][MP4].mp4
输出: {"title": "Violet Evergarden", "year": null, "season": 1, "episode": 6}

返回格式：
{
  "title": "",
  "year": null,
  "season": 1,
  "episode": 1
}
"""

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": filename},
        ],
        "stream": False,
        "think": False,
        "options": {
            "temperature": normalize_temperature(temperature),
            "top_p": normalize_top_p(top_p),
            "num_predict": 512,
        },
        "timeout": TIMEOUT_OLLAMA_CHAT[1],
    }

    try:
        response = ollama_post_json(
            base_url, "/api/chat", payload, timeout=TIMEOUT_OLLAMA_CHAT
        )
        response.raise_for_status()
        resp = response.json()

        content = resp.get("message", {}).get("content", "").strip()
        if not content:
            return None, format_error_message(ERROR_CODE_PARSE, "Ollama返回空内容")

        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE)

        try:
            data = json.loads(content)
            if not isinstance(data, dict):
                return None, format_error_message(ERROR_CODE_PARSE, "返回内容不是 JSON 对象")
            return normalize_ollama_parse_result(data), "Ollama解析成功"
        except json.JSONDecodeError:
            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return normalize_ollama_parse_result(data), "Ollama解析成功"
            return None, format_error_message(ERROR_CODE_PARSE, "无法解析返回的JSON")

    except requests.exceptions.Timeout:
        return None, format_error_message(ERROR_CODE_TIMEOUT, "Ollama请求超时")
    except Exception as err:
        return None, format_error_message(ERROR_CODE_UNKNOWN, f"Ollama失败: {str(err)}")
