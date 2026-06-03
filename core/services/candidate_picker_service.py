import difflib
import json
import logging
import os
import re

import requests

from ai.ollama_ai import (
    _extract_siliconflow_content,
    _normalize_temperature as _normalize_online_temperature,
    _normalize_top_p as _normalize_online_top_p,
    _post_openai_compatible,
)
from utils.app_runtime import TIMEOUT_AI_CHAT, TIMEOUT_OLLAMA_CHAT
from utils.candidate_utils import format_candidate_label
from utils.title_parsing import clean_search_title
from utils.value_utils import extract_year_from_release, safe_str


def candidate_rating(candidate):
    try:
        return float(candidate.get("rating") or 0)
    except (TypeError, ValueError):
        return 0.0


def candidate_title_similarity(query_title, candidate):
    q_norm = re.sub(r"[\W_]+", "", str(query_title or "").lower())
    if not q_norm:
        return 0.0

    meta = candidate.get("meta") or {}
    names = [
        candidate.get("title"),
        candidate.get("alt_title"),
        meta.get("original_title"),
    ]
    best = 0.0
    for name in names:
        n_norm = re.sub(r"[\W_]+", "", str(name or "").lower())
        if not n_norm:
            continue
        if n_norm == q_norm:
            return 1.0
        ratio = difflib.SequenceMatcher(None, q_norm, n_norm).ratio()
        if len(q_norm) >= 4 and len(n_norm) >= 4:
            if q_norm in n_norm or n_norm in q_norm:
                ratio = max(ratio, 0.92)
        best = max(best, ratio)
    return best


def candidate_metadata_score(candidate):
    meta = candidate.get("meta") or {}
    score = 0
    if extract_year_from_release(candidate.get("release") or ""):
        score += 2
    if meta.get("overview"):
        score += 3
    if meta.get("original_title"):
        score += 1
    if candidate_rating(candidate) > 0:
        score += 1
    if meta.get("poster"):
        score += 1
    return score


def auto_pick_candidate_by_score(query_title, year, source_name, candidates):
    if not candidates:
        return None, ""
    if len(candidates) == 1:
        return candidates[0], "唯一候选"

    requested_year = safe_str(year)
    scored = []
    for rank, candidate in enumerate(candidates):
        candidate_year = extract_year_from_release(candidate.get("release") or "")
        title_sim = candidate_title_similarity(query_title, candidate)
        meta_score = candidate_metadata_score(candidate)

        score = 0.0
        score += title_sim * 45.0
        score += max(0, 8 - rank * 2)
        score += meta_score * 3.0

        if requested_year:
            if candidate_year == requested_year:
                score += 25.0
            elif candidate_year:
                score -= 30.0

        scored.append(
            {
                "candidate": candidate,
                "score": score,
                "title_sim": title_sim,
                "meta_score": meta_score,
                "year": candidate_year,
                "rank": rank,
            }
        )

    scored.sort(key=lambda row: row["score"], reverse=True)
    top = scored[0]
    second = scored[1] if len(scored) > 1 else None
    margin = top["score"] - (second["score"] if second else 0.0)

    if top["title_sim"] >= 0.98:
        return top["candidate"], "标题完全匹配"

    if top["title_sim"] >= 0.90 and (not second or margin >= 8.0):
        return top["candidate"], "标题高置信"

    if requested_year and top["year"] == requested_year and (not second or margin >= 10.0):
        return top["candidate"], "年份匹配高置信"

    if (
        top["rank"] == 0
        and top["year"]
        and top["meta_score"] >= 6
        and (
            not second
            or margin >= 8.0
            or (top["meta_score"] - second["meta_score"]) >= 4
        )
    ):
        return top["candidate"], "搜索排序与元数据高置信"

    return None, f"自动评分不足 top={top['score']:.1f} margin={margin:.1f}"


def parse_candidate_pick_response(content):
    text = str(content or "").strip()
    if not text:
        return None

    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE).strip()
    if re.fullmatch(r"\d+", text):
        return {"pick": int(text), "reason": "numeric output"}

    candidates = [text]
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        candidates.append(match.group())

    for raw in candidates:
        repaired = raw
        repaired = re.sub(
            r'("?(?:pick|index|candidate)"?\s*:\s*"?\d+"?)\s+("?(?:reason|id)"?\s*:)',
            r"\1, \2",
            repaired,
            flags=re.IGNORECASE,
        )
        try:
            parsed = json.loads(repaired)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue

    parsed = {}
    pick_match = re.search(
        r'(?i)"?(?:pick|index|candidate)"?\s*[:=]\s*"?(?P<pick>\d+)"?',
        text,
    )
    if pick_match:
        parsed["pick"] = int(pick_match.group("pick"))

    id_match = re.search(
        r'(?i)"?id"?\s*[:=]\s*"?(?P<id>[A-Za-z0-9_.:-]+)"?',
        text,
    )
    if id_match:
        parsed["id"] = id_match.group("id")

    reason_match = re.search(
        r'(?i)"?reason"?\s*[:=]\s*"(?P<reason>[^"]{0,160})"',
        text,
    )
    if reason_match:
        parsed["reason"] = reason_match.group("reason")

    return parsed or None


def extract_item_old_name(item):
    if isinstance(item, dict):
        return str(item.get("old_name", "") or "")
    return str(getattr(item, "old_name", "") or "")


def build_candidate_pick_prompt(item, query_title, year, is_tv, source_name, candidates):
    prompt_lines = []
    for idx, candidate in enumerate(candidates, 1):
        meta = candidate.get("meta") or {}
        overview = " ".join(str(meta.get("overview") or "").split())
        if len(overview) > 120:
            overview = overview[:120] + "..."
        prompt_lines.append(
            f"{idx}. 标题={candidate.get('title', '')}; 原名={candidate.get('alt_title', '')}; "
            f"original={meta.get('original_title', '')}; 年份={extract_year_from_release(candidate.get('release')) or '-'}; "
            f"ID={candidate.get('id')}; 评分={candidate.get('rating', 0)}; 简介={overview}"
        )

    clean_name = os.path.splitext(extract_item_old_name(item))[0]
    return f"""你是媒体数据库匹配助手。请根据文件名、解析出的标题和年份，从候选中选出最可能匹配的一项。
如果无法确定，必须返回 pick 为 0。只允许输出 JSON，不要输出额外说明。
JSON 格式: {{"pick": 0或候选序号, "reason": "简短原因"}}
文件名: {clean_name}
解析标题: {query_title}
年份: {safe_str(year)}
类型: {"剧集" if is_tv else "电影"}
来源: {source_name}
候选列表:
{chr(10).join(prompt_lines)}"""


def extract_openai_message_content(payload):
    choices = payload.get("choices") if isinstance(payload, dict) else None
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        return ""
    content = message.get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts).strip()
    return ""


def pick_candidate_from_content(content, candidates, source_label):
    parsed = parse_candidate_pick_response(content)

    if not isinstance(parsed, dict):
        return None, f"{source_label}返回格式无效"

    pick = parsed.get("pick", parsed.get("index", parsed.get("candidate")))
    picked_id = parsed.get("id")
    reason = parsed.get("reason", "")

    if isinstance(pick, str) and pick.strip().isdigit():
        pick = int(pick.strip())

    if picked_id is not None:
        picked_id = str(picked_id).strip()
        for candidate in candidates:
            if str(candidate.get("id")) == picked_id:
                return candidate, reason or f"{source_label}按 ID 选中"

    if isinstance(pick, int) and 1 <= pick <= len(candidates):
        return candidates[pick - 1], reason or f"{source_label}已选择候选"

    return None, reason or f"{source_label}无法确定"


def pick_candidate_with_ollama(
    ollama_post_json,
    normalize_temperature,
    base_url,
    model,
    item,
    query_title,
    year,
    is_tv,
    source_name,
    candidates,
    temperature=0.2,
):
    if not str(base_url or "").strip() or not str(model or "").strip():
        return None, "未配置本地模型"

    prompt = build_candidate_pick_prompt(item, query_title, year, is_tv, source_name, candidates)

    payload = {
        "model": str(model).strip(),
        "messages": [
            {
                "role": "system",
                "content": "你只输出 JSON。拿不准时 pick 必须返回 0。",
            },
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "think": False,
        "options": {"temperature": normalize_temperature(temperature)},
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
            return None, "本地模型返回空内容"

        return pick_candidate_from_content(content, candidates, "本地模型")
    except requests.exceptions.Timeout:
        return None, "本地模型判定超时"
    except Exception as err:
        logging.error(f"Ollama候选判定失败: {err}")
        return None, f"本地模型判定失败: {err}"


def pick_candidate_with_openai_compatible(
    api_url,
    api_key,
    model,
    item,
    query_title,
    year,
    is_tv,
    source_name,
    candidates,
    temperature=0.2,
    top_p=0.9,
):
    if not str(api_key or "").strip():
        return None, "未配置在线 AI Key"
    if not str(model or "").strip():
        return None, "未配置在线 AI 模型"

    prompt_lines = []
    for idx, candidate in enumerate(candidates, 1):
        meta = candidate.get("meta") or {}
        overview = " ".join(str(meta.get("overview") or "").split())
        if len(overview) > 120:
            overview = overview[:120] + "..."
        prompt_lines.append(
            f"{idx}. 标题={candidate.get('title', '')}; 原名={candidate.get('alt_title', '')}; "
            f"original={meta.get('original_title', '')}; "
            f"年份={extract_year_from_release(candidate.get('release')) or '-'}; "
            f"ID={candidate.get('id')}; 评分={candidate.get('rating', 0)}; 简介={overview}"
        )

    clean_name = os.path.splitext(extract_item_old_name(item))[0]
    prompt = f"""你是媒体数据库候选判定助手。请根据文件名、已解析标题、年份和候选列表，选择最可能匹配的一项。
候选列表非空时必须选择其中一个候选，返回 1 到 {len(candidates)} 的 pick；不要返回 0。
只允许输出 JSON，不要输出解释、Markdown 或代码块。
JSON 格式: {{"pick": 候选序号, "reason": "简短原因"}}

文件名: {clean_name}
解析标题: {query_title}
年份: {safe_str(year)}
类型: {"剧集" if is_tv else "电影"}
来源: {source_name}

候选列表:
{chr(10).join(prompt_lines)}"""

    base_url = (api_url or "https://api.siliconflow.cn/v1").strip().rstrip("/")
    url = f"{base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {str(api_key).strip()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": str(model).strip(),
        "messages": [
            {
                "role": "system",
                "content": (
                    "你只输出 JSON。候选列表非空时必须选择一个最可能候选，"
                    "pick 必须是 1 到候选数量之间的整数。"
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": _normalize_online_temperature(temperature),
        "top_p": _normalize_online_top_p(top_p),
        "max_tokens": 300,
    }

    try:
        response = _post_openai_compatible(
            url, payload, headers=headers, timeout=TIMEOUT_AI_CHAT
        )
        response.raise_for_status()
        content = _extract_siliconflow_content(response.json()).strip()
        content = re.sub(
            r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE
        ).strip()

        parsed = parse_candidate_pick_response(content)

        if not isinstance(parsed, dict):
            return None, "在线 AI 返回格式无效"

        pick = parsed.get("pick", parsed.get("index", parsed.get("candidate")))
        picked_id = parsed.get("id")
        reason = str(parsed.get("reason") or "").strip()

        if isinstance(pick, str) and pick.strip().isdigit():
            pick = int(pick.strip())

        if picked_id is not None:
            picked_id = str(picked_id).strip()
            for candidate in candidates:
                if str(candidate.get("id")) == picked_id:
                    return candidate, reason or "在线 AI 按 ID 选中"

        if isinstance(pick, int) and 1 <= pick <= len(candidates):
            return candidates[pick - 1], reason or "在线 AI 已选择候选"

        if candidates:
            return candidates[0], reason or "在线 AI 未给有效序号，按候选首位补全"
        return None, reason or "在线 AI 无法确定"
    except requests.exceptions.Timeout:
        return None, "在线 AI 判定超时"
    except Exception as err:
        logging.error(f"在线 AI 候选判定失败: {err}")
        return None, f"在线 AI 判定失败: {err}"


def populate_candidate_listbox(lb, candidates):
    for candidate in candidates:
        lb.insert(0x7FFFFFFF, format_candidate_label(candidate))
