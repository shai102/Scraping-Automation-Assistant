import logging
import os

import requests

from utils.app_runtime import TIMEOUT_OLLAMA_EMBED
from utils.title_parsing import clean_search_title
from utils.value_utils import extract_year_from_release, safe_str
from utils.proxy import request_post


def cosine_similarity(vec_a, vec_b):
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b))
    norm_a = sum(float(a) * float(a) for a in vec_a) ** 0.5
    norm_b = sum(float(b) * float(b) for b in vec_b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def build_candidate_embedding_text(candidate):
    title = candidate.get("title") or ""
    alt = candidate.get("alt_title") or ""
    year = extract_year_from_release(candidate.get("release")) or ""
    overview = (candidate.get("meta") or {}).get("overview") or ""
    return f"标题:{title}; 原名:{alt}; 年份:{year}; 简介:{overview[:120]}"


def get_embedding(
    ollama_post_json,
    base_url,
    embedding_model,
    text,
    cache,
    cache_lock,
    preferred_endpoint=None,
):
    clean_text = str(text or "").strip()
    model = str(embedding_model or "").strip()
    if not str(base_url or "").strip() or not model or not clean_text:
        return None, preferred_endpoint

    cache_key = f"{model}::{clean_text}"
    with cache_lock:
        cached = cache.get(cache_key)
    if cached:
        return cached, preferred_endpoint

    payload = {"model": model, "prompt": clean_text}
    endpoints = []
    if preferred_endpoint:
        endpoints.append(preferred_endpoint)
    for endpoint in ("/api/embed", "/api/embeddings"):
        if endpoint not in endpoints:
            endpoints.append(endpoint)

    for endpoint in endpoints:
        try:
            response = ollama_post_json(
                base_url, endpoint, payload, timeout=TIMEOUT_OLLAMA_EMBED
            )
            if response.status_code == 404:
                continue
            response.raise_for_status()
            data = response.json()

            emb = data.get("embedding")
            if not emb:
                emb_list = data.get("embeddings")
                if isinstance(emb_list, list) and emb_list:
                    emb = emb_list[0]

            if isinstance(emb, list) and emb:
                with cache_lock:
                    cache[cache_key] = emb
                return emb, endpoint
        except requests.exceptions.Timeout:
            logging.warning("Embedding请求超时")
            return None, preferred_endpoint
        except Exception as err:
            logging.error(f"Embedding请求失败({endpoint}): {err}")
    return None, preferred_endpoint


def get_online_embedding(api_url, api_key, embedding_model, text, cache, cache_lock):
    clean_text = str(text or "").strip()
    model = str(embedding_model or "").strip()
    normalized = str(api_url or "").strip().rstrip("/")
    key = str(api_key or "").strip()
    if not normalized or not key or not model or not clean_text:
        return None

    endpoint = normalized if normalized.endswith("/embeddings") else normalized + "/embeddings"
    cache_key = f"online::{endpoint}::{model}::{clean_text}"
    with cache_lock:
        cached = cache.get(cache_key)
    if cached:
        return cached

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "input": clean_text,
        "encoding_format": "float",
    }

    try:
        response = request_post(
            endpoint, headers=headers, json=payload, timeout=TIMEOUT_OLLAMA_EMBED
        )
        response.raise_for_status()
        data = response.json()
        rows = data.get("data")
        if not isinstance(rows, list) or not rows:
            logging.error("在线 Embedding 返回格式无效: 缺少 data")
            return None
        emb = rows[0].get("embedding") if isinstance(rows[0], dict) else None
        if isinstance(emb, list) and emb:
            with cache_lock:
                cache[cache_key] = emb
            return emb
        logging.error("在线 Embedding 返回格式无效: 缺少 embedding")
        return None
    except requests.exceptions.Timeout:
        logging.warning("在线 Embedding 请求超时")
    except Exception as err:
        logging.error(f"在线 Embedding 请求失败: {err}")
    return None


def rerank_candidates_with_embedding(
    item, query_title, year, is_tv, source_name, candidates, get_embedding_func
):
    if not candidates:
        return candidates, None, ""

    clean_fn = clean_search_title(os.path.splitext(_extract_item_old_name(item))[0])
    query_text = (
        f"标题:{query_title}; "
        f"文件:{clean_fn}; "
        f"年份:{safe_str(year)}; "
        f"类型:{'剧集' if is_tv else '电影'}"
    )
    q_emb = get_embedding_func(query_text)
    if not q_emb:
        return candidates, None, ""

    scored = []
    for candidate in candidates:
        c_emb = get_embedding_func(build_candidate_embedding_text(candidate))
        if not c_emb:
            continue
        score = cosine_similarity(q_emb, c_emb)
        scored.append((score, candidate))

    if not scored:
        return candidates, None, ""

    scored.sort(key=lambda row: row[0], reverse=True)
    scored_candidates = [candidate for _, candidate in scored]
    ranked = scored_candidates + [candidate for candidate in candidates if candidate not in scored_candidates]

    top_score = scored[0][0]
    second_score = scored[1][0] if len(scored) > 1 else -1.0
    rank_msg = f"Embedding重排 top={top_score:.3f}"

    if top_score >= 0.78 and (len(scored) == 1 or top_score - second_score >= 0.10):
        return ranked, scored[0][1], rank_msg

    return ranked, None, rank_msg


def _extract_item_old_name(item):
    if isinstance(item, dict):
        return str(item.get("old_name", "") or "")
    return str(getattr(item, "old_name", "") or "")
