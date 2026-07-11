"""Build confidence and an explainable trace from existing recognition state."""

from core.models.recognition_result import RecognitionResult


def _confidence_level(value: float) -> str:
    if value >= 0.85:
        return "high"
    if value >= 0.60:
        return "medium"
    return "low"


def calculate_confidence(state: dict, match_state: dict) -> tuple[float, list[str]]:
    tid = str(match_state.get("tid") or "None")
    parse_source = str(state.get("parse_source") or "")
    match_reason = str(match_state.get("db_message") or "")
    warnings = []
    score = 0.45 if tid != "None" else 0.10

    score += {"guessit": 0.15, "hybrid": 0.12, "ai": 0.10}.get(parse_source, 0.05)
    if "文件夹ID锁定" in match_reason:
        score += 0.30
    elif "标题匹配" in match_reason or "标题完全匹配" in match_reason:
        score += 0.25
    elif "直搜首位" in match_reason or "高置信" in match_reason:
        score += 0.20
    elif "自动评分" in match_reason:
        score += 0.17
    elif "判定" in match_reason:
        score += 0.14
    elif "命中" in match_reason:
        score += 0.10

    if state.get("year"):
        score += 0.05
    else:
        warnings.append("文件名和父目录未提供明确年份")
    if state.get("is_tv"):
        if state.get("episode_calc") is not None:
            score += 0.05
        else:
            warnings.append("未解析到明确集数")
    if parse_source == "ai":
        warnings.append("标题主要来自 AI 解析")
    if tid == "None":
        warnings.append(match_reason or "没有稳定的资料库匹配")

    return round(max(0.0, min(1.0, score)), 3), warnings


def build_recognition_result(state: dict, match_state: dict) -> RecognitionResult:
    confidence, warnings = calculate_confidence(state, match_state)
    trace = [
        {
            "stage": "filename_parse",
            "source": state.get("parse_source") or "unknown",
            "input": state.get("pure") or "",
            "title": state.get("title") or "",
            "year": state.get("year"),
            "season": state.get("season"),
            "episode": state.get("episode_calc"),
        },
        {
            "stage": "database_match",
            "provider": match_state.get("provider_name") or "",
            "query_title": state.get("title") or "",
            "matched_title": match_state.get("std_title") or "",
            "matched_id": str(match_state.get("tid") or "None"),
            "reason": match_state.get("db_message") or "",
        },
        {
            "stage": "decision",
            "confidence": confidence,
            "confidence_level": _confidence_level(confidence),
            "warnings": warnings,
        },
    ]
    return RecognitionResult(
        title=str(match_state.get("std_title") or ""),
        year=state.get("year"),
        media_type=str(state.get("media_type") or ""),
        season=state.get("season"),
        episode=state.get("episode_calc"),
        episode_end=(state.get("episode_range") or [None, None])[1]
        if state.get("episode_range") else None,
        provider=str(match_state.get("provider_name") or ""),
        provider_id=str(match_state.get("tid") or "None"),
        parse_source=str(state.get("parse_source") or ""),
        query_title=str(state.get("title") or ""),
        match_reason=str(match_state.get("db_message") or ""),
        confidence=confidence,
        confidence_level=_confidence_level(confidence),
        warnings=warnings,
        trace=trace,
    )
