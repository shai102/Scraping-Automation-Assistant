_MOJIBAKE_SUSPECT_CHARS = set(
    "\u0447\u20ac\u50a8\u53a4\u546e\u589c\u5956\u59af\u5bf0\u612d\u6212\u6b13"
    "\u6ce6\u6ec5\u6fb6\u703d\u70ba\u72b5\u7459\u7487\u7730\u7966\u7ca8\u7f03"
    "\u8151\u89e6\u8fac\u934a\u9353\u9354\u935b\u9365\u93bc\u93c3\u93c8\u93cb"
    "\u9422\u950b\u95c4\u975b\ue048\uff46"
)
_MOJIBAKE_TEXT_REPLACEMENTS = {
    "\u935b\u6212\u8151": "命中",
    "\u9353\u0447\u6ce6": "剧集",
    "\u9422\u975b\u5956": "电影",
    "\u93bc\u6ec5\u50a8": "搜索",
    "\u6fb6\u8fac\u89e6": "失败",
    "\u93c8\ue048\u53a4\u7f03": "未配置",
    "\u7487\u950b\u7730": "请求",
    "\u7459\uff46\u703d": "解析",
    "\u9365\u70ba\u20ac": "回退",
    "\u934a\u6b13\u20ac": "候选",
    "\u59af\u2033\u7037": "模型",
    "\u9352\u3085\u757e": "判定",
    "\u9477\ue044\u59e9": "自动",
    "\u7487\u55d7\u57c6": "识别",
    "\u93c3\u72b5\u7ca8\u93cb": "无结果",
    "\u93c3\u72b3\u6665": "无效",
    "\u95c4\u612d\u7966": "限流",
    "\u5bf0\u546e\u589c\u9354": "待手动",
    "\u7f02\u64b3\u74e8": "缓存",
}


def looks_like_mojibake(text):
    sample = str(text or "")
    if not sample:
        return False
    return any(ch in _MOJIBAKE_SUSPECT_CHARS for ch in sample)


def score_human_readable_text(text):
    sample = str(text or "")
    if not sample:
        return 0
    score = 0
    for ch in sample:
        code = ord(ch)
        if 0x4E00 <= code <= 0x9FFF:
            score += 3
        elif ch.isascii() and (ch.isalnum() or ch in " -_:/.,()[]{}+&!?"):
            score += 1
        elif ch not in "\r\n\t":
            score -= 1
    return score


def repair_mojibake_text(text):
    sample = str(text or "")
    if not looks_like_mojibake(sample):
        return sample
    replaced = sample
    for old, new in _MOJIBAKE_TEXT_REPLACEMENTS.items():
        replaced = replaced.replace(old, new)
    if replaced != sample and not looks_like_mojibake(replaced):
        return replaced
    try:
        repaired = sample.encode("gbk", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return replaced
    if not repaired or repaired == sample:
        return replaced
    if score_human_readable_text(repaired) < score_human_readable_text(sample):
        return replaced
    return repaired


def repair_legacy_cache_strings(value):
    if isinstance(value, str):
        return repair_mojibake_text(value)
    if isinstance(value, list):
        return [repair_legacy_cache_strings(v) for v in value]
    if isinstance(value, tuple):
        return tuple(repair_legacy_cache_strings(v) for v in value)
    if isinstance(value, dict):
        return {
            repair_legacy_cache_strings(k): repair_legacy_cache_strings(v)
            for k, v in value.items()
        }
    return value
