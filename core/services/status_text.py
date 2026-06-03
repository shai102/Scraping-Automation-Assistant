from utils.error_utils import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    parse_error_message,
)


def friendly_status_text(message):
    raw_text = str(message or "").strip()
    if not raw_text:
        return ""

    has_error_hint = (
        ":" in raw_text
        or any(
            token in raw_text
            for token in (
                "超时",
                "未配置",
                "HTTP",
                "解析失败",
                "JSON",
                "无结果",
                "未匹配",
                "无效",
                "失败",
                "异常",
                "错误",
            )
        )
    )
    if not has_error_hint:
        return raw_text

    code, detail = parse_error_message(message)
    if not code:
        return raw_text

    if code == ERROR_CODE_HTTP and ("429" in raw_text.lower() or "rate limit" in raw_text.lower()):
        return "AI接口限流，请稍后重试"

    template = {
        ERROR_CODE_TIMEOUT: "请求超时，请稍后重试",
        ERROR_CODE_CONFIG: "配置缺失，请检查密钥设置",
        ERROR_CODE_HTTP: "接口请求失败，请检查网络或服务状态",
        ERROR_CODE_PARSE: "返回解析失败，请稍后重试",
        ERROR_CODE_NO_RESULT: "未找到匹配结果",
        ERROR_CODE_INVALID: "输入无效或资源不存在",
        ERROR_CODE_UNKNOWN: "处理失败，请查看日志",
    }.get(code, "处理失败，请查看日志")

    if detail and code in {ERROR_CODE_PARSE, ERROR_CODE_HTTP, ERROR_CODE_UNKNOWN}:
        compact_detail = " ".join(str(detail).split())
        return f"{template} (返回: {compact_detail[:60]})"
    return template


def build_status_text(*messages):
    raw_parts = [str(m).strip() for m in messages if str(m or "").strip()]
    if not raw_parts:
        return ""

    friendly_parts = [friendly_status_text(m) for m in raw_parts]
    merged = list(dict.fromkeys(friendly_parts))
    return " / ".join(merged)
