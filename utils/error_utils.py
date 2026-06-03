ERROR_CODE_TIMEOUT = "TIMEOUT"
ERROR_CODE_CONFIG = "CONFIG"
ERROR_CODE_HTTP = "HTTP"
ERROR_CODE_PARSE = "PARSE"
ERROR_CODE_NO_RESULT = "NO_RESULT"
ERROR_CODE_INVALID = "INVALID"
ERROR_CODE_UNKNOWN = "UNKNOWN"
ERROR_CODES = {
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_PARSE,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_INVALID,
    ERROR_CODE_UNKNOWN,
}


def format_error_message(code, message):
    code_text = str(code or "").strip().upper()
    message_text = str(message or "").strip()
    if code_text in ERROR_CODES:
        return f"{code_text}:{message_text}"
    return message_text


def parse_error_message(message):
    text = str(message or "").strip()
    if not text:
        return "", ""

    if ":" in text:
        code, detail = text.split(":", 1)
        code = code.strip().upper()
        if code in ERROR_CODES:
            return code, detail.strip()

    if "超时" in text:
        return ERROR_CODE_TIMEOUT, text
    if "未配置" in text:
        return ERROR_CODE_CONFIG, text
    if "HTTP" in text:
        return ERROR_CODE_HTTP, text
    if "解析失败" in text or "JSON" in text:
        return ERROR_CODE_PARSE, text
    if "无结果" in text or "未匹配" in text:
        return ERROR_CODE_NO_RESULT, text
    if "无效" in text:
        return ERROR_CODE_INVALID, text
    return ERROR_CODE_UNKNOWN, text
