import re


EXTRA_TITLE_MARKER_RE = re.compile(
    r"(?i)(?:总集|總集|総集|recap|summary|compilation|digest|特别篇|特別編|特別篇|specials?|ova|oad|prologue|nc\.?\s*ver|creditless)"
)
VARIANT_TITLE_MARKERS = {
    "diary": re.compile(r"(?i)(?:diar(?:y|ies)|nikki|日记|日記|日志|日誌)"),
    "spinoff": re.compile(r"(?i)(?:spin[-_. ]?off|外传|外傳|番外)"),
    "prequel": re.compile(r"(?i)(?:prequel|origin|前传|前傳)"),
    "reunion": re.compile(r"(?i)(?:reunion|hosted\s+by|aftershow|after\s+the)"),
}


def text_mentions_extra_title(text):
    return bool(EXTRA_TITLE_MARKER_RE.search(str(text or "")))


def title_variant_markers(text):
    raw = str(text or "")
    return {
        name for name, pattern in VARIANT_TITLE_MARKERS.items() if pattern.search(raw)
    }


def candidate_looks_like_extra_title(candidate):
    meta = (candidate or {}).get("meta") or {}
    fields = [
        (candidate or {}).get("title") or "",
        (candidate or {}).get("alt_title") or "",
        meta.get("original_title") or "",
    ]
    return text_mentions_extra_title(" ".join(str(value) for value in fields if value))


def candidate_looks_like_unrequested_variant(candidate, source_text):
    meta = (candidate or {}).get("meta") or {}
    fields = [
        (candidate or {}).get("title") or "",
        (candidate or {}).get("alt_title") or "",
        meta.get("original_title") or "",
    ]
    candidate_markers = title_variant_markers(
        " ".join(str(value) for value in fields if value)
    )
    if not candidate_markers:
        return False
    source_markers = title_variant_markers(source_text)
    return not candidate_markers.issubset(source_markers)

