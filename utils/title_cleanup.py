import re

from utils.media_defaults import DEFAULT_LANG_TAGS
from utils.value_utils import normalize_compare_text


INVALID_QUERY_TITLES = {
    "unknown",
    "none",
    "null",
    "untitled",
    "na",
    "nan",
    "未知",
    "test",
    "tests",
    "sample",
    "samples",
    "tmp",
    "temp",
    "newfolder",
    "新建文件夹",
    "电视剧",
    "电视剧集",
    "电影",
    "动漫",
    "动画",
}
INVALID_QUERY_TITLES_NORMALIZED = set(INVALID_QUERY_TITLES)
GENERIC_SEASON_TITLE_RE = re.compile(
    r"(?i)^(?:season\s*\d{1,2}|s\s*\d{1,2}|第\s*\d{1,2}\s*季)$"
)
BRACKET_CONTENT_RE = re.compile(r"\[([^\]]+)\]")
GROUP_RELEASE_BRACKET_RE = re.compile(r"^\s*(?:\[[^\]]+\]\s*){2,}")
LEADING_RELEASE_GROUP_RE = re.compile(r"^\s*\[([^\]]+)\]\s*([^\[\]\(\)]+)")

_LANG_TAG_PART = "|".join(
    re.escape(tag) for tag in DEFAULT_LANG_TAGS.split("|") if tag.strip()
)
LANG_TAG_TOKEN_RE = re.compile(
    rf"(?i)(?:(?<=^)|(?<=[\s._\-\[\(]))(?:{_LANG_TAG_PART})(?:(?=$)|(?=[\s._\-\]\)]))"
)
LANG_TAG_COMBO_RE = re.compile(
    rf"(?i)(?:(?<=^)|(?<=[\s._\-\[\(]))(?:{_LANG_TAG_PART})(?:\s*[&+]\s*(?:{_LANG_TAG_PART}))+(?:(?=$)|(?=[\s._\-\]\)]))"
)
BRACKET_NOISE_RE = re.compile(
    r"""(?ix)^
    (?:
        \d{1,4}(?:v\d+)?
        | \d{3,4}p | \d{2,3}\s*fps | 4k | 8k | 2k | 10bit | 12bit | hdr\d* | hdr10\+? | dv | dovi
        | dolby(?:\s*vision)? | hlg | edr | sdr | 3d | fhd | uhd | qhd | imax
        | web[-_. ]?dl | web[-_. ]?rip | web | bdrip | bluray | blu[-_. ]?ray | bd | dvd | dvdrip
        | hdtv | uhdtv | hddvd | remux | bdremux | hdrip
        | x264 | x265 | h\.?264 | h\.?265 | hevc | avc | av1
        | avs\+? | avs[23] | vc[-_. ]?1 | mpeg\d? | divx | xvid
        | aac\d? | flac\d? | truehd\d? | atmos | dolby\s*atmos | ddp?\d?
        | dts(?:[-_. ]?hd)?(?:[-_. ]?ma)? | eac3 | ac3 | lpcm\d? | opus\d? | vorbis\d?
        | dd\+?\d? | ma\d? | hr\d? | pcm
        | diy | repack | hq | proper | rerip | internal | limited | extended | uncut | unrated | hybrid
        | chs | cht | sc | tc | gb | big5 | zh[-_. ]?(?:cn|tw|hans|hant)?
        | jpsc | jptc | jap | jpn | eng | kor | chi | zho
        | sub | subs | 字幕 | 简繁 | 内封 | 内嵌 | 外挂 | 简体 | 繁体 | 简日 | 繁日 | 简中 | 繁中
        | baha | b-global | bilibili | netflix | nf | dsnp | disney\+? | amzn | amazon | prime
        | atvp | apple\s*tv\+? | hmax | hbo\s*max | hulu | tving | colortv
        | pmtp | paramount\+? | itunes | max
        | part\d* | cd\d* | disc\d* | disk\d*
        | mp4 | mkv | avi | ts | flv | rmvb | wmv | m2ts | iso
    )$
    """
)
QUERY_SEASON_EP_RE = re.compile(
    r"(?ix)\b(?:S\d{1,2}E\d{1,4}|Season\s*\d{1,2}|S\d{1,2}|Episode\s*\d{1,4}|EP?\s*\d{1,4})\b"
)
PLATFORM_PREFIX_RE = re.compile(
    r"""(?ix)
    ^(?P<prefix>
        HBO\s*MAX|HBOMAX|MAX|ITUNES|I[Tt]
        |NF|NETFLIX|AMZN|AMAZON
        |ATVP|APPLE\s*TV\+?
        |DSNP|DISNEY\+?
        |PMTP|PARAMOUNT\+?
        |HMAX|HULU|TVING|COLORTV
        |B-GLOBAL|BILIBILI|BAHA
        |KKTV|LINETV|FRIDAY|CATCHPLAY
        |CRAVE|STAN|MUBI|PEACOCK|STARZ
    )
    [\s._\-]+(?P<rest>.+)$
    """,
)
_PLATFORM_ABBREVS = frozenset({
    "nf", "netflix", "amzn", "amazon", "dsnp", "disney", "atvp", "hmax",
    "hulu", "pmtp", "paramount", "tving", "colortv", "hbomax", "max",
    "itunes", "bilibili", "baha", "kktv", "linetv", "friday", "catchplay",
    "crave", "stan", "mubi", "peacock", "starz",
})
MEDIA_NOISE_TOKEN_RE = re.compile(
    r"""(?ix)^(
        NF|NETFLIX|AMZN|AMAZON|DSNP|DISNEY|TVING|WEB|WEBDL|WEBRIP|BLURAY|BDRIP|BDREMUX|REMUX|UHD
        |HBOMAX|HBO|MAX|ITUNES|ATVP|PMTP|PARAMOUNT|HMAX|HULU|COLORTV
        |X264|X265|H264|H265|HEVC|AVC|AV1|DIVX|XVID|VC1|MPEG\d?|AVS\d?
        |HDR|HDR10|HDR10PLUS|DV|DOVI|DOLBY|HLG|EDR|SDR|3D|FHD|QHD|IMAX
        |AAC\d*|DDP\d*|DD\d*|DTS(?:HD)?(?:MA)?\d*|TRUEHD\d*|ATMOS|MA|EAC3|AC3
        |LPCM\d*|OPUS\d*|VORBIS\d*|FLAC\d*|PCM
        |PROPER|REPACK|RERIP|INTERNAL|LIMITED|EXTENDED|UNCUT|UNRATED|HYBRID|DIY|HQ
        |10BIT|12BIT|8BIT
    )$"""
)


def strip_platform_prefix(text):
    raw = str(text or "").strip()
    if not raw:
        return raw
    match = PLATFORM_PREFIX_RE.match(raw)
    if match:
        return match.group("rest").strip()

    first_dot = raw.find(".")
    first_space = raw.find(" ")
    sep_pos = -1
    if first_dot > 0 and (first_space < 0 or first_dot < first_space):
        sep_pos = first_dot
    elif first_space > 0:
        sep_pos = first_space
    if sep_pos > 0:
        token = raw[:sep_pos].strip()
        lowered = token.lower().rstrip("+")
        if lowered in _PLATFORM_ABBREVS and lowered not in ("max", "it", "stan"):
            rest = raw[sep_pos + 1 :].strip()
            rest_tokens = [tok for tok in re.split(r"[\s._-]+", rest) if tok]
            if rest_tokens and not re.fullmatch(r"(?i)\d{4}", rest_tokens[0]):
                return rest
    return raw


def clean_search_title(title):
    if not title:
        return ""
    text = strip_platform_prefix(title)
    text = re.sub(r"[\[\]\(\)（）]", " ", text)
    text = re.sub(r"(?<![a-z0-9])[A-Z0-9]{2,}(?:-[A-Z0-9]{2,})+(?![a-z0-9])", " ", text)
    text = re.sub(r"(?<![a-z0-9])\w+@\w+(?![a-z0-9])", " ", text)
    text = LANG_TAG_COMBO_RE.sub(" ", text)
    text = re.sub(r"(?<!\d)\.(?!\d)", " ", text)
    text = re.sub(r"_", " ", text)
    text = re.sub(
        r"(?i)(?:10bit|12bit|FLAC|AAC|AVC|H\.?264|H\.?265|BluRay|Blu-Ray|1080p|2160p|720p|480p|4K|8K|2K|FHD|UHD|QHD|\d{2,3}\s*FPS|SRTx?\d*|x264|x265|HEVC|AV1|AVS[+23]?|VC-?1|MPEG\d?|DivX|Xvid|Remastered|D3D-Raw|BDRip|BDRemux|Web-DL|Web-Rip|HDTV|HDRip|DVDRip|REMUX|Baha|NC\.Ver|完结合集|第.*?季|第.*?集|S\d{1,2}E\d{1,4}|EP?\s*\d{1,4}|DTS(?:-?HD)?(?:-?MA)?|TrueHD|Atmos|Dolby(?:\s*Vision|\s*Atmos)?|DOVI|DDPlus|DDP?\d?|EAC3|AC3|LPCM|Opus|Vorbis|HDR10\+?|HDR|HLG|EDR|SDR|DV|IMAX|REPACK|PROPER|RERIP|DIY|HQ)",
        "",
        text,
    )
    text = LANG_TAG_TOKEN_RE.sub(" ", text)
    text = re.sub(
        r"(?i)\.?\s*(?:mkv|mp4|avi|ts|rmvb|wmv|flv|m2ts|iso|strm|mov|mpg|mpeg|3gp|asf|m4v|f4v)\s*$",
        "",
        text,
    )
    text = re.sub(r"^[\W_]+|[\W_]+$", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_meaningful_query_title(title):
    text = str(title or "").strip()
    if not text:
        return False
    if GENERIC_SEASON_TITLE_RE.match(text):
        return False
    key = normalize_compare_text(text)
    return bool(key) and key not in INVALID_QUERY_TITLES_NORMALIZED


def _is_noise_title_fragment(text):
    raw = str(text or "").strip()
    if not raw:
        return True
    compact = re.sub(r"[\s._-]+", "", raw)
    if BRACKET_NOISE_RE.match(raw) or BRACKET_NOISE_RE.match(compact):
        return True
    tokens = [tok for tok in re.split(r"[\s._-]+", raw) if tok]
    return bool(tokens) and all(BRACKET_NOISE_RE.match(tok) for tok in tokens)


def _looks_like_release_group(text):
    raw = str(text or "").strip()
    if not raw:
        return False
    if re.search(r"[!?！？]", raw):
        return False
    compact = re.sub(r"[\s._-]+", "", raw)
    if re.fullmatch(
        r"""(?ix)
        CHD(?:Bits|PAD|TV|WEB|HKTV)?|HDS(?:ky|TV|Pad|WEB)?|HDH(?:ome|Pad|TV|WEB)?|MTeam(?:TV)?
        |PTer(?:DIY|Game|TV|MTV|WEB)?|Our(?:Bits|TV)|PiGo(?:NF|HB|WEB)?|FROG(?:E|Web)?
        |UB(?:its|WEB|TV)|L(?:eague(?:CD|HD|MTV|TV|NF|WEB)|HD)|VCB-?Studio|(?:Lilith|NC)-?Raws
        |Nekomoe[\s-]?kissaten|KTXP|ANi|JPTV|LOLi|SweetSub|QME|CMCT(?:V)?|FHDMv|FFans(?:BD|TV|WEB|DIY)?
        |BeyondHD|BTN|CMRG|NTb|NTG|ARiN|ExREN|TrollHD|Taengoo|TEPES|D-Z0N3|Shark(?:WEB|DIY|TV|MV)?
        |FLTth|EPSiLON|FLUX|NOSiViD|PlayWEB|HONE(?:yG)?
        """,
        compact,
    ):
        return True
    if "-" in raw:
        return True
    if len(compact) <= 16 and bool(re.fullmatch(r"[A-Za-z0-9]+", compact)):
        return True
    tokens = [tok for tok in re.split(r"[\s._-]+", raw) if tok]
    return 1 <= len(tokens) <= 3 and all(re.fullmatch(r"[A-Za-z0-9]+", tok) for tok in tokens)


def extract_title_after_leading_release_group(pure_name):
    text = str(pure_name or "")
    match = LEADING_RELEASE_GROUP_RE.match(text)
    if not match:
        return ""

    group = clean_search_title(match.group(1))
    if not _looks_like_release_group(group):
        return ""

    title = clean_search_title(match.group(2))
    title = re.sub(r"(?i)[\s._-]+S\d{1,2}E\d{1,4}.*$", "", title).strip()
    title = re.sub(r"\s*[-–]\s*\d{1,3}(?:v\d)?\s*$", "", title).strip()
    title = re.sub(r"(?i)\s+S\d{1,2}\s*$", "", title).strip()
    if not is_meaningful_query_title(title) or _is_noise_title_fragment(title):
        return ""
    return title


def extract_bracket_title_from_filename(pure_name):
    blocks = BRACKET_CONTENT_RE.findall(str(pure_name or ""))
    if not blocks:
        return ""

    candidates = []
    for index, block in enumerate(blocks):
        cleaned = clean_search_title(block)
        if not is_meaningful_query_title(cleaned):
            continue
        if _is_noise_title_fragment(cleaned):
            continue
        candidates.append((index, cleaned))

    if not candidates:
        return ""

    group_release_style = bool(GROUP_RELEASE_BRACKET_RE.match(str(pure_name or "")))
    for index, cleaned in candidates:
        if index == 0 and len(candidates) > 1 and (
            group_release_style or _looks_like_release_group(cleaned)
        ):
            continue
        return cleaned
    return candidates[0][1]


def _normalize_query_token(token):
    return re.sub(r"[\W_]+", "", str(token or "").strip())


def _query_token_is_noise(token):
    raw = str(token or "").strip()
    if not raw:
        return True
    compact = _normalize_query_token(raw)
    if not compact:
        return True
    if BRACKET_NOISE_RE.match(raw) or BRACKET_NOISE_RE.match(compact):
        return True
    if MEDIA_NOISE_TOKEN_RE.match(compact.upper()):
        return True
    if QUERY_SEASON_EP_RE.fullmatch(raw):
        return True
    if LANG_TAG_TOKEN_RE.fullmatch(raw):
        return True
    return compact.isdigit() and 1900 <= int(compact) <= 2099


def _looks_like_trailing_release_group_token(token):
    raw = str(token or "").strip()
    if not raw:
        return False
    compact = _normalize_query_token(raw)
    if len(compact) < 3 or len(compact) > 24:
        return False
    if BRACKET_NOISE_RE.match(raw) or BRACKET_NOISE_RE.match(compact):
        return True
    if compact.isupper():
        return True
    suffixes = ("TV", "RAW", "RAWS", "SUB", "FANSUB", "STUDIO")
    upper_compact = compact.upper()
    for suffix in suffixes:
        if upper_compact.endswith(suffix):
            prefix = compact[: -len(suffix)]
            if len(prefix) >= 2 and not prefix.islower():
                return True
    return bool(re.search(r"[a-z]", raw) and re.search(r"[A-Z]{2,}", raw))


def normalize_search_query_title(title):
    text = clean_search_title(title)
    if not text:
        return ""

    text = re.sub(r"[._]+", " ", text)
    text = QUERY_SEASON_EP_RE.sub(" ", text)
    tokens = [tok for tok in re.split(r"\s+", text) if tok]
    filtered = []
    for index, token in enumerate(tokens):
        if _query_token_is_noise(token):
            continue
        if len(tokens) >= 2 and index == len(tokens) - 1 and _looks_like_trailing_release_group_token(token):
            continue
        filtered.append(token)

    normalized = clean_search_title(" ".join(filtered))
    if normalized and is_meaningful_query_title(normalized):
        return normalized
    return clean_search_title(text)


def build_fallback_token_queries(title, min_length=4):
    text = normalize_search_query_title(title)
    latin_word_tokens = [tok for tok in re.split(r"\s+", text) if re.search(r"[A-Za-z]", tok)]
    has_cjk = bool(re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", text))
    if not has_cjk and len(latin_word_tokens) >= 2:
        return []
    seen = set()
    queries = []
    for raw in re.split(r"\s+", text):
        token = clean_search_title(raw)
        compact = normalize_compare_text(token)
        if not token or len(compact) < min_length:
            continue
        if _query_token_is_noise(token) or compact in seen:
            continue
        seen.add(compact)
        queries.append(token)
    return queries


def derive_title_from_filename(pure_name):
    text = str(pure_name or "")
    leading_group_title = extract_title_after_leading_release_group(text)
    if leading_group_title:
        return leading_group_title
    bracket_title = extract_bracket_title_from_filename(text)
    if bracket_title:
        return bracket_title
    text = text.replace("_", " ").replace(".", " ")
    text = re.sub(r"(?i)\bS\d{1,2}E\d{1,4}\b.*$", "", text)
    text = re.sub(r"(?i)\bEP?\s*\d{1,4}\b.*$", "", text)
    text = re.sub(r"(?i)第\s*\d{1,4}\s*[集话話].*$", "", text)
    text = re.sub(r"(?i)[\[\(（]\s*\d{1,4}(?:v\d+)?\s*[\]\)）]\s*$", "", text)
    return clean_search_title(text)


def split_mixed_title(title):
    if not title or not isinstance(title, str):
        return []
    text = title.strip()
    has_chinese = bool(re.search(r"[一-鿿]", text))
    has_latin = bool(re.search(r"[a-zA-Z]", text))
    if not (has_chinese and has_latin):
        return []

    parts = re.split(r"[\s\.\-_]+", text)
    chinese_parts = []
    latin_parts = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if re.search(r"[一-鿿]", part):
            chinese_parts.append(part)
        elif re.search(r"[a-zA-Z]", part):
            latin_parts.append(part)

    results = []
    if latin_parts:
        results.append(" ".join(latin_parts))
    if chinese_parts:
        results.append("".join(chinese_parts))
    return results

