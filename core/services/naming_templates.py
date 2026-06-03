import logging
import os
import re

from utils.value_utils import normalize_compare_text, safe_str


MEDIA_SUFFIX_START_RE = re.compile(
    r"""(?ix)
    (?:^|[.\s_\-\[\(])
    (
        \d{3,4}p
        |fhd
        |2k|qhd
        |web[.\s_-]?dl
        |web[.\s_-]?rip
        |blu[.\s_-]?ray
        |bluray
        |bdrip
        |bdremux
        |remux
        |hdtv
        |uhdtv
        |hddvd
        |hdrip
        |dvdrip
        |uhd
        |hevc
        |x265
        |x264
        |h[.\s_-]?265
        |h[.\s_-]?264
        |av1
        |avs\+?
        |avs[23]
        |vc[.\s_-]?1
        |mpeg\d?
        |divx
        |xvid
        |hdr10\+?
        |hdr
        |dolby[.\s_-]?vision
        |dovi
        |dv
        |hlg
        |edr
        |sdr
        |imax
        |3d
        |aac(?:[.\-_]?\d\.\d)?
        |ddp(?:[.\-_]?\d\.\d)?
        |dd(?:[.\-_]?\d\.\d)?
        |dts(?:[.\-_]?hd)?(?:[.\-_]?ma)?
        |eac3
        |ac3
        |truehd
        |atmos
        |dolby[.\s_-]?atmos
        |lpcm
        |opus
        |vorbis
        |flac
        |pcm
        |tving
        |nf
        |netflix
        |amzn
        |amazon
        |dsnp
        |disney\+?
        |hmax
        |hbo[.\s_-]?max
        |hulu
        |colortv
        |atvp
        |apple[.\s_-]?tv
        |pmtp
        |paramount\+?
        |itunes
        |max
        |diy
        |repack
        |proper
        |rerip
        |hq
        |10bit
        |12bit
    )
    """,
)
LEGACY_EXT_PLACEHOLDER_RE = re.compile(r"(\s*-\s*)?\{ext\}")
JINJA_EXT_PLACEHOLDER_RE = re.compile(r"(\s*-\s*)?\{\{\s*ext\s*\}\}")
MEDIA_SUFFIX_PLACEHOLDER_RE = re.compile(r"\{media_suffix\}|\{\{\s*media_suffix\s*\}\}")


def extract_lang_and_ext(filename, lang_tags):
    tags = str(lang_tags or "").strip()
    if not tags:
        return os.path.splitext(filename)

    tag_items = [t.strip() for t in tags.split("|") if t.strip()]
    if not tag_items:
        return os.path.splitext(filename)

    safe_tags = "|".join(re.escape(t) for t in tag_items)
    pattern = rf"(\.(?:{safe_tags}))?(\.[a-z0-9]+)$"
    try:
        regex = re.compile(pattern, re.I)
    except re.error:
        return os.path.splitext(filename)

    match = regex.search(filename)
    if match and match.group(1):
        return filename[: match.start()], match.group(1) + match.group(2)
    return os.path.splitext(filename)


def extract_media_suffix(filename, pure_name=None):
    text = str(
        pure_name
        if pure_name not in (None, "")
        else os.path.splitext(str(filename or ""))[0]
    ).strip()
    if not text:
        return ""

    match = MEDIA_SUFFIX_START_RE.search(text)
    if not match:
        return ""

    suffix = text[match.start(1):].strip(" ._-[]()")
    if not suffix:
        return ""
    if normalize_compare_text(suffix) == normalize_compare_text(text):
        return ""
    return suffix


def cleanup_rendered_filename(text):
    cleaned = str(text or "")
    cleaned = re.sub(r"\s*[\(\[]\s*[\)\]]", "", cleaned)
    cleaned = re.sub(r"\s*\{\s*\}", "", cleaned)
    cleaned = re.sub(r"\s*\(\s*\)", "", cleaned)
    cleaned = re.sub(r"\s+-\s*-\s+", " - ", cleaned)
    cleaned = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", cleaned)
    cleaned = re.sub(r"\s+(?=\.)", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    return cleaned.strip()


def _inject_media_suffix_before_ext(template, ext_pattern, media_suffix_placeholder, ext_placeholder):
    working = str(template or "")
    match = ext_pattern.search(working)
    if match:
        separator = match.group(1) or " - "
        replacement = f"{separator}{media_suffix_placeholder}{ext_placeholder}"
        return ext_pattern.sub(replacement, working, count=1)

    if re.search(r"\s*-\s*$", working):
        return working + media_suffix_placeholder
    return working + f" - {media_suffix_placeholder}"


def apply_media_suffix_template(template, media_suffix, preserve_media_suffix):
    working = str(template or "")
    suffix = str(media_suffix or "").strip()
    if preserve_media_suffix and suffix and not MEDIA_SUFFIX_PLACEHOLDER_RE.search(working):
        if is_jinja2_template(working):
            working = _inject_media_suffix_before_ext(
                working,
                JINJA_EXT_PLACEHOLDER_RE,
                "{{ media_suffix }}",
                "{{ ext }}",
            )
        else:
            working = _inject_media_suffix_before_ext(
                working,
                LEGACY_EXT_PLACEHOLDER_RE,
                "{media_suffix}",
                "{ext}",
            )
    return working


def is_jinja2_template(template):
    return bool(re.search(r"\{\{|\{%", str(template or "")))


def render_jinja2(template, context):
    try:
        from jinja2 import Undefined
        from jinja2.sandbox import SandboxedEnvironment

        env = SandboxedEnvironment(undefined=Undefined, autoescape=False)
        rendered = env.from_string(str(template)).render(**context)
        return cleanup_rendered_filename(rendered)
    except Exception as err:
        logging.warning("Jinja2 模板渲染失败，回退保留原模板: %s", err)
        return cleanup_rendered_filename(str(template))


def render_filename_template(template, context, preserve_media_suffix=False):
    context = context or {}
    media_suffix = safe_str(context.get("media_suffix"))
    working = apply_media_suffix_template(template, media_suffix, preserve_media_suffix)

    if is_jinja2_template(working):
        return render_jinja2(working, context)

    rendered = (
        str(working)
        .replace("{title}", safe_str(context.get("title")))
        .replace("{year}", safe_str(context.get("year")))
        .replace("{s:02d}", safe_str(context.get("season")))
        .replace("{s}", safe_str(context.get("season")))
        .replace("{season}", safe_str(context.get("season")))
        .replace("{e:02d}", safe_str(context.get("episode")))
        .replace("{e}", safe_str(context.get("episode")))
        .replace("{episode}", safe_str(context.get("episode")))
        .replace("{ep_name}", safe_str(context.get("ep_name")))
        .replace("{media_suffix}", media_suffix)
        .replace("{ext}", safe_str(context.get("ext")))
    )
    return cleanup_rendered_filename(rendered)
