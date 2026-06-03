import re


VERSION_TAG_RE = re.compile(
    r"\[(NC\.Ver|SP|OVA|Extra|Special|OAD|Creditless)\]", re.I
)
