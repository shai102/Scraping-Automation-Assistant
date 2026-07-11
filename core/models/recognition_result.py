"""Typed, serializable result for the recognition pipeline."""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RecognitionResult:
    title: str = ""
    year: Any = None
    media_type: str = ""
    season: int | None = None
    episode: int | None = None
    episode_end: int | None = None
    provider: str = ""
    provider_id: str = "None"
    parse_source: str = ""
    query_title: str = ""
    match_reason: str = ""
    confidence: float = 0.0
    confidence_level: str = "low"
    warnings: list[str] = field(default_factory=list)
    trace: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)
