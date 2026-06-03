"""Recognition test API — run the current parser chain without touching files."""

from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.recognition.test_service import run_batch_test, run_single_test


router = APIRouter(prefix="/api/recognition-test", tags=["recognition-test"])


class RecognitionTestBody(BaseModel):
    filename: str
    use_ai: bool = False
    bypass_cache: bool = False
    media_type: str = "auto"
    data_source: Optional[str] = None


class RecognitionBatchCase(BaseModel):
    filename: str
    expected_title: Optional[str] = None
    expected_year: Optional[Any] = None
    expected_season: Optional[Any] = None
    expected_episode: Optional[Any] = None
    expected_provider: Optional[str] = None
    expected_id: Optional[str] = None
    media_type: str = "auto"


class RecognitionBatchBody(BaseModel):
    cases: list[RecognitionBatchCase]
    data_source: Optional[str] = None
    bypass_cache: bool = False


def _model_to_dict(model):
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


@router.post("", response_model=dict)
def run_recognition_test(body: RecognitionTestBody):
    try:
        return run_single_test(_model_to_dict(body))
    except ValueError as err:
        raise HTTPException(400, detail=str(err)) from err


@router.post("/batch", response_model=dict)
def run_recognition_batch(body: RecognitionBatchBody):
    payload = _model_to_dict(body)
    payload["cases"] = [_model_to_dict(case) for case in body.cases]
    try:
        return run_batch_test(payload)
    except ValueError as err:
        raise HTTPException(400, detail=str(err)) from err
