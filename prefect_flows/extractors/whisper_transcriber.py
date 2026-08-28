"""faster-whisper transcription, model loaded once per worker process"""
from typing import Optional
from faster_whisper import WhisperModel

WHISPER_MODEL_SIZE = "medium"
WHISPER_DEVICE = "cpu"
WHISPER_COMPUTE_TYPE = "int8"

BASE_PROMPT = (
    "Speech by President Donald Trump. Political and governmental vocabulary: "
    "Congress, administration, tariffs, border security, inflation, the economy, "
    "NATO, immigration, executive order, the White House, Republicans, Democrats."
)

CONTEXT_FIELD_CANDIDATES = ["description", "summary", "about"]

_model: Optional[WhisperModel] = None


def get_whisper_model() -> WhisperModel:
    global _model
    if _model is None:
        _model = WhisperModel(WHISPER_MODEL_SIZE, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE_TYPE)
    return _model


def _first_present(raw_metadata: dict, keys: list[str]) -> Optional[str]:
    extra = raw_metadata.get("extra") or {}
    for key in keys:
        value = raw_metadata.get(key) or extra.get(key)
        if value and str(value).strip():
            return str(value).strip()
    return None


def build_prompt(raw_metadata: dict) -> str:
    """
    Baseline domain vocabulary + whatever context fields this doc's metadata happens to offer
    Field-agnostic on purpose: works the same regardless of which source produced the doc
    """
    title = raw_metadata.get("title")
    context = _first_present(raw_metadata, CONTEXT_FIELD_CANDIDATES)

    parts = [BASE_PROMPT]
    if title:
        parts.append(f"Title: {title}.")
    if context:
        parts.append(f"Context: {context[:300]}") 

    return " ".join(parts)


def transcribe(local_path: str, raw_metadata: Optional[dict] = None) -> str:
    model = get_whisper_model()
    prompt = build_prompt(raw_metadata or {})
    segments, _info = model.transcribe(
        local_path, 
        beam_size=3,
        initial_prompt=prompt,
        language="en"
    )
    return "\n".join(seg.text.strip() for seg in segments).strip()