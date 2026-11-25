"""faster-whisper transcription, model loaded once per worker process"""
import re
import os
from typing import Optional
from faster_whisper import WhisperModel

WHISPER_MODEL_SIZE = "medium"
WHISPER_DEVICE = os.environ.get("WHISPER_DEVICE", "auto")          # "cuda", "cpu", "auto"
WHISPER_COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "")   # automated choice

BASE_PROMPT = (
    "Speech by President Donald Trump. Political and governmental vocabulary: "
    "Congress, administration, tariffs, border security, inflation, the economy, "
    "NATO, immigration, executive order, the White House, Republicans, Democrats."
)

CONTEXT_FIELD_CANDIDATES = ["description", "summary", "about"]

DEFAULT_LANGUAGE = "en"

# Languages we accept from the researcher (ISO 639-1). Anything else falls back to DEFAULT_LANGUAGE.
_SUPPORTED_LANGUAGES = frozenset(
    {
        "en",  # English
        "fr",  # French
        "ar",  # Arabic
        "es",  # Spanish
        "de",  # German
        "it",  # Italian
        "pt",  # Portuguese
        "tr",  # Turkish
        "ru",  # Russian
        "uk",  # Ukrainian
        "zh",  # Chinese
        "ja",  # Japanese
        "ko",  # Korean
        "hi",  # Hindi
    }
)
_LANGUAGE_NAMES = {
    "english": "en",
    "french": "fr",
    "arabic": "ar",
    "spanish": "es",
    "german": "de",
    "italian": "it",
    "portuguese": "pt",
    "turkish": "tr",
    "russian": "ru",
    "ukrainian": "uk",
    "chinese": "zh",
    "japanese": "ja",
    "korean": "ko",
    "hindi": "hi",
}

_model: Optional[WhisperModel] = None


def resolve_language(raw_metadata: dict) -> str:
    """
    Whisper language from the researcher's input: an ISO code ("fr", "en-US", "zh_CN") or a common
    English name ("French"). Unknown or unsupported values fall back to DEFAULT_LANGUAGE.
    """
    value = str(((raw_metadata or {}).get("user_provided") or {}).get("language") or "").strip().lower()
   
    if not value:
        return None

    value = _LANGUAGE_NAMES.get(value, value)
    value = re.split(r"[-_]", value, maxsplit=1)[0]

    return value if value in _SUPPORTED_LANGUAGES else None


def get_whisper_model() -> WhisperModel:
    global _model
    if _model is None:
        device = WHISPER_DEVICE
        if device == "auto":
            import ctranslate2
            device = "cuda" if ctranslate2.get_cuda_device_count() > 0 else "cpu"
        compute = WHISPER_COMPUTE_TYPE or ("float16" if device == "cuda" else "int8")
        _model = WhisperModel(WHISPER_MODEL_SIZE, device=device, compute_type=compute)
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
        language=resolve_language(raw_metadata or {}),
    )
    return "\n".join(seg.text.strip() for seg in segments).strip()