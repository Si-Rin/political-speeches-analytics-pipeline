"""
Content Extractor: Internet Archive TV News Archive items.

Loops over included (non-excluded) segments, slicing each one locally
from the already-downloaded full audio (no re-fetching from the web),
transcribing each slice independently with Whisper, then joining the
resulting text. Transcribing segments separately — rather than
concatenating audio first and running one pass — avoids Whisper
hallucinating continuity across spliced-together, unrelated time ranges.
"""
import os
import tempfile
from typing import Optional
from pydub import AudioSegment

from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.whisper_transcriber import transcribe
from prefect_flows.extractors.utils import parse_date


class InternetArchiveExtractor(BaseExtractor):
    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        extra = raw_metadata.get("extra", {})
        segments = extra.get("segments", [])
        excluded = set(extra.get("excluded_indices", []))

        included = [
            s for s in segments
            if s["idx"] not in excluded and s["start_sec"] is not None and s["end_sec"] is not None
        ]
        if not included:
            raise ValueError("All segments excluded for this item — nothing to transcribe")

        full_audio = AudioSegment.from_file(local_path)  # decoded once, sliced in memory below
        transcripts = []

        for seg in included:
            slice_audio = full_audio[seg["start_sec"] * 1000: seg["end_sec"] * 1000]
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mp3")
            os.close(tmp_fd)
            try:
                slice_audio.export(tmp_path, format="mp3")
                text = transcribe(tmp_path, raw_metadata)
                if text and text.strip():
                    transcripts.append(text.strip())
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        return "\n\n".join(transcripts) if transcripts else None

    def extract_title(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        return raw_metadata.get("title")

    def extract_pub_date(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        return parse_date(raw_metadata.get("publication_date"))