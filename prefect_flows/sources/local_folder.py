"""
Source adapter: reads all media/text files from a local folder.
Useful for testing the pipeline end-to-end before a real scraper/RSS/API source is wired up.
"""
import mimetypes
from pathlib import Path
from typing import Iterator

from prefect_flows.sources.base import BaseSource, Candidate

_EXT_TO_SOURCE_TYPE = {
    ".mp4": "video", ".mov": "video", ".mkv": "video", ".avi": "video",
    ".mp3": "audio", ".wav": "audio", ".m4a": "audio", ".flac": "audio", ".webm": "audio",
    ".txt": "text", ".html": "text", ".pdf": "text",
}


class LocalFolderSource(BaseSource):
    def __init__(self, folder: str):
        self.folder = Path(folder)

    def discover(self) -> Iterator[Candidate]:
        if not self.folder.exists():
            raise FileNotFoundError(f"Local source folder not found: {self.folder}")

        for path in sorted(self.folder.rglob("*")): # rglob recursively finds all files in the folder and subfolders
            if not path.is_file():
                continue
            source_type = _EXT_TO_SOURCE_TYPE.get(path.suffix.lower())
            if source_type is None:
                continue  # unsupported extension, skip silently

            mime_type, _ = mimetypes.guess_type(str(path))
            yield Candidate(
                source_url=str(path.resolve()),
                source_type=source_type,
                file_name=path.name,
                is_local=True,
                mime_type=mime_type,
            )
