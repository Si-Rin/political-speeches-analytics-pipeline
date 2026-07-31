"""
Source adapter: YouTube videos, via yt-dlp.

YouTube URLs (youtube.com/watch?v=..., youtu.be/...) are web pages, not
direct file links — a plain HTTP GET returns HTML, not video bytes. yt-dlp
resolves the page, picks a real media stream, and downloads it to disk.

Because of that, this adapter (unlike LocalFolderSource/UrlListSource)
actually downloads the file during discover() rather than just listing it —
there's no way to get a checksummable byte stream from a YouTube URL without
doing the extraction first. Downloaded files go to a temp directory; the
ingestion flow reads them from there exactly like a local file.

Note: downloaded files are left in the OS temp directory rather than
explicitly deleted after ingestion (discover() runs before we know whether
staging/upload will succeed). On a long-lived worker this can accumulate —
fine for occasional/manual runs; add explicit cleanup if this becomes a
scheduled/recurring job.
"""
import tempfile
from pathlib import Path
from typing import Iterator, List

from prefect_flows.sources.base import BaseSource, Candidate

try:
    import yt_dlp
except ImportError as e:
    raise ImportError(
        "yt-dlp is required for YoutubeSource. Install with: pip install yt-dlp"
    ) from e


class YoutubeSource(BaseSource):
    def __init__(self, urls: List[str], audio_only: bool = False):
        """
        urls: list of YouTube video URLs
        audio_only: if True, downloads best audio track only (smaller,
                    faster — use this if you only need the transcript and
                    don't care about video)
        """
        self.urls = urls
        self.audio_only = audio_only

    def discover(self) -> Iterator[Candidate]:
        download_dir = Path(tempfile.mkdtemp(prefix="youtube_dl_"))

        ydl_opts = {
            "format": "bestaudio/best" if self.audio_only else "best[ext=mp4]/best",
            "outtmpl": str(download_dir / "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
        }

        for url in self.urls:
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    downloaded_path = Path(ydl.prepare_filename(info))

                if not downloaded_path.exists():
                    raise FileNotFoundError(f"yt-dlp reported success but file not found: {downloaded_path}")

                yield Candidate(
                    source_url=str(downloaded_path),
                    source_type="audio" if self.audio_only else "video",
                    file_name=downloaded_path.name,
                    is_local=True,
                    mime_type="audio/mp4" if self.audio_only else "video/mp4",
                )
            except Exception as e:
                # one bad/unavailable URL shouldn't stop the whole batch
                print(f"[YoutubeSource] Failed to download '{url}': {e}")
                continue
