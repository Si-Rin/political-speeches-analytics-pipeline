"""
Source adapter: YouTube videos, via yt-dlp.

YouTube URLs (youtube.com/watch?v=..., youtu.be/...) are web pages, not
direct file links — a plain HTTP GET returns HTML, not video bytes. yt-dlp
resolves the page, picks a real media stream, and downloads it to disk.

Because of that, this adapter (unlike LocalFolderSource)
actually downloads the file during candidates() rather than just listing it —
there's no way to get a checksummable byte stream from a YouTube URL without
doing the extraction first. Downloaded files go to a temp directory; the
ingestion flow reads them from there exactly like a local file.

Each download lands in its own WorkArea scope directory (yt-dlp leaves .part files and merge intermediates there).
The scope travels with the Candidate (TempFileHandle): the flow releases it once the candidate is consumed, and
this adapter releases it itself when a download fails, so nothing is left behind.
"""
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, urlparse
from typing import Iterator, List
import os
from dotenv import load_dotenv

from prefect_flows.collection.enums import CandidateStatus
from prefect_flows.collection.handles import TempFileHandle
from prefect_flows.collection.recorder import DiscoveryEntry
from prefect_flows.collection.workarea import WorkArea
from prefect_flows.sources.base import BaseSource, Candidate

load_dotenv()

try:
    import yt_dlp
except ImportError as e:
    raise ImportError(
        "yt-dlp is required for YoutubeSource. Install with: pip install yt-dlp"
    ) from e


_YT_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")     # youtube video IDs (exp: https://www.youtube.com/watch?v=l1infKsQLr0 --> l1infKsQLr0)


def canonical_youtube_url(url: str) -> str:
    """https://www.youtube.com/watch?v=<id> for watch/youtu.be/shorts/embed/live URLs (tracking params, &t= dropped);
    anything else (playlist entries with odd shapes, other hosts) is returned unchanged."""
    try:
        u = urlparse(url.strip())
        host = (u.hostname or "").lower().removeprefix("www.").removeprefix("m.")
        vid = None
        if host == "youtu.be":
            vid = u.path.lstrip("/").split("/")[0]
        elif host in ("youtube.com", "music.youtube.com"):
            if u.path == "/watch":
                vid = (parse_qs(u.query).get("v") or [None])[0]
            else:
                m = re.match(r"^/(?:shorts|embed|live|v)/([^/?]+)", u.path)
                vid = m.group(1) if m else None
        if vid and _YT_ID.match(vid):
            return f"https://www.youtube.com/watch?v={vid}"
    except Exception:
        pass
    return url


class YoutubeSource(BaseSource):
    def __init__(self, urls: List[str], work: WorkArea, playlist_mode: bool = False, max_downloads: int = None,
                 audio_only: bool = False, delay_seconds: float = 2.0, known_urls: list = None, recorder=None):
        """
        urls: list of YouTube video URLs
        playlist_mode: if True, treats the URLs as playlists and downloads all videos in the playlist
        max_downloads: maximum number of videos to download (None for no limit)
        audio_only: if True, downloads best audio track only (smaller, faster — use this if you only need the transcript and don't care about video)
        errors: list of error messages encountered during download
        """
        self.urls = urls
        self.work = work                  # hands out one scope directory per download
        self.delay_seconds = delay_seconds
        self.playlist_mode = playlist_mode
        self.max_downloads = max_downloads
        self.audio_only = audio_only
        self.known_urls = known_urls      # port: known_url(canonical_url) -> bool (Bronze already has this video no matter the format)
        self.recorder = recorder
        self.errors = []
    
    def _extract_metadata(self, info: dict) -> dict:
            """Pull the fields Silver will need out of yt-dlp's raw info dict.
            Keeping this narrow — the full info dict includes every format/thumbnail
            variant and isn't worth storing wholesale."""
            return {
                "title": info.get("title"),
                "description": info.get("description"),
                "upload_date": info.get("upload_date"),  # YYYYMMDD string, Silver parses to date
                "channel": info.get("channel"),
                "channel_id": info.get("channel_id"),
                "uploader_url": info.get("uploader_url"),
                "duration": info.get("duration"),          # seconds
                "view_count": info.get("view_count"),
                "like_count": info.get("like_count"),
                "tags": info.get("tags"),
                "categories": info.get("categories"),
                "webpage_url": info.get("webpage_url"),
                "language": info.get("language"),           # yt-dlp's own guess, if present
            }

    def candidates(self) -> Iterator[Candidate]:
        base_opts = {
            "format": "bestaudio/best" if self.audio_only else "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
            "merge_output_format": "mp4",
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
        }

        pot_url = os.getenv("YTDLP_POT_URL")

        if pot_url:
            base_opts["extractor_args"] = {
                "youtubepot-bgutilhttp": {"base_url": [pot_url]},
            }
        # check if the urls are given explicitly as a list or in a file
        if len(self.urls) == 1 and Path(self.urls[0]).is_file():
            with open(self.urls[0], "r") as f:
                urls = [line.strip() for line in f if line.strip()]
        else:
            urls = self.urls

        for url in urls:
            entry_urls = [url]  # the url is considered a single entry by default
            
            if self.playlist_mode:  # if the url refers to a playlist, we need to extract the individual video URLs
                listing_opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "extract_flat": True,
                }
                with yt_dlp.YoutubeDL(listing_opts) as ydl:
                    try:
                        playlist_info = ydl.extract_info(url, download=False)
                        entries = playlist_info.get("entries", [])
                        if self.max_downloads:
                            entries = entries[:self.max_downloads]
                        entry_urls = [entry.get("url") for entry in entries if entry.get("url")]
                        print(f"[YoutubeSource] Extracted {len(entry_urls)} video URLs from playlist")
                        
                        for entry_url in entry_urls:
                            print(f"  - {entry_url}")  # Debug: print each entry URL
                        
                    except Exception as e:
                        print(f"[YoutubeSource] Failed to extract playlist '{url}': {e}")
                        continue  # skip this url and move to the next one

            # Video(s) download loop
            failed_count = 0
            for entry_url in entry_urls:
                scope = self.work.scope("yt")
                handed_over = False
                try:
                    entry_url = canonical_youtube_url(entry_url)
                    # The checksum would not catch the content if the format differs, so we need to check the canonical url separately
                    if self.known_urls is not None and self.known_urls.known_url(entry_url):
                        print(f"[YoutubeSource] Skipping '{entry_url}': already ingested")
                        if self.recorder is not None:
                            self.recorder.record(DiscoveryEntry(
                                source_name="youtube", source_url=entry_url, canonical_url=entry_url, status=CandidateStatus.KNOWN_URL_SKIPPED,
                                detail="This video is already in Bronze (any format); skipped before download."))
                        continue
                    opts = {**base_opts, "outtmpl": str(scope.path / "%(id)s.%(ext)s")}
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(entry_url, download=True)
                        downloaded_path = Path(ydl.prepare_filename(info))

                    if not downloaded_path.exists():
                        raise FileNotFoundError(f"yt-dlp reported success but file not found: {downloaded_path}")

                    candidate = Candidate(
                        source_url=entry_url,
                        canonical_url=entry_url,
                        source_type="audio" if self.audio_only else "video",
                        file_name=downloaded_path.name,
                        handle=TempFileHandle(downloaded_path, scope),
                        mime_type="audio/mp4" if self.audio_only else "video/mp4",
                        raw_metadata=self._extract_metadata(info),
                    )
                    handed_over = True          # from here on the consumer owns (and releases) the scope
                    yield candidate
                except Exception as e:
                    failed_count += 1
                    # one bad/unavailable url shouldnt stop the whole batch
                    print(f"[YoutubeSource] Failed to download '{entry_url}': {type(e).__name__}: {e}")
                    self.errors.append(f"{entry_url}: {type(e).__name__}: {e}")
                    continue
                finally:
                    if not handed_over:
                        scope.release()         # producer releases what it does not yield
                    if self.delay_seconds:
                        time.sleep(self.delay_seconds)

            if failed_count:
                print(f"[YoutubeSource] Failed to download {failed_count} out of {len(entry_urls)} entries from '{url}'")
                
        