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
import time
from typing import Iterator, List

from prefect_flows.sources.base import BaseSource, Candidate

try:
    import yt_dlp
except ImportError as e:
    raise ImportError(
        "yt-dlp is required for YoutubeSource. Install with: pip install yt-dlp"
    ) from e


class YoutubeSource(BaseSource):
    def __init__(self, urls: List[str], playlist_mode: bool = False, max_downloads: int = None, audio_only: bool = False):
        """
        urls: list of YouTube video URLs
        playlist_mode: if True, treats the URLs as playlists and downloads all videos in the playlist
        max_downloads: maximum number of videos to download (None for no limit)
        audio_only: if True, downloads best audio track only (smaller,
                    faster — use this if you only need the transcript and
                    don't care about video)
        """
        self.urls = urls
        self.playlist_mode = playlist_mode
        self.max_downloads = max_downloads
        self.audio_only = audio_only
    
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

    def discover(self) -> Iterator[Candidate]:
        download_dir = Path(tempfile.mkdtemp(prefix="youtube_dl_"))
        
        download_opts = {
            "format": "bestaudio/best" if self.audio_only else "best[ext=mp4]/best",
            "outtmpl": str(download_dir / "%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
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
                try:
                    with yt_dlp.YoutubeDL(download_opts) as ydl:
                        info = ydl.extract_info(entry_url, download=True)
                        downloaded_path = Path(ydl.prepare_filename(info))

                    if not downloaded_path.exists():
                        raise FileNotFoundError(f"yt-dlp reported success but file not found: {downloaded_path}")

                    yield Candidate(
                        source_url=str(downloaded_path),
                        source_type="audio" if self.audio_only else "video",
                        file_name=downloaded_path.name,
                        is_local=True,
                        mime_type="audio/mp4" if self.audio_only else "video/mp4",
                        raw_metadata=self._extract_metadata(info)
                    )
                except Exception as e:
                    failed_count += 1
                    # one bad/unavailable url shouldnt stop the whole batch
                    print(f"[YoutubeSource] Failed to download '{entry_url}': {e}")
                    continue
                finally:
                    time.sleep(2)
                    
            if failed_count:
                print(f"[YoutubeSource] Failed to download {failed_count} out of {len(entry_urls)} entries from '{url}'")
                
        