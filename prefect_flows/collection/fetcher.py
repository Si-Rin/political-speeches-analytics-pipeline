"""
PoliteFetcher that implements :
    - Politeness (per-host delay, robots.txt)
    - retries with backoff
    - size cap
    - shared User-Agent

Downloads stream into a Scope directory owned by the caller.
"""
import time
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser
import requests

from prefect_flows.collection.enums import ReasonCode
from prefect_flows.collection.workarea import Scope
from prefect_flows.http_config import USER_AGENT

CHUNK_SIZE = 1024 * 1024
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class FetchError(Exception):
    def __init__(self, reason: Optional[ReasonCode], detail: str = ""):
        self.reason, self.detail = reason, detail
        super().__init__(f"{reason.value if reason else 'fetch_error'}: {detail}")


class PoliteFetcher:
    def __init__(
        self,
        user_agent: str = USER_AGENT,
        delay_seconds: float = 1.0,
        timeout: float = 30.0,
        max_retries: int = 2,
        backoff_seconds: float = 2.0,
        max_bytes: Optional[int] = None,
        respect_robots: bool = True,
        session: Optional[requests.Session] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.user_agent = user_agent
        self.delay_seconds = delay_seconds
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.max_bytes = max_bytes
        self.respect_robots = respect_robots
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = user_agent
        self._sleep, self._clock = sleep, clock
        self._last_request = {}       # host -> monotonic time of the last request ()
        self._robots = {}             # origin -> RobotFileParser | None (allow all)

    # Check delay between requests to same host and update last request time (sleep) if needed
    def _wait_turn(self, host: str) -> None:
        last = self._last_request.get(host)
        if last is not None:
            remaining = self.delay_seconds - (self._clock() - last)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request[host] = self._clock()

    # Check whether the url is allowed by robots.txt
    def allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin not in self._robots:
            parser = None
            try:
                resp = self.session.get(f"{origin}/robots.txt", timeout=self.timeout)
                if resp.status_code == 200:
                    parser = RobotFileParser()
                    parser.parse(resp.text.splitlines())
            except requests.RequestException:
                parser = None          # robots unreachable, proceed politely without blocking the run
            self._robots[origin] = parser
        parser = self._robots[origin]
        return True if parser is None else parser.can_fetch(self.user_agent, url)

    # Access the url with retries, backoff and per host delay
    def _open(self, url: str, check_robots: Optional[bool] = None) -> requests.Response:
        """Streaming GET with robots check, per-host delay and retries. Caller closes the response."""
        if (self.respect_robots if check_robots is None else check_robots) and not self.allowed(url):
            raise FetchError(ReasonCode.ROBOTS_DISALLOWED, url)
        host = urlsplit(url).netloc
        last_error = FetchError(ReasonCode.HTTP_ERROR, "no attempt made")
        for attempt in range(self.max_retries + 1):
            if attempt:
                self._sleep(self.backoff_seconds * attempt)
            self._wait_turn(host)
            try:
                resp = self.session.get(url, stream=True, timeout=self.timeout)
            except requests.Timeout:
                last_error = FetchError(ReasonCode.TIMEOUT, url)
                continue
            except requests.RequestException as exc:
                last_error = FetchError(ReasonCode.HTTP_ERROR, f"{type(exc).__name__}: {exc}")
                continue
            if resp.status_code in _RETRYABLE_STATUS:
                resp.close()
                last_error = FetchError(ReasonCode.HTTP_ERROR, f"HTTP {resp.status_code}")
                continue
            if resp.status_code in (401, 403):
                resp.close()
                raise FetchError(ReasonCode.ACCESS_RESTRICTED, f"HTTP {resp.status_code}")
            if resp.status_code >= 400:
                resp.close()
                raise FetchError(ReasonCode.HTTP_ERROR, f"HTTP {resp.status_code}")
            return resp
        raise last_error

    # Read response data, decode and return as string (bytes -> str)
    def get_text(self, url: str, check_robots: Optional[bool] = None) -> str:
        with self._open(url, check_robots) as resp:
            data = self._read_capped(resp)
            encoding = resp.encoding or resp.apparent_encoding or "utf-8"
        return data.decode(encoding, errors="replace")

    # Download response data into a file in the scope directory (stream -> file)
    def download(self, url: str, scope: Scope, filename: Optional[str] = None,
                 check_robots: Optional[bool] = False) -> Path:
        """Stream url into the scope directory and return the file path (direct-file URLs skip robots by default)."""
        name = filename or Path(urlsplit(url).path).name or "download"
        target = scope.path_for(name)
        with self._open(url, check_robots) as resp:
            self._check_length(resp)
            written = 0
            with open(target, "wb") as out:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    written += len(chunk)
                    if self.max_bytes is not None and written > self.max_bytes:
                        out.close()
                        target.unlink(missing_ok=True)
                        raise FetchError(ReasonCode.CONTENT_TOO_LARGE, f"more than {self.max_bytes} bytes")
                    out.write(chunk)
        return target

    # Check response length against max_bytes
    def _check_length(self, resp: requests.Response) -> None:
        length = resp.headers.get("Content-Length")
        if self.max_bytes is not None and length and length.isdigit() and int(length) > self.max_bytes:
            raise FetchError(ReasonCode.CONTENT_TOO_LARGE, f"Content-Length {length} > {self.max_bytes}")

    # Read response data into a bytes object (stream -> bytes)
    def _read_capped(self, resp: requests.Response) -> bytes:
        self._check_length(resp)
        buf = bytearray()
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            buf.extend(chunk)
            if self.max_bytes is not None and len(buf) > self.max_bytes:
                raise FetchError(ReasonCode.CONTENT_TOO_LARGE, f"more than {self.max_bytes} bytes")
        return bytes(buf)
