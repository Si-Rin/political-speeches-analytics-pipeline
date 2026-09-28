"""
ContentHandle: defines the data obtaining and clean up.

- LocalPathHandle : returns the user's path and releases nothing (the file is never ours)
- TempFileHandle : returns an already-downloaded file and releases the scope that owns it
- HttpHandle : downloads into the caller's scope and releases nothing (the scope is the flow's)
- StagingObjectHandle : downloads the MinIO staging object and deletes the staging object
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from prefect_flows.collection.workarea import Scope


@dataclass
class MaterializeContext:
    scope: Scope
    fetcher: Any = None


class ContentHandle(ABC):
    @abstractmethod
    def materialize(self, ctx: MaterializeContext) -> Path:
        """Return a local path holding the bytes."""

    def release(self) -> None:
        """Free whatever this handle owns. Idempotent."""


class LocalPathHandle(ContentHandle):
    def __init__(self, path):
        self.path = Path(path)

    def materialize(self, ctx: MaterializeContext) -> Path:
        if not self.path.is_file():
            raise FileNotFoundError(f"Local file not found: {self.path}")
        return self.path


class TempFileHandle(ContentHandle):
    """A file a source already downloaded (page fetched for matching, YouTube download). Owns its scope, if any."""

    def __init__(self, path, scope: Optional[Scope] = None):
        self.path = Path(path)
        self.scope = scope

    def materialize(self, ctx: MaterializeContext) -> Path:
        if not self.path.is_file():
            raise FileNotFoundError(f"Downloaded file missing: {self.path}")
        return self.path

    def release(self) -> None:
        if self.scope is not None:
            self.scope.release() # if there was a scope, we don't own the file and just release the scope (delete the temporary directory)
        else:
            self.path.unlink(missing_ok=True) # if there was no scope, we own the file and delete it


class HttpHandle(ContentHandle):
    def __init__(self, url: str, filename: Optional[str] = None, check_robots: bool = False):
        self.url, self.filename, self.check_robots = url, filename, check_robots

    def materialize(self, ctx: MaterializeContext) -> Path:
        if ctx.fetcher is None:
            raise RuntimeError("HttpHandle needs a fetcher in the MaterializeContext")
        return ctx.fetcher.download(self.url, ctx.scope, self.filename, check_robots=self.check_robots)


class StagingObjectHandle(ContentHandle):
    """A user upload parked in the MinIO staging bucket by the backend; deleted once Bronze has it (or the run ends)."""

    def __init__(self, client, key: str, bucket: str = "staging"):
        self.client, self.key, self.bucket = client, key, bucket
        self._released = False

    def materialize(self, ctx: MaterializeContext) -> Path:
        target = ctx.scope.path_for(self.key)
        self.client.fget_object(self.bucket, self.key, str(target))
        return target

    def release(self) -> None:
        if not self._released:
            self._released = True
            try:
                self.client.remove_object(self.bucket, self.key)
            except Exception:
                pass    # the staging bucket's expiry rule is the backstop
