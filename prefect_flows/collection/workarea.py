"""
Run-scoped working directory with per-candidate scope directories.

Directories (not files) are handed out because tools such as yt-dlp leave .part files and merge intermediates.
Ownership rule: the producer releases what it does not yield, the consumer releases what it receives.
"""
import hashlib
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional


MAX_NAME_BYTES = 200        # 255 bytes per name on Linux/ext4; keep headroom for a hash suffix


class WorkAreaFull(RuntimeError):
    pass


def default_work_root() -> Path:
    return Path(os.environ.get("WORK_DIR") or Path(tempfile.gettempdir()) / "speeches_work")


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


class Scope:
    """One candidate's private directory. Releasing is idempotent."""

    def __init__(self, path: Path):
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self.released = False

    def path_for(self, name: str) -> Path:
        name = Path(name).name                  # never escape the scope (when name is a path, take only the last component)
        if len(name.encode("utf-8")) > MAX_NAME_BYTES:      # for a file named after a long URL: keep the extension, cut the stem
            suffix = Path(name).suffix[:16]
            stem = name[: len(name) - len(suffix)].encode("utf-8")[: MAX_NAME_BYTES - len(suffix.encode("utf-8")) - 9].decode("utf-8", "ignore")
            name = f"{stem}_{hashlib.sha1(name.encode('utf-8')).hexdigest()[:8]}{suffix}"
        return self.path / Path(name).name      # never escape the scope

    def size(self) -> int:
        return _dir_size(self.path) if self.path.exists() else 0

    def release(self) -> None:
        if not self.released:
            shutil.rmtree(self.path, ignore_errors=True)
            self.released = True

    def __enter__(self) -> "Scope":
        return self

    def __exit__(self, *exc) -> None:
        self.release()


class WorkArea:
    """Run-scoped working directory with per-candidate scope directories."""
    def __init__(self, run_id: str, root: Optional[Path] = None, max_bytes: Optional[int] = None,
                 stale_hours: float = 24.0):
        self.run_id = str(run_id)
        self.root = Path(root) if root else default_work_root()
        self.run_dir = self.root / self.run_id
        self.max_bytes = max_bytes
        self.stale_hours = stale_hours
        self._counter = 0

    def __enter__(self) -> "WorkArea":
        self.root.mkdir(parents=True, exist_ok=True)
        self.sweep_stale()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, *exc) -> None:
        shutil.rmtree(self.run_dir, ignore_errors=True)   # crash, cancel or normal end

    def scope(self, label: str = "c") -> Scope:
        self._counter += 1
        return Scope(self.run_dir / f"{label}-{self._counter:06d}")

    def used_bytes(self) -> int:
        return _dir_size(self.run_dir) if self.run_dir.exists() else 0

    def check_budget(self) -> None:
        if self.max_bytes is not None and self.used_bytes() > self.max_bytes:
            raise WorkAreaFull(f"work area exceeded {self.max_bytes} bytes")

    def sweep_stale(self) -> list:
        """Remove run directories left behind by a hard-killed worker (untouched for stale_hours)."""
        removed = []
        cutoff = time.time() - self.stale_hours * 3600
        for child in self.root.iterdir() if self.root.exists() else []:
            if child.is_dir() and child != self.run_dir and child.stat().st_mtime < cutoff:
                shutil.rmtree(child, ignore_errors=True)
                removed.append(child.name)
        return removed
