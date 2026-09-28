"""Run lifecycle: running -> completed / completed_with_errors / failed / cancelled."""
from contextlib import contextmanager
from typing import Callable, Optional

from prefect_flows.collection.enums import RunStatus
from prefect_flows.collection.recorder import DiscoveryRecorder


@contextmanager
def track(repo, run_id: str, recorder: DiscoveryRecorder, pages_visited: Optional[Callable[[], int]] = None):
    """
    Mark the run running, then finalize it from what actually happened:
    unhandled exception -> failed
    Ctrl-C/cancel -> cancelled
    any fetch_failed/ingest_failed -> completed_with_errors
    """
    repo.mark_running(run_id)
    pages = lambda: pages_visited() if pages_visited else 0   # noqa: E731
    try:
        yield
    except Exception as exc:
        repo.finalize_run(run_id, RunStatus.FAILED, f"{type(exc).__name__}: {exc}", pages())
        raise
    except BaseException:
        repo.finalize_run(run_id, RunStatus.CANCELLED, "cancelled", pages())
        raise
    else:
        status = RunStatus.COMPLETED_WITH_ERRORS if recorder.error_count else RunStatus.COMPLETED
        repo.finalize_run(run_id, status, None, pages())
