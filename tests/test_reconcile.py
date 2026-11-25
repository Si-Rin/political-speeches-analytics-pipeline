from datetime import datetime, timedelta, timezone

from backend.reconcile import QUEUE_GRACE, decide
from backend.schemas import RunStatus

NOW = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
RECENT, OLD = NOW - timedelta(minutes=1), NOW - QUEUE_GRACE - timedelta(minutes=1)


def test_terminal_runs_are_never_touched():
    assert decide(RunStatus.COMPLETED, "p", "FAILED", OLD, NOW) is None


def test_running_but_prefect_crashed_or_failed_becomes_failed():
    for state in ("CRASHED", "FAILED", "crashed"):
        status, message = decide(RunStatus.RUNNING, "p", state, RECENT, NOW)
        assert status == RunStatus.FAILED and state.upper() in message


def test_prefect_cancelled_becomes_cancelled():
    assert decide(RunStatus.RUNNING, "p", "CANCELLED", RECENT, NOW)[0] == RunStatus.CANCELLED
    assert decide(RunStatus.QUEUED, "p", "CANCELLING", RECENT, NOW)[0] == RunStatus.CANCELLED


def test_flow_completed_but_row_not_final_is_a_failure():
    assert decide(RunStatus.RUNNING, "p", "COMPLETED", RECENT, NOW)[0] == RunStatus.FAILED


def test_healthy_or_unknown_states_change_nothing():
    for state in ("RUNNING", "PENDING", "SCHEDULED", None):
        assert decide(RunStatus.RUNNING, "p", state, OLD, NOW) is None


def test_lost_submission_fails_only_after_the_grace_period():
    assert decide(RunStatus.QUEUED, None, None, RECENT, NOW) is None
    assert decide(RunStatus.QUEUED, None, None, OLD, NOW)[0] == RunStatus.FAILED
