"""The Python enums and the SQL CHECK constraints must list exactly the same values."""
import re
from pathlib import Path

import pytest

from prefect_flows.collection.enums import CandidateStatus, MatchScope, Mode, ReasonCode, RunStatus

SQL = (Path(__file__).resolve().parent.parent / "init_scripts/postgres/updates.sql").read_text()


def check_values(column: str, table_marker: str) -> set:
    """Values of "<column> ... CHECK (<column> IN (...))" inside the CREATE TABLE that mentions table_marker."""
    block = SQL[SQL.index(table_marker):]
    m = re.search(rf"{column}\s+TEXT[^,]*?CHECK\s*\(\s*(?:{column}\s+IN\s*)?\(([^)]*)\)", block, re.S)
    assert m, f"no CHECK found for {column}"
    return set(re.findall(r"'([^']+)'", m.group(1)))


@pytest.mark.parametrize("column, marker, enum", [
    ("mode", "collection.runs (", Mode),
    ("status", "collection.runs (", RunStatus),
    ("status", "bronze.discovery_log (", CandidateStatus),
    ("reason_code", "bronze.discovery_log (", ReasonCode),
    ("match_scope", "bronze.discovery_log (", MatchScope),
])
def test_enum_matches_sql(column, marker, enum):
    assert check_values(column, marker) == {e.value for e in enum}
