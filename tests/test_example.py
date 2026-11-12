import shutil
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from data_stellar.duckdb import DataQualityError, DuckDBExecutor, ExecutionError
from data_stellar.example import build_project_accounting_mart

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "project_accounting"


@pytest.fixture
def sources(tmp_path: Path) -> Path:
    destination = tmp_path / "sources"
    shutil.copytree(EXAMPLE, destination)
    return destination


def test_example_reconciles_and_replay_is_persistent(tmp_path: Path, sources: Path) -> None:
    database = tmp_path / "mart.duckdb"
    mart = build_project_accounting_mart(sources)
    with duckdb.connect(str(database)) as connection:
        executor = DuckDBExecutor(connection)
        first = executor.run(mart)
        second = executor.run(mart)
        assert first.row_counts == second.row_counts
        assert first.run_id != second.run_id
        assert connection.execute("""
            SELECT project_id, sum(hours) FROM gold.fact_time_entry
            JOIN gold.dim_project USING (project_key) GROUP BY project_id ORDER BY project_id
        """).fetchall() == [("P100", Decimal("12.50")), ("P200", Decimal("5.00"))]
        assert connection.execute("""
            SELECT project_id, currency, actual_cost FROM gold.project_summary ORDER BY project_id
        """).fetchall() == [
            ("P100", "USD", Decimal("800.00")),
            ("P200", "USD", Decimal("400.00")),
        ]
        assert connection.execute("""
            SELECT sum(debit), sum(credit), sum(signed_amount) FROM gold.fact_journal_line
        """).fetchone() == (Decimal("1200.00"), Decimal("1200.00"), Decimal("0.00"))
        assert connection.execute("""
            SELECT count(*) FROM gold.allocation_coverage
            WHERE original_amount <> allocated_amount
        """).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(DISTINCT batch_id) FROM bronze.projects"
        ).fetchone() == (1,)
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM gold.fact_time_entry").fetchone() == (3,)
        assert connection.execute(
            "SELECT count(*) FROM _stellar.runs WHERE status = 'succeeded'"
        ).fetchone() == (2,)


@pytest.mark.parametrize(
    ("file", "before", "after", "error", "message"),
    [
        ("allocations.csv", "0.60", "0.50", DataQualityError, "total one"),
        ("allocations.csv", "0.60", "-0.60", DataQualityError, "between zero and one"),
        ("time_entries.csv", "T1,P100", "T1,MISSING", DataQualityError, "orphan reference"),
        ("time_entries.csv", "8.00", "8.001", ExecutionError, "Invalid decimal precision"),
        (
            "journal_lines.csv",
            "J1000,1,",
            "J1000,1.1,",
            ExecutionError,
            "Invalid journal line number",
        ),
        ("journal_lines.csv", "1000.00,0.00", "999.00,0.00", DataQualityError, "balanced_journal"),
    ],
)
def test_invalid_sources_do_not_replace_published_data(
    sources: Path, file: str, before: str, after: str, error: type[Exception], message: str
) -> None:
    with duckdb.connect(":memory:") as connection:
        executor = DuckDBExecutor(connection)
        executor.run(build_project_accounting_mart(sources))
        path = sources / file
        path.write_text(path.read_text().replace(before, after), encoding="utf-8")
        with pytest.raises(error, match=message):
            executor.run(build_project_accounting_mart(sources))
        assert connection.execute(
            "SELECT sum(actual_cost) FROM gold.project_summary"
        ).fetchone() == (Decimal("1200.00"),)
        assert connection.execute(
            "SELECT status FROM _stellar.runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone() == ("failed",)


def test_missing_allocation_is_not_silently_excluded(sources: Path) -> None:
    path = sources / "allocations.csv"
    lines = path.read_text().splitlines()
    path.write_text(
        "\n".join(line for line in lines if "J1001,1" not in line) + "\n", encoding="utf-8"
    )
    with duckdb.connect(":memory:") as connection:
        with pytest.raises(DataQualityError, match="every_line_requires_allocation"):
            DuckDBExecutor(connection).run(build_project_accounting_mart(sources))


def test_fractional_cent_allocations_reconcile_without_negative_expenses(sources: Path) -> None:
    path = sources / "journal_lines.csv"
    path.write_text(path.read_text().replace("1000.00", "0.01"), encoding="utf-8")
    path = sources / "allocations.csv"
    path.write_text(
        path.read_text().replace("0.60", "0.50").replace("0.40", "0.50"), encoding="utf-8"
    )
    with duckdb.connect(":memory:") as connection:
        DuckDBExecutor(connection).run(build_project_accounting_mart(sources))
        assert connection.execute("""
            SELECT project_id, actual_cost FROM gold.project_summary ORDER BY project_id
        """).fetchall() == [("P100", Decimal("200.01")), ("P200", Decimal("0.00"))]
        assert connection.execute("""
            SELECT sum(allocated_amount), min(allocated_amount)
            FROM gold.bridge_journal_project WHERE journal_id = 'J1000' AND line_number = 1
        """).fetchone() == (Decimal("0.01"), Decimal("0.00"))


def test_currency_and_entity_totals_are_not_combined(sources: Path) -> None:
    for name in ("journal_lines", "allocations"):
        path = sources / f"{name}.csv"
        original = path.read_text()
        extra = "\n".join(original.splitlines()[1:]).replace("LEG1", "LEG2")
        if name == "journal_lines":
            extra = extra.replace("USD", "EUR")
        path.write_text(original.rstrip() + "\n" + extra + "\n", encoding="utf-8")
    with duckdb.connect(":memory:") as connection:
        DuckDBExecutor(connection).run(build_project_accounting_mart(sources))
        assert connection.execute("""
            SELECT entity_id, currency, sum(actual_cost) FROM gold.project_summary
            GROUP BY entity_id, currency ORDER BY entity_id
        """).fetchall() == [
            ("LEG1", "USD", Decimal("1200.00")),
            ("LEG2", "EUR", Decimal("1200.00")),
        ]
