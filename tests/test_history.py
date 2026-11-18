from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from data_stellar import (
    SCD2,
    Column,
    DataType,
    Dimension,
    Fact,
    Incremental,
    Mart,
    Measure,
    ModelError,
    Reference,
    TemporalLookup,
)
from data_stellar.duckdb import DataQualityError, DuckDBExecutor
from data_stellar.history_example import build_project_history_mart


def moment(month: int, day: int = 1) -> datetime:
    return datetime(2026, month, day, tzinfo=UTC)


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(":memory:") as connection:
        connection.execute("""
            CREATE TABLE project_changes (
                project_id VARCHAR, label VARCHAR, valid_from TIMESTAMPTZ, updated_at TIMESTAMPTZ
            );
            CREATE TABLE time_changes (
                entry_id VARCHAR, project_id VARCHAR, work_at TIMESTAMPTZ,
                hours DECIMAL(18, 2), updated_at TIMESTAMPTZ
            );
        """)
        yield connection


def mart() -> Mart:
    project = Dimension(
        name="gold.dim_project",
        columns=(
            Column("project_key", DataType.TEXT),
            Column("project_id", DataType.TEXT),
            Column("label", DataType.TEXT, nullable=True),
            Column("valid_from", DataType.TIMESTAMP),
            Column("valid_to", DataType.TIMESTAMP, nullable=True),
            Column("is_current", DataType.BOOLEAN),
            Column("updated_at", DataType.TIMESTAMP),
        ),
        grain=("project_key",),
        business_key=("project_id",),
        surrogate_key="project_key",
        history=SCD2(("label",)),
        incremental=Incremental("updated_at"),
        query="SELECT * FROM project_changes",
    )
    fact = Fact(
        name="gold.fact_time",
        columns=(
            Column("entry_id", DataType.TEXT),
            Column("project_id", DataType.TEXT),
            Column("project_key", DataType.TEXT),
            Column("work_at", DataType.TIMESTAMP),
            Column("hours", DataType.AMOUNT),
            Column("updated_at", DataType.TIMESTAMP),
        ),
        grain=("entry_id",),
        incremental=Incremental("updated_at"),
        depends_on=(project.name,),
        measures=(Measure("hours"),),
        temporal_lookups=(TemporalLookup(("project_id",), project.name, "work_at", "project_key"),),
        query="SELECT * FROM time_changes",
    )
    return Mart("history", (project, fact))


def seed(connection: duckdb.DuckDBPyConnection) -> None:
    connection.executemany(
        "INSERT INTO project_changes VALUES ('P1', ?, ?, ?)",
        [("A", moment(1), moment(4)), ("C", moment(3), moment(4))],
    )
    connection.executemany(
        "INSERT INTO time_changes VALUES (?, 'P1', ?, 1.00, ?)",
        [("T1", moment(2, 15), moment(4)), ("T2", moment(3), moment(4))],
    )


def clear_batch(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("DELETE FROM project_changes; DELETE FROM time_changes")


def assignments(connection: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return connection.execute("""
        SELECT entry_id, label FROM gold.fact_time
        JOIN gold.dim_project USING (project_key) ORDER BY entry_id
    """).fetchall()


def test_backdated_split_repairs_retained_facts_and_replay_preserves_keys(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    old_keys = connection.execute(
        "SELECT project_key FROM gold.dim_project ORDER BY valid_from"
    ).fetchall()
    clear_batch(connection)
    connection.execute(
        "INSERT INTO project_changes VALUES ('P1', 'B', ?, ?)", [moment(2), moment(4, 2)]
    )
    executor.run(mart())
    assert assignments(connection) == [("T1", "B"), ("T2", "C")]
    versions = connection.execute("""
        SELECT label, valid_from, valid_to, is_current FROM gold.dim_project ORDER BY valid_from
    """).fetchall()
    assert versions == [
        ("A", moment(1), moment(2), False),
        ("B", moment(2), moment(3), False),
        ("C", moment(3), None, True),
    ]
    assert (
        connection.execute(
            "SELECT project_key FROM gold.dim_project WHERE label IN ('A', 'C') ORDER BY valid_from"
        ).fetchall()
        == old_keys
    )
    before = connection.execute("SELECT * FROM gold.dim_project ORDER BY valid_from").fetchall()
    executor.run(mart())
    assert (
        connection.execute("SELECT * FROM gold.dim_project ORDER BY valid_from").fetchall()
        == before
    )


def test_noop_assertions_are_retained_for_future_backdated_splits(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    connection.execute("UPDATE project_changes SET label = 'A'")
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    assert connection.execute("SELECT count(*) FROM gold.dim_project").fetchone() == (1,)
    clear_batch(connection)
    connection.execute(
        "INSERT INTO project_changes VALUES ('P1', 'B', ?, ?)", [moment(2), moment(4, 2)]
    )
    executor.run(mart())
    assert assignments(connection) == [("T1", "B"), ("T2", "A")]
    assert connection.execute("SELECT count(*) FROM gold.dim_project").fetchone() == (3,)


def test_missing_temporal_version_rolls_back_watermarks_and_can_retry(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    before = connection.execute(
        "SELECT model_name, watermark FROM _stellar.model_state ORDER BY model_name"
    ).fetchall()
    clear_batch(connection)
    connection.execute(
        "INSERT INTO project_changes VALUES ('P1', 'B', ?, ?)", [moment(2), moment(4, 2)]
    )
    connection.execute(
        "INSERT INTO time_changes VALUES ('T3', 'MISSING', ?, 2.00, ?)",
        [moment(1), moment(4, 2)],
    )
    with pytest.raises(DataQualityError, match="missing temporal version"):
        executor.run(mart())
    assert assignments(connection) == [("T1", "A"), ("T2", "C")]
    assert (
        connection.execute(
            "SELECT model_name, watermark FROM _stellar.model_state ORDER BY model_name"
        ).fetchall()
        == before
    )
    connection.execute(
        "INSERT INTO project_changes VALUES ('MISSING', 'New project', ?, ?)",
        [moment(1), moment(4, 2)],
    )
    executor.run(mart())
    assert assignments(connection) == [("T1", "B"), ("T2", "C"), ("T3", "New project")]


def test_fact_before_earliest_history_fails(connection: duckdb.DuckDBPyConnection) -> None:
    seed(connection)
    connection.execute("UPDATE time_changes SET work_at = '2025-12-31T23:59:59Z'")
    with pytest.raises(DataQualityError, match="missing temporal version"):
        DuckDBExecutor(connection).run(mart())
    assert connection.execute("SELECT count(*) FROM _stellar.model_state").fetchone() == (0,)


def test_late_facts_updates_equal_cursor_ties_and_empty_batches(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    clear_batch(connection)
    connection.execute(
        "INSERT INTO time_changes VALUES ('T3', 'P1', ?, 2.50, ?)", [moment(1), moment(4)]
    )
    executor.run(mart())
    connection.execute(
        "UPDATE time_changes SET entry_id = 'T1', hours = 7, updated_at = ?", [moment(4, 2)]
    )
    executor.run(mart())
    assert assignments(connection) == [("T1", "A"), ("T2", "C"), ("T3", "A")]
    assert connection.execute("SELECT sum(hours) FROM gold.fact_time").fetchone() == (
        Decimal("10.50"),
    )
    clear_batch(connection)
    result = executor.run(mart())
    assert result.row_counts == {"gold.dim_project": 2, "gold.fact_time": 3}


def test_watermark_filters_old_arrival_not_old_effective_date(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    clear_batch(connection)
    connection.execute(
        "INSERT INTO project_changes VALUES ('P1', 'ignored', ?, ?)",
        [moment(2), moment(3, 31)],
    )
    executor.run(mart())
    assert assignments(connection) == [("T1", "A"), ("T2", "C")]
    connection.execute("UPDATE project_changes SET updated_at = ?", [moment(4, 2)])
    executor.run(mart())
    assert assignments(connection) == [("T1", "ignored"), ("T2", "C")]


def test_same_cursor_conflicts_and_duplicate_events_fail(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    connection.execute("UPDATE project_changes SET label = 'conflict' WHERE label = 'A'")
    with pytest.raises(DataQualityError, match="conflicting payload"):
        executor.run(mart())
    clear_batch(connection)
    connection.execute("""
        INSERT INTO project_changes VALUES
          ('P1', 'B', '2026-02-01T00:00:00Z'::TIMESTAMPTZ, '2026-04-02T00:00:00Z'::TIMESTAMPTZ),
          ('P1', 'B', '2026-02-01T00:00:00Z'::TIMESTAMPTZ, '2026-04-03T00:00:00Z'::TIMESTAMPTZ)
    """)
    with pytest.raises(DataQualityError, match="duplicate incremental key"):
        executor.run(mart())


def test_same_effective_time_correction_and_nullable_attribute_changes(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    original_key = connection.execute(
        "SELECT project_key FROM gold.dim_project WHERE label = 'A'"
    ).fetchone()
    clear_batch(connection)
    connection.execute(
        "INSERT INTO project_changes VALUES ('P1', NULL, ?, ?)", [moment(1), moment(4, 2)]
    )
    executor.run(mart())
    assert (
        connection.execute(
            "SELECT project_key FROM gold.dim_project WHERE label IS NULL"
        ).fetchone()
        == original_key
    )
    assert connection.execute("SELECT count(*) FROM gold.dim_project").fetchone() == (2,)


def test_contract_changes_require_explicit_migration(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    executor = DuckDBExecutor(connection)
    executor.run(mart())
    with pytest.raises(DataQualityError, match="contract/owner"):
        executor.run(replace(mart(), name="other_owner"))
    project, fact = mart().models
    with pytest.raises(DataQualityError, match="switch a stateful model"):
        executor.run(Mart("history", (project, replace(fact, incremental=None))))


@pytest.mark.parametrize("column", ["updated_at", "valid_from", "work_at"])
def test_infinite_timestamps_fail(connection: duckdb.DuckDBPyConnection, column: str) -> None:
    seed(connection)
    table = "time_changes" if column == "work_at" else "project_changes"
    connection.execute(f"UPDATE {table} SET {column} = 'infinity'::TIMESTAMPTZ")
    with pytest.raises(DataQualityError, match="must be finite"):
        DuckDBExecutor(connection).run(mart())


def test_history_contracts_reject_ambiguous_references_and_invalid_columns() -> None:
    project, fact = mart().models
    assert isinstance(project, Dimension)
    assert isinstance(fact, Fact)
    with pytest.raises(ModelError, match="requires an incremental"):
        replace(project, incremental=None)
    with pytest.raises(ModelError, match="distinct and cover"):
        replace(project, history=SCD2(("project_id",)))
    with pytest.raises(ModelError, match="non-null TIMESTAMP"):
        replace(fact, incremental=Incremental("entry_id"))
    with pytest.raises(ModelError, match="declared unique key"):
        Mart(
            "bad",
            (
                project,
                replace(
                    fact, references=(Reference(("project_id",), project.name, ("project_id",)),)
                ),
            ),
        ).plan()
    with pytest.raises(ModelError, match="separate column"):
        replace(
            fact,
            temporal_lookups=(
                TemporalLookup(("project_id",), project.name, "work_at", "entry_id"),
            ),
        )


def test_empty_first_batch_is_valid_and_does_not_invent_watermarks(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    executor = DuckDBExecutor(connection)
    assert executor.run(mart()).row_counts == {"gold.dim_project": 0, "gold.fact_time": 0}
    assert connection.execute(
        "SELECT count(*) FROM _stellar.model_state WHERE watermark IS NOT NULL"
    ).fetchone() == (0,)
    seed(connection)
    executor.run(mart())
    assert assignments(connection) == [("T1", "A"), ("T2", "C")]


def test_temporal_date_is_utc_and_surrogates_are_timezone_independent(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    project, fact = mart().models
    assert isinstance(fact, Fact)
    date_fact = replace(
        fact,
        columns=tuple(
            replace(c, data_type=DataType.DATE) if c.name == "work_at" else c for c in fact.columns
        ),
        query="""SELECT entry_id, project_id,
                        CAST(work_at AT TIME ZONE 'UTC' AS DATE) AS work_at, hours, updated_at
                 FROM time_changes""",
    )
    date_mart = Mart("history", (project, date_fact))
    executor = DuckDBExecutor(connection)
    executor.run(date_mart)
    keys = connection.execute(
        "SELECT project_key FROM gold.dim_project ORDER BY valid_from"
    ).fetchall()
    connection.execute("SET TimeZone = 'Asia/Tokyo'")
    executor.run(date_mart)
    assert assignments(connection) == [("T1", "A"), ("T2", "C")]
    assert (
        connection.execute(
            "SELECT project_key FROM gold.dim_project ORDER BY valid_from"
        ).fetchall()
        == keys
    )


def test_multiple_temporal_roles_resolve_independently(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    project, fact = mart().models
    assert isinstance(fact, Fact)
    fact = replace(
        fact,
        columns=(
            *fact.columns,
            Column("approval_at", DataType.TIMESTAMP),
            Column("approval_project_key", DataType.TEXT),
        ),
        temporal_lookups=(
            *fact.temporal_lookups,
            TemporalLookup(("project_id",), project.name, "approval_at", "approval_project_key"),
        ),
        query="SELECT *, '2026-03-01T00:00:00Z'::TIMESTAMPTZ AS approval_at FROM time_changes",
    )
    DuckDBExecutor(connection).run(Mart("history", (project, fact)))
    assert connection.execute("""
        SELECT entry_id, work.label, approval.label FROM gold.fact_time
        JOIN gold.dim_project work ON work.project_key = fact_time.project_key
        JOIN gold.dim_project approval ON approval.project_key = fact_time.approval_project_key
        ORDER BY entry_id
    """).fetchall() == [("T1", "A", "C"), ("T2", "C", "C")]


def test_composite_business_keys_isolate_history_and_fact_resolution(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    project, fact = mart().models
    assert isinstance(project, Dimension) and isinstance(fact, Fact)
    project = replace(
        project,
        columns=(*project.columns, Column("source_system", DataType.TEXT)),
        business_key=("source_system", "project_id"),
        query="""SELECT *, 'erp_a' AS source_system FROM project_changes
                 UNION ALL
                 SELECT project_id, 'Other entity' AS label, valid_from, updated_at,
                        'erp_b' AS source_system FROM project_changes""",
    )
    fact = replace(
        fact,
        columns=(*fact.columns, Column("source_system", DataType.TEXT)),
        temporal_lookups=(
            TemporalLookup(("source_system", "project_id"), project.name, "work_at", "project_key"),
        ),
        query="""SELECT entry_id, project_id, work_at, hours, updated_at,
                        CASE WHEN entry_id = 'T1' THEN 'erp_a' ELSE 'erp_b' END AS source_system
                 FROM time_changes""",
    )
    executor = DuckDBExecutor(connection)
    composite = Mart("history", (project, fact))
    executor.run(composite)
    assert assignments(connection) == [("T1", "A"), ("T2", "Other entity")]
    assert connection.execute(
        "SELECT count(DISTINCT project_key) FROM gold.dim_project"
    ).fetchone() == (3,)
    executor.run(composite)
    assert assignments(connection) == [("T1", "A"), ("T2", "Other entity")]


def test_temporal_lookup_uses_exact_half_open_boundaries(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    seed(connection)
    connection.execute("""
        UPDATE time_changes SET work_at = CASE WHEN entry_id = 'T1'
          THEN '2026-02-28T23:59:59.999999Z'::TIMESTAMPTZ
          ELSE '2026-03-01T00:00:00Z'::TIMESTAMPTZ END
    """)
    DuckDBExecutor(connection).run(mart())
    assert assignments(connection) == [("T1", "A"), ("T2", "C")]


def test_history_example_is_persistent_and_replayable(tmp_path: Path) -> None:
    sources = Path(__file__).resolve().parents[1] / "examples" / "project_history"
    database = tmp_path / "history.duckdb"
    with duckdb.connect(str(database)) as connection:
        executor = DuckDBExecutor(connection)
        executor.run(build_project_history_mart(sources / "initial"))
        executor.run(build_project_history_mart(sources / "backdated"))
    with duckdb.connect(str(database)) as connection:
        before = connection.execute(
            "SELECT * FROM gold.fact_project_time ORDER BY entry_id"
        ).fetchall()
        executor = DuckDBExecutor(connection)
        executor.run(build_project_history_mart(sources / "initial"))
        executor.run(build_project_history_mart(sources / "backdated"))
        assert (
            connection.execute("SELECT * FROM gold.fact_project_time ORDER BY entry_id").fetchall()
            == before
        )
        assert connection.execute("""
            SELECT entry_id, project_name, hours FROM gold.fact_project_time
            JOIN gold.dim_project_history USING (project_key) ORDER BY entry_id
        """).fetchall() == [
            ("T1", "Design", Decimal("8.00")),
            ("T2", "Delivery", Decimal("5.00")),
            ("T3", "Discovery", Decimal("2.50")),
        ]
