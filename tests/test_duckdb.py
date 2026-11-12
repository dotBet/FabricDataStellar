from collections.abc import Iterator
from dataclasses import replace

import duckdb
import pytest

from data_stellar import Column, DataType, Dimension, Mart, Reference, Table
from data_stellar.duckdb import DataQualityError, DuckDBExecutor, ExecutionError, RunContext


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(":memory:") as connection:
        yield connection


def project() -> Dimension:
    return Dimension(
        name="gold.dim_project",
        columns=(Column("project_key", DataType.INTEGER), Column("project_id", DataType.TEXT)),
        grain=("project_key",),
        business_key=("project_id",),
        surrogate_key="project_key",
        query="SELECT 1 AS project_key, 'P100' AS project_id",
    )


def test_full_refresh_replays_without_duplicates_and_updates_type1(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    model = project()
    executor = DuckDBExecutor(connection)
    executor.run(Mart("example", (model,)), RunContext(batch_id="same-batch"))
    executor.run(Mart("example", (model,)), RunContext(batch_id="same-batch"))
    assert connection.execute("SELECT count(*) FROM gold.dim_project").fetchone() == (1,)
    changed = replace(model, query="SELECT 1 AS project_key, 'P200' AS project_id")
    executor.run(Mart("example", (changed,)))
    assert connection.execute("SELECT project_id FROM gold.dim_project").fetchone() == ("P200",)


def test_failed_rebuild_rolls_back_all_models_and_audits_failure(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    executor = DuckDBExecutor(connection)
    model = project()
    executor.run(Mart("example", (model,)))
    changed = replace(model, query="SELECT 2 AS project_key, 'P200' AS project_id")
    bad = Table(
        name="gold.broken",
        columns=(Column("id", DataType.INTEGER),),
        grain=("id",),
        query="SELECT 1 AS id UNION ALL SELECT 1",
        depends_on=(model.name,),
    )
    with pytest.raises(DataQualityError, match="duplicate key"):
        executor.run(Mart("example", (changed, bad)))
    assert connection.execute("SELECT * FROM gold.dim_project").fetchall() == [(1, "P100")]
    assert connection.execute(
        "SELECT status, error IS NOT NULL FROM _stellar.runs ORDER BY started_at"
    ).fetchall() == [("succeeded", False), ("failed", True)]
    assert connection.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'broken'"
    ).fetchone() == (0,)
    executor.run(Mart("example", (model,)))


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("SELECT 1 AS wrong", "query columns"),
        (
            "SELECT 1 AS project_key, 'P100' AS project_id UNION ALL SELECT 2, 'P100'",
            "duplicate key",
        ),
    ],
)
def test_invalid_shape_and_duplicate_business_key_fail(
    connection: duckdb.DuckDBPyConnection, query: str, message: str
) -> None:
    with pytest.raises(DataQualityError, match=message):
        DuckDBExecutor(connection).run(Mart("example", (replace(project(), query=query),)))


def test_null_required_column_is_an_explicit_failure(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    model = replace(project(), query="SELECT 1 AS project_key, NULL AS project_id")
    with pytest.raises(ExecutionError, match="gold.dim_project"):
        DuckDBExecutor(connection).run(Mart("example", (model,)))


def test_orphan_reference_fails(connection: duckdb.DuckDBPyConnection) -> None:
    child = Table(
        name="silver.child",
        columns=(Column("id", DataType.INTEGER),),
        grain=("id",),
        query="SELECT 2 AS id",
        depends_on=("gold.dim_project",),
        references=(Reference(("id",), "gold.dim_project", ("project_key",)),),
    )
    with pytest.raises(DataQualityError, match="orphan reference"):
        DuckDBExecutor(connection).run(Mart("example", (project(), child)))


@pytest.mark.parametrize(
    ("left", "right", "valid"),
    [("NULL", "NULL", True), ("1", "NULL", False), ("1", "'A'", True)],
)
def test_optional_composite_reference_requires_all_or_no_components(
    connection: duckdb.DuckDBPyConnection, left: str, right: str, valid: bool
) -> None:
    parent = Table(
        name="silver.parent",
        columns=(Column("a", DataType.INTEGER), Column("b", DataType.TEXT)),
        grain=("a", "b"),
        query="SELECT 1 AS a, 'A' AS b",
    )
    child = Table(
        name="silver.child",
        columns=(
            Column("id", DataType.INTEGER),
            Column("a", DataType.INTEGER, nullable=True),
            Column("b", DataType.TEXT, nullable=True),
        ),
        grain=("id",),
        query=f"SELECT 1 AS id, {left} AS a, {right} AS b",
        depends_on=(parent.name,),
        references=(Reference(("a", "b"), parent.name, ("a", "b")),),
    )
    executor = DuckDBExecutor(connection)
    if valid:
        executor.run(Mart("example", (parent, child)))
    else:
        with pytest.raises(DataQualityError, match="orphan reference"):
            executor.run(Mart("example", (parent, child)))
