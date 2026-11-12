from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

import duckdb

from data_stellar.models import Bridge, Dimension, Mart, Table, identifier, qualified_name


class DataQualityError(RuntimeError):
    """Materialized data violates a model contract."""


class ExecutionError(RuntimeError):
    """DuckDB could not execute a model."""


def quote(name: str) -> str:
    return f'"{identifier(name)}"'


def relation(name: str) -> str:
    return ".".join(quote(part) for part in qualified_name(name))


@dataclass(frozen=True)
class RunContext:
    batch_id: str = field(default_factory=lambda: str(uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")


@dataclass(frozen=True)
class RunResult:
    run_id: str
    batch_id: str
    row_counts: dict[str, int]


class DuckDBExecutor:
    """Atomically rebuild a mart from a complete source snapshot."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.connection = connection

    def run(self, mart: Mart, context: RunContext | None = None) -> RunResult:
        plan = mart.plan()
        context = context or RunContext()
        run_id = str(uuid4())
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS _stellar")
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS _stellar.runs (
                run_id VARCHAR PRIMARY KEY,
                batch_id VARCHAR NOT NULL,
                mart_name VARCHAR NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                status VARCHAR NOT NULL,
                error VARCHAR
            )
        """)
        self.connection.execute(
            "INSERT INTO _stellar.runs VALUES (?, ?, ?, ?, NULL, 'running', NULL)",
            [run_id, context.batch_id, mart.name, context.started_at],
        )
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute("""
                CREATE OR REPLACE TEMP TABLE _stellar_context (
                    batch_id VARCHAR, loaded_at TIMESTAMPTZ
                )
            """)
            self.connection.execute(
                "INSERT INTO _stellar_context VALUES (?, ?)",
                [context.batch_id, context.started_at],
            )
            counts = {model.name: self._materialize(model) for model in plan}
            self.connection.execute(
                """UPDATE _stellar.runs
                   SET status = 'succeeded', completed_at = current_timestamp
                   WHERE run_id = ?""",
                [run_id],
            )
            self.connection.execute("DROP TABLE _stellar_context")
            self.connection.execute("COMMIT")
        except Exception as error:
            self.connection.execute("ROLLBACK")
            self.connection.execute(
                """UPDATE _stellar.runs
                   SET status = 'failed', completed_at = current_timestamp, error = ?
                   WHERE run_id = ?""",
                [str(error), run_id],
            )
            raise
        return RunResult(run_id, context.batch_id, counts)

    def _count(self, query: str) -> int:
        row = self.connection.execute(query).fetchone()
        if row is None:
            raise ExecutionError("Expected a count result")
        return int(row[0])

    def _assert_empty(self, model: Table, rule: str, query: str) -> None:
        failures = self._count(f"SELECT count(*) FROM ({query}) AS violations")
        if failures:
            raise DataQualityError(f"{model.name}: {rule} ({failures} violations)")

    def _materialize(self, model: Table) -> int:
        try:
            self.connection.execute(f"CREATE OR REPLACE TEMP TABLE _stellar_stage AS {model.query}")
            actual = tuple(
                row[1]
                for row in self.connection.execute("PRAGMA table_info('_stellar_stage')").fetchall()
            )
            expected = tuple(column.name for column in model.columns)
            if set(actual) != set(expected):
                raise DataQualityError(
                    f"{model.name}: query columns {actual} do not match declared columns {expected}"
                )
            schema, _ = qualified_name(model.name)
            self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(schema)}")
            definitions = ", ".join(
                f"{quote(column.name)} {column.data_type.value}"
                f"{'' if column.nullable else ' NOT NULL'}"
                for column in model.columns
            )
            target = relation(model.name)
            self.connection.execute(f"CREATE OR REPLACE TABLE {target} ({definitions})")
            columns = ", ".join(quote(column.name) for column in model.columns)
            self.connection.execute(f"INSERT INTO {target} SELECT {columns} FROM _stellar_stage")
            self.connection.execute("DROP TABLE _stellar_stage")
            self._validate(model)
            return self._count(f"SELECT count(*) FROM {target}")
        except duckdb.Error as error:
            raise ExecutionError(f"{model.name}: {error}") from error

    def _validate(self, model: Table) -> None:
        target = relation(model.name)
        keys = [model.grain]
        if isinstance(model, Dimension) and model.business_key not in keys:
            keys.append(model.business_key)
        for key in keys:
            columns = ", ".join(quote(name) for name in key)
            self._assert_empty(
                model,
                f"duplicate key {key}",
                f"SELECT {columns} FROM {target} GROUP BY {columns} HAVING count(*) > 1",
            )
        for check in model.checks:
            self._assert_empty(
                model,
                check.name,
                f"SELECT 1 FROM {target} WHERE NOT coalesce(({check.predicate}), false)",
            )
        for reference in model.references:
            matches = " AND ".join(
                f"src.{quote(source)} = dst.{quote(destination)}"
                for source, destination in zip(
                    reference.columns, reference.target_columns, strict=True
                )
            )
            all_null = " AND ".join(f"src.{quote(column)} IS NULL" for column in reference.columns)
            self._assert_empty(
                model,
                f"orphan reference to {reference.target}",
                f"""SELECT 1 FROM {target} src WHERE NOT ({all_null})
                    AND NOT EXISTS (
                        SELECT 1 FROM {relation(reference.target)} dst WHERE {matches}
                    )""",
            )
        if isinstance(model, Bridge) and model.allocation is not None:
            weight = quote(model.allocation.weight_column)
            groups = ", ".join(quote(name) for name in model.allocation.group_by)
            self._assert_empty(
                model,
                "allocation weights must be between zero and one",
                f"SELECT 1 FROM {target} WHERE {weight} < 0 OR {weight} > 1",
            )
            self._assert_empty(
                model,
                "allocation weights must total one per group",
                f"SELECT {groups} FROM {target} GROUP BY {groups} HAVING sum({weight}) <> 1",
            )
