from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

import duckdb

from data_stellar.models import (
    Bridge,
    DataType,
    Dimension,
    Fact,
    Mart,
    Table,
    identifier,
    qualified_name,
)


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
    """Atomically publish full-refresh and incremental models on a dedicated connection."""

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
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS _stellar.model_state (
                model_name VARCHAR PRIMARY KEY,
                mart_name VARCHAR NOT NULL,
                contract VARCHAR NOT NULL,
                watermark TIMESTAMPTZ
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
            registry = {model.name: model for model in plan}
            counts = {model.name: self._materialize(model, mart.name, registry) for model in plan}
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

    def _materialize(self, model: Table, mart_name: str, registry: dict[str, Table]) -> int:
        try:
            self.connection.execute(f"CREATE OR REPLACE TEMP TABLE _stellar_query AS {model.query}")
            actual = tuple(
                row[1]
                for row in self.connection.execute("PRAGMA table_info('_stellar_query')").fetchall()
            )
            managed: set[str] = set()
            if isinstance(model, Dimension) and model.history is not None:
                managed = {
                    model.surrogate_key,
                    model.history.valid_to,
                    model.history.current_flag,
                }
            if isinstance(model, Fact):
                managed = {lookup.surrogate_column for lookup in model.temporal_lookups}
            inputs = tuple(column for column in model.columns if column.name not in managed)
            expected = tuple(column.name for column in inputs)
            if set(actual) != set(expected):
                raise DataQualityError(
                    f"{model.name}: query columns {actual} do not match declared columns {expected}"
                )
            input_definitions = ", ".join(
                f"{quote(column.name)} {column.data_type.value}"
                f"{'' if column.nullable else ' NOT NULL'}"
                for column in inputs
            )
            input_columns = ", ".join(quote(column.name) for column in inputs)
            self.connection.execute(
                f"CREATE OR REPLACE TEMP TABLE _stellar_stage ({input_definitions})"
            )
            self.connection.execute(
                f"INSERT INTO _stellar_stage SELECT {input_columns} FROM _stellar_query"
            )
            self.connection.execute("DROP TABLE _stellar_query")
            if model.incremental is not None:
                self._incremental(model, mart_name, input_columns)
            elif self.connection.execute(
                "SELECT 1 FROM _stellar.model_state WHERE model_name = ?", [model.name]
            ).fetchone():
                raise DataQualityError(
                    f"{model.name}: cannot switch a stateful model to full refresh"
                )
            if isinstance(model, Dimension) and model.history is not None:
                self._history(model)
            if isinstance(model, Fact):
                self._temporal(model, registry)
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
        if (
            isinstance(model, Dimension)
            and model.history is None
            and model.business_key not in keys
        ):
            keys.append(model.business_key)
        for key in keys:
            columns = ", ".join(quote(name) for name in key)
            self._assert_empty(
                model,
                f"duplicate key {key}",
                f"SELECT {columns} FROM {target} GROUP BY {columns} HAVING count(*) > 1",
            )
        if isinstance(model, Dimension) and model.history is not None:
            history = model.history
            start, end = quote(history.valid_from), quote(history.valid_to)
            groups = ", ".join(quote(name) for name in model.business_key)
            self._assert_empty(
                model,
                "invalid SCD2 intervals",
                f"""SELECT 1 FROM (
                    SELECT *, lead({start}) OVER (PARTITION BY {groups} ORDER BY {start}) next_start
                    FROM {target}
                ) versions
                WHERE NOT isfinite({start}) OR ({end} IS NOT NULL AND {end} <= {start})
                   OR {end} IS DISTINCT FROM next_start
                   OR {quote(history.current_flag)} IS DISTINCT FROM ({end} IS NULL)""",
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

    def _incremental(self, model: Table, mart_name: str, columns: str) -> None:
        policy = model.incremental
        assert policy is not None
        history = model.history if isinstance(model, Dimension) else None
        contract = json.dumps(
            {
                "columns": [asdict(column) for column in model.columns],
                "grain": model.grain,
                "incremental": asdict(policy),
                "history": asdict(history) if history else None,
                "business_key": model.business_key if isinstance(model, Dimension) else None,
                "lookups": (
                    [asdict(lookup) for lookup in model.temporal_lookups]
                    if isinstance(model, Fact)
                    else []
                ),
            },
            sort_keys=True,
        )
        state = self.connection.execute(
            "SELECT mart_name, contract, watermark FROM _stellar.model_state WHERE model_name = ?",
            [model.name],
        ).fetchone()
        if state is not None and (state[0] != mart_name or state[1] != contract):
            raise DataQualityError(
                f"{model.name}: stateful contract/owner changed; migration required"
            )
        schema, table = qualified_name(model.name)
        exists = self.connection.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        if state is None and exists:
            raise DataQualityError(
                f"{model.name}: cannot adopt existing table without loading state"
            )
        if state is not None and not exists:
            raise DataQualityError(f"{model.name}: state exists but published table is missing")
        cursor = quote(policy.cursor)
        self._assert_empty(
            model,
            "incremental cursor must be finite",
            f"SELECT 1 FROM _stellar_stage WHERE NOT isfinite({cursor})",
        )
        if state is not None and state[2] is not None:
            self.connection.execute(f"DELETE FROM _stellar_stage WHERE {cursor} < ?", [state[2]])
        if history is not None:
            self._assert_empty(
                model,
                "SCD2 effective time must be finite",
                f"SELECT 1 FROM _stellar_stage WHERE NOT isfinite({quote(history.valid_from)})",
            )
        keys = (
            (*model.business_key, history.valid_from)
            if isinstance(model, Dimension) and history is not None
            else model.grain
        )
        groups = ", ".join(quote(name) for name in keys)
        self._assert_empty(
            model,
            "duplicate incremental key",
            f"SELECT {groups} FROM _stellar_stage GROUP BY {groups} HAVING count(*) > 1",
        )
        if history is not None:
            digest = hashlib.sha256(model.name.encode()).hexdigest()[:24]
            stored = f'_stellar."history_{digest}"'
        else:
            stored = relation(model.name)
        if state is None:
            self.connection.execute(
                """INSERT INTO _stellar.model_state VALUES (?, ?, ?, NULL)""",
                [model.name, mart_name, contract],
            )
            if history is not None:
                self.connection.execute(
                    f"CREATE TABLE {stored} AS SELECT * FROM _stellar_stage WHERE false"
                )
        if state is not None:
            matches = " AND ".join(f"old.{quote(k)} = new.{quote(k)}" for k in keys)
            names = tuple(
                row[1]
                for row in self.connection.execute("PRAGMA table_info('_stellar_stage')").fetchall()
            )
            changes = " OR ".join(
                f"old.{quote(name)} IS DISTINCT FROM new.{quote(name)}" for name in names
            )
            self._assert_empty(
                model,
                "conflicting payload at the same cursor",
                f"""SELECT 1 FROM {stored} old JOIN _stellar_stage new ON {matches}
                    WHERE old.{cursor} = new.{cursor} AND ({changes})""",
            )
            self.connection.execute(
                f"""CREATE OR REPLACE TEMP TABLE _stellar_merged AS
                    SELECT {", ".join("old." + quote(n) for n in names)}
                    FROM {stored} old
                    WHERE NOT EXISTS (SELECT 1 FROM _stellar_stage new WHERE {matches})
                    UNION ALL SELECT {columns} FROM _stellar_stage"""
            )
        maximum = self.connection.execute(f"SELECT max({cursor}) FROM _stellar_stage").fetchone()
        if maximum is not None and maximum[0] is not None:
            self.connection.execute(
                "UPDATE _stellar.model_state SET watermark = ? WHERE model_name = ?",
                [maximum[0], model.name],
            )
        if state is not None:
            self.connection.execute("DELETE FROM _stellar_stage")
            self.connection.execute("INSERT INTO _stellar_stage SELECT * FROM _stellar_merged")
            self.connection.execute("DROP TABLE _stellar_merged")
        if history is not None:
            self.connection.execute(f"DELETE FROM {stored}")
            self.connection.execute(f"INSERT INTO {stored} SELECT * FROM _stellar_stage")

    def _history(self, model: Dimension) -> None:
        history = model.history
        assert history is not None
        start, end = quote(history.valid_from), quote(history.valid_to)
        groups = ", ".join(quote(name) for name in model.business_key)
        comparisons = " OR ".join(
            f"{quote(name)} IS DISTINCT FROM lag({quote(name)}) OVER versions"
            for name in history.tracked_columns
        )
        identity = ", ".join([*(quote(name) for name in model.business_key), f"epoch_us({start})"])
        self.connection.execute(f"""
            CREATE OR REPLACE TEMP TABLE _stellar_versions AS
            WITH marked AS (
                SELECT *,
                       row_number() OVER versions = 1 OR ({comparisons}) AS _changed
                FROM _stellar_stage
                WINDOW versions AS (PARTITION BY {groups} ORDER BY {start})
            ), changes AS (
                SELECT * EXCLUDE (_changed) FROM marked WHERE _changed
            )
            SELECT *, md5(json_array({identity})) AS {quote(model.surrogate_key)},
                   lead({start}) OVER versions AS {end},
                   lead({start}) OVER versions IS NULL AS {quote(history.current_flag)}
            FROM changes
            WINDOW versions AS (PARTITION BY {groups} ORDER BY {start})
        """)
        self.connection.execute("DROP TABLE _stellar_stage")
        self.connection.execute("ALTER TABLE _stellar_versions RENAME TO _stellar_stage")

    def _temporal(self, model: Fact, registry: dict[str, Table]) -> None:
        for lookup in model.temporal_lookups:
            target = registry[lookup.target]
            assert isinstance(target, Dimension) and target.history is not None
            history = target.history
            event = f"src.{quote(lookup.time_column)}"
            event_type = next(c.data_type for c in model.columns if c.name == lookup.time_column)
            if event_type == DataType.DATE:
                event = f"(CAST({event} AS TIMESTAMP) AT TIME ZONE 'UTC')"
            self._assert_empty(
                model,
                "temporal event time must be finite",
                f"SELECT 1 FROM _stellar_stage src WHERE NOT isfinite({event})",
            )
            keys = " AND ".join(
                f"src.{quote(source)} = dst.{quote(destination)}"
                for source, destination in zip(lookup.columns, target.business_key, strict=True)
            )
            predicate = (
                f"{keys} AND {event} >= dst.{quote(history.valid_from)} "
                f"AND ({event} < dst.{quote(history.valid_to)} "
                f"OR dst.{quote(history.valid_to)} IS NULL)"
            )
            self._assert_empty(
                model,
                f"missing temporal version in {target.name}",
                f"""SELECT 1 FROM _stellar_stage src
                    WHERE (SELECT count(*) FROM {relation(target.name)} dst
                           WHERE {predicate}) <> 1""",
            )
            self.connection.execute(f"""
                CREATE OR REPLACE TEMP TABLE _stellar_resolved AS
                SELECT src.*, dst.{quote(target.surrogate_key)} AS {quote(lookup.surrogate_column)}
                FROM _stellar_stage src JOIN {relation(target.name)} dst ON {predicate}
            """)
            self.connection.execute("DROP TABLE _stellar_stage")
            self.connection.execute("ALTER TABLE _stellar_resolved RENAME TO _stellar_stage")
