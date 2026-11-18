from __future__ import annotations

from pathlib import Path

from data_stellar import (
    SCD2,
    Check,
    Column,
    DataType,
    Dimension,
    Fact,
    Incremental,
    Mart,
    Measure,
    Table,
    TemporalLookup,
)


def build_project_history_mart(source_dir: Path) -> Mart:
    """Load change batches; updated_at is the arrival cursor, not the effective time."""
    for name in ("project_events", "time_changes"):
        if not (source_dir / f"{name}.csv").is_file():
            raise FileNotFoundError(source_dir / f"{name}.csv")

    def source(name: str) -> str:
        path = str((source_dir / f"{name}.csv").resolve()).replace("'", "''")
        return f"read_csv('{path}', header = true, all_varchar = true)"

    project_columns = (
        Column("project_id", DataType.TEXT),
        Column("project_name", DataType.TEXT),
        Column("valid_from", DataType.TIMESTAMP),
        Column("updated_at", DataType.TIMESTAMP),
    )
    time_columns = (
        Column("entry_id", DataType.TEXT),
        Column("project_id", DataType.TEXT),
        Column("work_at", DataType.TIMESTAMP),
        Column("hours", DataType.AMOUNT),
        Column("updated_at", DataType.TIMESTAMP),
    )
    events = Table(
        name="silver.project_events",
        columns=project_columns,
        grain=("project_id", "valid_from"),
        incremental=Incremental("updated_at"),
        query=f"""
            SELECT project_id, project_name, CAST(valid_from AS TIMESTAMPTZ) AS valid_from,
                   CAST(updated_at AS TIMESTAMPTZ) AS updated_at FROM {source("project_events")}
        """,
    )
    changes = Table(
        name="silver.time_changes",
        columns=time_columns,
        grain=("entry_id",),
        incremental=Incremental("updated_at"),
        query=f"""
            SELECT entry_id, project_id, CAST(work_at AS TIMESTAMPTZ) AS work_at,
                   CASE WHEN regexp_full_match(hours, '[0-9]+([.][0-9]{{1,2}})?')
                        THEN CAST(hours AS DECIMAL(18, 2))
                        ELSE error('Invalid hours precision') END AS hours,
                   CAST(updated_at AS TIMESTAMPTZ) AS updated_at FROM {source("time_changes")}
        """,
    )
    project = Dimension(
        name="gold.dim_project_history",
        columns=(
            Column("project_key", DataType.TEXT),
            *project_columns,
            Column("valid_to", DataType.TIMESTAMP, nullable=True),
            Column("is_current", DataType.BOOLEAN),
        ),
        grain=("project_key",),
        business_key=("project_id",),
        surrogate_key="project_key",
        history=SCD2(("project_name",)),
        incremental=Incremental("updated_at"),
        depends_on=(events.name,),
        query="SELECT project_id, project_name, valid_from, updated_at FROM silver.project_events",
    )
    fact = Fact(
        name="gold.fact_project_time",
        columns=(*time_columns, Column("project_key", DataType.TEXT)),
        grain=("entry_id",),
        incremental=Incremental("updated_at"),
        depends_on=(changes.name, project.name),
        temporal_lookups=(TemporalLookup(("project_id",), project.name, "work_at", "project_key"),),
        measures=(Measure("hours"),),
        checks=(Check("nonnegative_hours", "hours >= 0"),),
        query="SELECT entry_id, project_id, work_at, hours, updated_at FROM silver.time_changes",
    )
    return Mart("project_history", (events, changes, project, fact))
