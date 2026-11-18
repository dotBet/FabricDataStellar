from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from data_stellar.duckdb import DuckDBExecutor
from data_stellar.example import build_project_accounting_mart
from data_stellar.history_example import build_project_history_mart


def _history_demo(database: Path, source_dir: Path) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        executor = DuckDBExecutor(connection)
        for batch in ("initial", "backdated"):
            result = executor.run(build_project_history_mart(source_dir / batch))
            print(f"{batch}: succeeded ({result.run_id})")
            for entry_id, name, hours in connection.execute("""
                SELECT entry_id, project_name, hours FROM gold.fact_project_time
                JOIN gold.dim_project_history USING (project_key) ORDER BY entry_id
            """).fetchall():
                print(f"  {entry_id}: {name}, {hours} hours")
        print(f"Database: {database.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Metadata-driven analytics modeling")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Build the synthetic project-accounting mart")
    demo.add_argument("--database", type=Path, default=Path("output") / "project_accounting.duckdb")
    demo.add_argument("--source-dir", type=Path, default=Path("examples") / "project_accounting")
    history = commands.add_parser(
        "history-demo", help="Apply initial and backdated project changes with temporal fact repair"
    )
    history.add_argument("--database", type=Path, default=Path("output") / "project_history.duckdb")
    history.add_argument("--source-dir", type=Path, default=Path("examples") / "project_history")
    args = parser.parse_args()
    if args.command == "history-demo":
        _history_demo(args.database, args.source_dir)
        return
    mart = build_project_accounting_mart(args.source_dir)
    args.database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(args.database)) as connection:
        result = DuckDBExecutor(connection).run(mart)
        print(f"Run {result.run_id}: succeeded ({len(result.row_counts)} models)")
        print(f"Database: {args.database.resolve()}")
        print("Hours by project:")
        hours = connection.execute("""
            SELECT project_id, sum(hours) FROM gold.fact_time_entry
            JOIN gold.dim_project USING (project_key)
            GROUP BY project_id ORDER BY project_id
        """).fetchall()
        for project_id, total in hours:
            print(f"  {project_id}: {total}")
        print("Actual costs by project, entity and currency:")
        costs = connection.execute("""
            SELECT project_id, source_system, entity_id, currency, actual_cost
            FROM gold.project_summary ORDER BY project_id, source_system, entity_id, currency
        """).fetchall()
        for project_id, source_system, entity_id, currency, cost in costs:
            print(f"  {project_id} / {source_system} / {entity_id}: {cost} {currency}")


if __name__ == "__main__":
    main()
