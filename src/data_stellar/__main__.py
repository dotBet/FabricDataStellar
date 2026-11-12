from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from data_stellar.duckdb import DuckDBExecutor
from data_stellar.example import build_project_accounting_mart


def main() -> None:
    parser = argparse.ArgumentParser(description="Metadata-driven analytics modeling")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Build the synthetic project-accounting mart")
    demo.add_argument("--database", type=Path, default=Path("output") / "project_accounting.duckdb")
    demo.add_argument("--source-dir", type=Path, default=Path("examples") / "project_accounting")
    args = parser.parse_args()
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
