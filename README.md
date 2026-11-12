# Fabric Data Stellar

A Python library for metadata-driven analytics engineering, with a runnable
Project Management and Accounting mart. It runs locally on DuckDB; Fabric is a
future execution target, not a current dependency.

## Quick start (PowerShell)

Python 3.11 or newer and [uv](https://docs.astral.sh/uv/) are required.
Run these commands from the project root:

```powershell
uv sync --extra dev
uv run data-stellar demo
uv run pytest
uv run ruff check .
uv run mypy
```

The demo creates `output\project_accounting.duckdb`. Run it again to rebuild
the mart without duplicating business rows. Alternatively, select explicit paths:

```powershell
uv run data-stellar demo --database output\demo.duckdb --source-dir examples\project_accounting
```

Expected results:

| Project | Hours | Actual expense (USD, demo_erp / LEG1) |
|---|---:|---:|
| P100 | 12.50 | 800.00 |
| P200 | 5.00 | 400.00 |

All example records are synthetic.

## Architecture

```text
CSV snapshots -> bronze raw tables -> silver validated tables -> gold dimensional mart
                                   \-> failed run audit / transaction rollback
```

- **Bronze:** source fields are retained as text, with batch ID, load timestamp,
  source file and raw row number. Raw row numbers identify rows within the current
  snapshot, not stable business identities.
- **Silver:** explicit typing, declared grains, source relationships, nonnegative
  hours, currency-code format, and journal balancing per source/entity/journal/currency.
  Malformed values fail the run rather than being dropped or silently rounded.
- **Gold:** Project, Employee, Account and Date dimensions; Time Entry and Journal
  Line facts; a Journal-Project allocation bridge; allocation reconciliation and
  project cost summaries.
- **Runtime audit:** `_stellar.runs` records run ID, source batch, timestamps,
  status and error. Business tables and a success status commit together. Failed
  executions roll back all model changes, preserve previously published data and
  persist the failure status.

The calendar spans the inclusive minimum/maximum dates in the current snapshot.
MD5-derived dimension keys are deterministic for the example's single-column
business identities; both surrogate and business-key uniqueness are validated.
They are not security hashes or a general multi-source identity-resolution scheme.

## Modeling API

[models.py](src/data_stellar/models.py) contains frozen typed declarations:

| Declaration | Contract |
|---|---|
| `Table` | Schema, explicit grain, SQL, dependencies, references and quality predicates |
| `Dimension` | Surrogate-key grain and unique non-null business key |
| `Fact` | Dimension references and numeric measure metadata |
| `Bridge` | At least two references, optional exact decimal allocation weights |
| `Mart` | Registry validation and topological dependency planning |

`Mart.plan()` rejects duplicate models, unknown dependencies/columns, invalid
reference targets, and dependency cycles before execution.
`DuckDBExecutor.run()` materializes models in dependency order and validates each
before proceeding. Query output columns must match the declaration exactly.
All declared keys are non-null and unique. Optional composite references may be
entirely null; partially null references fail.

Example:

```python
import duckdb

from data_stellar import Column, DataType, Dimension, Mart
from data_stellar.duckdb import DuckDBExecutor

project = Dimension(
    name="gold.dim_project",
    columns=(
        Column("project_key", DataType.TEXT),
        Column("project_id", DataType.TEXT),
    ),
    grain=("project_key",),
    business_key=("project_id",),
    surrogate_key="project_key",
    query="SELECT md5('P100') AS project_key, 'P100' AS project_id",
)

with duckdb.connect(":memory:") as connection:
    result = DuckDBExecutor(connection).run(Mart("example", (project,)))
    print(result.row_counts)
```

Models represent datasets, not individual rows. Transformations are set-based SQL.
SQL and check predicates are trusted developer-authored code, not sandboxed input.
An example extension lives in [example.py](src/data_stellar/example.py).

## Accounting and allocation semantics

- Each journal line is identified by source system, entity, journal and line number.
- Debit and credit are nonnegative decimal amounts; signed amount is debit minus credit.
- The example accepts expense and cash account classifications only.
- Every journal line must have project allocations, including balancing cash lines.
  Unallocated lines fail explicitly; an unknown-project fallback is not invented.
- Weights have eight decimal places, lie between zero and one and sum **exactly**
  to one per journal line. Negative or over-precision values fail.
- Allocation uses the largest-remainder method on absolute integer cents, then
  restores the sign. Remaining cents go to the largest fractional remainder,
  breaking ties by project ID. Allocated amounts reconcile exactly to every line.
- This initial example assumes two-decimal currencies. It does not implement
  currency-specific minor units, exchange rates, or currency conversion.
- Costs remain separated by source, entity and currency.
- Hours and costs are reported separately: raw facts are never joined together.
  Measure aggregation metadata documents semantics; it does not generate a BI
  semantic layer or prevent arbitrary user-written SQL from mixing currencies.

## Current scope and boundaries

This is the first vertical slice, not a production warehouse framework.

**Implemented:** typed model contracts, dependency planning, transactional
full-refresh execution, snapshot/Type-1 dimension behavior, decimal allocations,
quality checks, audit records, an installed CLI, and regression tests.

**Not implemented:** SCD Type 2/temporal lookups, incremental watermarks, source CDC,
append-only Bronze history, budget/snapshot facts, Data Vault, Fabric/Spark execution,
Power BI, automatic schema migrations, or concurrent writers.

Every successful run replaces the participating tables from complete input snapshots,
including removal of absent source rows. Do not feed partial extracts. Business row
counts and keys are replay-stable, but ingestion timestamps and run IDs change.
Source files must remain unchanged during a run. Use a dedicated DuckDB connection
outside any existing transaction.

The next step is explicit historical-loading contracts and tests for backdated
changes, late-arriving facts and SCD2 interval integrity. Data Vault remains an
optional later integration layer. A future Fabric adapter must implement and test
its own SQL, storage, and commit semantics; DuckDB transactions do not imply that
multi-table Delta writes can be atomic.
