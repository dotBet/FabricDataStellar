from __future__ import annotations

from pathlib import Path

from data_stellar.models import (
    Allocation,
    Bridge,
    Check,
    Column,
    DataType,
    Dimension,
    Fact,
    Mart,
    Measure,
    Reference,
    Table,
)

TEXT = DataType.TEXT
INTEGER = DataType.INTEGER
DATE = DataType.DATE
AMOUNT = DataType.AMOUNT
WEIGHT = DataType.WEIGHT

JOURNAL_KEY = ("source_system", "entity_id", "journal_id", "line_number")


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _decimal(column: str, scale: int = 2) -> str:
    data_type = AMOUNT if scale == 2 else WEIGHT
    return (
        f"CASE WHEN regexp_full_match({column}, '-?[0-9]+([.][0-9]{{1,{scale}}})?') "
        f"THEN CAST({column} AS {data_type.value}) "
        f"ELSE error('Invalid decimal precision: {column}') END"
    )


def _line_number() -> str:
    return (
        "CASE WHEN regexp_full_match(line_number, '[1-9][0-9]*') "
        "THEN CAST(line_number AS BIGINT) ELSE error('Invalid journal line number') END"
    )


def _raw(source_dir: Path, name: str, columns: tuple[str, ...]) -> Table:
    path = (source_dir / f"{name}.csv").resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    declared = (
        Column("raw_row_number", INTEGER),
        *(Column(column, TEXT, nullable=True) for column in columns),
        Column("batch_id", TEXT),
        Column("loaded_at", DataType.TIMESTAMP),
        Column("source_file", TEXT),
    )
    return Table(
        name=f"bronze.{name}",
        columns=declared,
        grain=("raw_row_number",),
        query=f"""
            SELECT row_number() OVER () AS raw_row_number, raw.*,
                   ctx.batch_id, ctx.loaded_at, {_literal(str(path))} AS source_file
            FROM read_csv({_literal(str(path))}, header = true, all_varchar = true) raw
            CROSS JOIN _stellar_context ctx
        """,
    )


def build_project_accounting_mart(source_dir: Path) -> Mart:
    """Build the example from complete, single-currency CSV snapshots."""
    project_columns = (Column("project_id", TEXT), Column("project_name", TEXT))
    employee_columns = (Column("employee_id", TEXT), Column("employee_name", TEXT))
    account_columns = (
        Column("account_code", TEXT),
        Column("account_name", TEXT),
        Column("classification", TEXT),
    )
    bronze = (
        _raw(source_dir, "projects", tuple(c.name for c in project_columns)),
        _raw(source_dir, "employees", tuple(c.name for c in employee_columns)),
        _raw(source_dir, "accounts", tuple(c.name for c in account_columns)),
        _raw(
            source_dir,
            "time_entries",
            ("entry_id", "project_id", "employee_id", "work_date", "hours"),
        ),
        _raw(
            source_dir,
            "journal_lines",
            (*JOURNAL_KEY, "account_code", "posting_date", "currency", "debit", "credit"),
        ),
        _raw(source_dir, "allocations", (*JOURNAL_KEY, "project_id", "weight")),
    )
    silver = (
        Table(
            name="silver.projects",
            columns=project_columns,
            grain=("project_id",),
            depends_on=("bronze.projects",),
            query="SELECT project_id, project_name FROM bronze.projects",
            checks=(Check("nonblank_project", "length(trim(project_id)) > 0"),),
        ),
        Table(
            name="silver.employees",
            columns=employee_columns,
            grain=("employee_id",),
            depends_on=("bronze.employees",),
            query="SELECT employee_id, employee_name FROM bronze.employees",
        ),
        Table(
            name="silver.accounts",
            columns=account_columns,
            grain=("account_code",),
            depends_on=("bronze.accounts",),
            query="SELECT account_code, account_name, classification FROM bronze.accounts",
            checks=(Check("account_class", "classification IN ('expense', 'cash')"),),
        ),
        Table(
            name="silver.time_entries",
            columns=(
                Column("entry_id", TEXT),
                Column("project_id", TEXT),
                Column("employee_id", TEXT),
                Column("work_date", DATE),
                Column("hours", AMOUNT),
            ),
            grain=("entry_id",),
            depends_on=("bronze.time_entries", "silver.projects", "silver.employees"),
            query=f"""
                SELECT entry_id, project_id, employee_id, CAST(work_date AS DATE) AS work_date,
                       {_decimal("hours")} AS hours
                FROM bronze.time_entries
            """,
            references=(
                Reference(("project_id",), "silver.projects", ("project_id",)),
                Reference(("employee_id",), "silver.employees", ("employee_id",)),
            ),
            checks=(Check("nonnegative_hours", "hours >= 0"),),
        ),
        Table(
            name="silver.journal_lines",
            columns=(
                Column("source_system", TEXT),
                Column("entity_id", TEXT),
                Column("journal_id", TEXT),
                Column("line_number", INTEGER),
                Column("account_code", TEXT),
                Column("posting_date", DATE),
                Column("currency", TEXT),
                Column("debit", AMOUNT),
                Column("credit", AMOUNT),
                Column("signed_amount", AMOUNT),
                Column("journal_balance", AMOUNT),
            ),
            grain=JOURNAL_KEY,
            depends_on=("bronze.journal_lines", "silver.accounts"),
            references=(Reference(("account_code",), "silver.accounts", ("account_code",)),),
            query=f"""
                WITH typed AS (
                    SELECT source_system, entity_id, journal_id,
                           {_line_number()} AS line_number, account_code,
                           CAST(posting_date AS DATE) AS posting_date, currency,
                           {_decimal("debit")} AS debit, {_decimal("credit")} AS credit
                    FROM bronze.journal_lines
                )
                SELECT *, debit - credit AS signed_amount,
                       sum(debit - credit) OVER (
                           PARTITION BY source_system, entity_id, journal_id, currency
                       ) AS journal_balance
                FROM typed
            """,
            checks=(
                Check("balanced_journal", "journal_balance = 0"),
                Check(
                    "debit_credit", "debit >= 0 AND credit >= 0 AND NOT (debit > 0 AND credit > 0)"
                ),
                Check("currency_code", "regexp_full_match(currency, '[A-Z]{3}')"),
            ),
        ),
        Table(
            name="silver.allocations",
            columns=(
                Column("source_system", TEXT),
                Column("entity_id", TEXT),
                Column("journal_id", TEXT),
                Column("line_number", INTEGER),
                Column("project_id", TEXT),
                Column("weight", WEIGHT),
            ),
            grain=(*JOURNAL_KEY, "project_id"),
            depends_on=("bronze.allocations", "silver.journal_lines", "silver.projects"),
            query=f"""
                SELECT source_system, entity_id, journal_id,
                       {_line_number()} AS line_number, project_id,
                       {_decimal("weight", scale=8)} AS weight
                FROM bronze.allocations
            """,
            references=(
                Reference(JOURNAL_KEY, "silver.journal_lines", JOURNAL_KEY),
                Reference(("project_id",), "silver.projects", ("project_id",)),
            ),
        ),
    )
    dimensions = (
        Dimension(
            name="gold.dim_project",
            columns=(Column("project_key", TEXT), *project_columns),
            grain=("project_key",),
            business_key=("project_id",),
            surrogate_key="project_key",
            depends_on=("silver.projects",),
            query="SELECT md5(project_id) AS project_key, * FROM silver.projects",
        ),
        Dimension(
            name="gold.dim_employee",
            columns=(Column("employee_key", TEXT), *employee_columns),
            grain=("employee_key",),
            business_key=("employee_id",),
            surrogate_key="employee_key",
            depends_on=("silver.employees",),
            query="SELECT md5(employee_id) AS employee_key, * FROM silver.employees",
        ),
        Dimension(
            name="gold.dim_account",
            columns=(Column("account_key", TEXT), *account_columns),
            grain=("account_key",),
            business_key=("account_code",),
            surrogate_key="account_key",
            depends_on=("silver.accounts",),
            query="SELECT md5(account_code) AS account_key, * FROM silver.accounts",
        ),
        Dimension(
            name="gold.dim_date",
            columns=(Column("date_key", INTEGER), Column("calendar_date", DATE)),
            grain=("date_key",),
            business_key=("calendar_date",),
            surrogate_key="date_key",
            depends_on=("silver.time_entries", "silver.journal_lines"),
            query="""
                WITH dates AS (
                    SELECT work_date AS day FROM silver.time_entries
                    UNION ALL SELECT posting_date FROM silver.journal_lines
                ), bounds AS (SELECT min(day) AS first_day, max(day) AS last_day FROM dates)
                SELECT CAST(strftime(day, '%Y%m%d') AS BIGINT) AS date_key,
                       CAST(day AS DATE) AS calendar_date
                FROM bounds, generate_series(first_day, last_day, INTERVAL 1 DAY) days(day)
            """,
        ),
    )
    facts = (
        Fact(
            name="gold.fact_time_entry",
            columns=(
                Column("entry_id", TEXT),
                Column("project_key", TEXT),
                Column("employee_key", TEXT),
                Column("work_date_key", INTEGER),
                Column("hours", AMOUNT),
            ),
            grain=("entry_id",),
            depends_on=(
                "silver.time_entries",
                "gold.dim_project",
                "gold.dim_employee",
                "gold.dim_date",
            ),
            references=(
                Reference(("project_key",), "gold.dim_project", ("project_key",)),
                Reference(("employee_key",), "gold.dim_employee", ("employee_key",)),
                Reference(("work_date_key",), "gold.dim_date", ("date_key",)),
            ),
            measures=(Measure("hours"),),
            query="""
                SELECT entry_id, p.project_key, e.employee_key,
                       d.date_key AS work_date_key, t.hours
                FROM silver.time_entries t
                JOIN gold.dim_project p USING (project_id)
                JOIN gold.dim_employee e USING (employee_id)
                JOIN gold.dim_date d ON d.calendar_date = t.work_date
            """,
        ),
        Fact(
            name="gold.fact_journal_line",
            columns=(
                Column("source_system", TEXT),
                Column("entity_id", TEXT),
                Column("journal_id", TEXT),
                Column("line_number", INTEGER),
                Column("account_key", TEXT),
                Column("posting_date_key", INTEGER),
                Column("currency", TEXT),
                Column("debit", AMOUNT),
                Column("credit", AMOUNT),
                Column("signed_amount", AMOUNT),
            ),
            grain=JOURNAL_KEY,
            depends_on=("silver.journal_lines", "gold.dim_account", "gold.dim_date"),
            references=(
                Reference(("account_key",), "gold.dim_account", ("account_key",)),
                Reference(("posting_date_key",), "gold.dim_date", ("date_key",)),
            ),
            measures=tuple(
                Measure(name, currency_column="currency")
                for name in ("debit", "credit", "signed_amount")
            ),
            query="""
                SELECT source_system, entity_id, journal_id, line_number, a.account_key,
                       d.date_key AS posting_date_key, currency, debit, credit, signed_amount
                FROM silver.journal_lines j
                JOIN gold.dim_account a USING (account_code)
                JOIN gold.dim_date d ON d.calendar_date = j.posting_date
            """,
        ),
    )
    bridge = Bridge(
        name="gold.bridge_journal_project",
        columns=(
            Column("source_system", TEXT),
            Column("entity_id", TEXT),
            Column("journal_id", TEXT),
            Column("line_number", INTEGER),
            Column("project_key", TEXT),
            Column("weight", WEIGHT),
            Column("allocated_amount", AMOUNT),
        ),
        grain=(*JOURNAL_KEY, "project_key"),
        depends_on=("silver.allocations", "gold.fact_journal_line", "gold.dim_project"),
        references=(
            Reference(JOURNAL_KEY, "gold.fact_journal_line", JOURNAL_KEY),
            Reference(("project_key",), "gold.dim_project", ("project_key",)),
        ),
        allocation=Allocation("weight", JOURNAL_KEY),
        query="""
            WITH weighted AS (
                SELECT a.source_system, a.entity_id, a.journal_id, a.line_number,
                       p.project_key, p.project_id, a.weight, j.signed_amount,
                       CAST(floor(abs(j.signed_amount) * 100 * a.weight) AS BIGINT) AS base_cents,
                       abs(j.signed_amount) * 100 * a.weight
                           - floor(abs(j.signed_amount) * 100 * a.weight) AS remainder
                FROM silver.allocations a
                JOIN gold.fact_journal_line j
                  USING (source_system, entity_id, journal_id, line_number)
                JOIN gold.dim_project p USING (project_id)
            ), ranked AS (
                SELECT *,
                       abs(signed_amount) * 100 - sum(base_cents) OVER (
                           PARTITION BY source_system, entity_id, journal_id, line_number
                       ) AS remaining_cents,
                       row_number() OVER (
                           PARTITION BY source_system, entity_id, journal_id, line_number
                           ORDER BY remainder DESC, project_id
                       ) AS remainder_rank
                FROM weighted
            )
            SELECT source_system, entity_id, journal_id, line_number, project_key, weight,
                   CAST(sign(signed_amount) * (
                       base_cents + CASE WHEN remainder_rank <= remaining_cents THEN 1 ELSE 0 END
                   ) * 0.01 AS DECIMAL(18, 2)) AS allocated_amount
            FROM ranked
        """,
    )
    coverage = Table(
        name="gold.allocation_coverage",
        columns=(
            Column("source_system", TEXT),
            Column("entity_id", TEXT),
            Column("journal_id", TEXT),
            Column("line_number", INTEGER),
            Column("allocation_count", INTEGER),
            Column("original_amount", AMOUNT),
            Column("allocated_amount", AMOUNT),
        ),
        grain=JOURNAL_KEY,
        depends_on=("gold.fact_journal_line", "gold.bridge_journal_project"),
        query="""
            SELECT j.source_system, j.entity_id, j.journal_id, j.line_number,
                   count(b.project_key) AS allocation_count,
                   j.signed_amount AS original_amount,
                   CAST(coalesce(sum(b.allocated_amount), 0) AS DECIMAL(18, 2)) AS allocated_amount
            FROM gold.fact_journal_line j
            LEFT JOIN gold.bridge_journal_project b
              USING (source_system, entity_id, journal_id, line_number)
            GROUP BY j.source_system, j.entity_id, j.journal_id, j.line_number, j.signed_amount
        """,
        checks=(
            Check("every_line_requires_allocation", "allocation_count > 0"),
            Check("allocation_reconciles", "original_amount = allocated_amount"),
        ),
    )
    summary = Table(
        name="gold.project_summary",
        columns=(
            Column("project_key", TEXT),
            Column("project_id", TEXT),
            Column("project_name", TEXT),
            Column("source_system", TEXT),
            Column("entity_id", TEXT),
            Column("currency", TEXT),
            Column("actual_cost", AMOUNT),
        ),
        grain=("project_key", "source_system", "entity_id", "currency"),
        depends_on=(
            "gold.allocation_coverage",
            "gold.dim_account",
            "gold.dim_project",
            "gold.fact_journal_line",
            "gold.bridge_journal_project",
        ),
        query="""
            SELECT p.project_key, p.project_id, p.project_name,
                   j.source_system, j.entity_id, j.currency,
                   CAST(sum(b.allocated_amount) AS DECIMAL(18, 2)) AS actual_cost
            FROM gold.fact_journal_line j
            JOIN gold.dim_account a USING (account_key)
            JOIN gold.bridge_journal_project b
              USING (source_system, entity_id, journal_id, line_number)
            JOIN gold.dim_project p USING (project_key)
            WHERE a.classification = 'expense'
            GROUP BY p.project_key, p.project_id, p.project_name,
                     j.source_system, j.entity_id, j.currency
        """,
    )
    return Mart(
        "project_accounting", (*bronze, *silver, *dimensions, *facts, bridge, coverage, summary)
    )
