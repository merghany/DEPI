"""
DAG: load_marts_to_postgres
============================
Reads all 4 dbt mart tables from MySQL and loads them into
the PostgreSQL analytics database.

Strategy: TRUNCATE + full reload (idempotent — safe to re-run).
The marts are small enough (aggregates, not raw events) that a
full reload is simpler and safer than incremental upserts.

Schedule: daily at 03:00 UTC — runs AFTER dbt_daily_marts (02:00)

Data flow:
    MySQL (payment_gateway)
        mart_daily_transaction_summary
        mart_merchant_performance
        mart_client_360
        mart_fraud_analysis
            │
            ▼  (pandas read → type clean → SQLAlchemy write)
    PostgreSQL (analytics)
        mart_daily_transaction_summary
        mart_merchant_performance
        mart_client_360
        mart_fraud_analysis
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── Connection settings ───────────────────────────────────────────────────────
MYSQL_CONN = dict(
    host     = "mysql",
    port     = 3306,
    user     = "pg_user",
    password = "pg_password_2024",
    database = "payment_gateway",
)

PG_CONN = dict(
    host     = "postgres",
    port     = 5432,
    user     = "airflow",       # same user as POSTGRES_USER in .env
    password = "airflow123",    # same as POSTGRES_PASSWORD in .env
    database = "analytics",
)

# Mart table definitions: (mysql_schema.table, pg_table, pk_columns)
MART_TABLES = [
    ("payment_gateway.mart_daily_transaction_summary",
     "mart_daily_transaction_summary",
     ["txn_date", "status", "channel", "currency"]),

    ("payment_gateway.mart_merchant_performance",
     "mart_merchant_performance",
     ["merchant_id"]),

    ("payment_gateway.mart_client_360",
     "mart_client_360",
     ["client_id"]),

    ("payment_gateway.mart_fraud_analysis",
     "mart_fraud_analysis",
     ["alert_id"]),
]

default_args = {
    "owner":             "airflow",
    "retries":           2,
    "retry_delay":       timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

# ── Helper functions ──────────────────────────────────────────────────────────

def _mysql_engine():
    from sqlalchemy import create_engine
    c = MYSQL_CONN
    return create_engine(
        f"mysql+mysqlconnector://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['database']}",
        pool_pre_ping=True,
    )


def _pg_engine():
    from sqlalchemy import create_engine
    c = PG_CONN
    return create_engine(
        f"postgresql+psycopg2://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['database']}",
        pool_pre_ping=True,
    )


def _clean_dataframe(df):
    """Normalise types that cause issues with PostgreSQL via pandas."""
    import pandas as pd
    for col in df.columns:
        # Convert object columns that look like datetimes
        if df[col].dtype == object:
            try:
                df[col] = pd.to_datetime(df[col], errors="ignore")
            except Exception:
                pass
        # Replace NaN with None for nullable integer/float columns
        if df[col].dtype in ["float64", "Float64"]:
            df[col] = df[col].where(df[col].notna(), other=None)
    return df


# ── Core load function ────────────────────────────────────────────────────────

def load_mart(mysql_table: str, pg_table: str, pk_cols: list[str], **ctx):
    """
    1. Read entire mart table from MySQL into a DataFrame.
    2. Add loaded_at timestamp.
    3. TRUNCATE the target PostgreSQL table.
    4. Write all rows via pandas to_sql (fast bulk insert).
    """
    import pandas as pd
    from sqlalchemy import text

    log.info("Loading %s → PostgreSQL:%s …", mysql_table, pg_table)

    # ── Read from MySQL ───────────────────────────────────────────────────────
    mysql_eng = _mysql_engine()
    df = pd.read_sql(f"SELECT * FROM {mysql_table}", con=mysql_eng)
    mysql_eng.dispose()

    if df.empty:
        log.warning("%s returned 0 rows — skipping load.", mysql_table)
        return {"table": pg_table, "rows_loaded": 0}

    row_count = len(df)
    log.info("Read %d rows from MySQL:%s", row_count, mysql_table)

    # ── Clean types ───────────────────────────────────────────────────────────
    df = _clean_dataframe(df)

    # Add load timestamp
    df["loaded_at"] = pd.Timestamp.utcnow()

    # ── Write to PostgreSQL ───────────────────────────────────────────────────
    pg_eng = _pg_engine()
    with pg_eng.begin() as conn:
        # TRUNCATE before reload — keeps the table schema intact
        conn.execute(text(f'TRUNCATE TABLE {pg_table}'))
        log.info("Truncated PostgreSQL:%s", pg_table)

    # Bulk insert via pandas (uses COPY-style multi-row INSERT)
    df.to_sql(
        name       = pg_table,
        con        = pg_eng,
        if_exists  = "append",    # table already exists — just append
        index      = False,
        chunksize  = 5_000,       # write in 5k-row batches
        method     = "multi",     # multi-row INSERT — faster than single rows
    )
    pg_eng.dispose()

    log.info(
        "✅  %s → PostgreSQL:%s — %d rows loaded",
        mysql_table, pg_table, row_count,
    )
    return {"table": pg_table, "rows_loaded": row_count}


def verify_load(**ctx):
    """
    Pull XCom results from all load tasks and verify row counts
    match between MySQL and PostgreSQL.
    """
    import pandas as pd

    ti = ctx["ti"]
    results = {}
    for _, pg_table, _ in MART_TABLES:
        task_id = f"load_{pg_table}"
        result  = ti.xcom_pull(task_ids=task_id)
        results[pg_table] = result

    # Verify in PostgreSQL
    pg_eng = _pg_engine()
    log.info("=" * 60)
    log.info("LOAD VERIFICATION — %s", datetime.utcnow().isoformat())
    total_rows = 0
    for _, pg_table, _ in MART_TABLES:
        df = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {pg_table}", pg_eng)
        pg_count  = df["cnt"].iloc[0]
        src_count = (results.get(pg_table) or {}).get("rows_loaded", "?")
        status    = "✅" if pg_count == src_count else "⚠️"
        log.info(
            "  %s  %-40s  source=%s  pg=%s",
            status, pg_table, src_count, pg_count,
        )
        total_rows += int(pg_count)
    pg_eng.dispose()
    log.info("  Total rows in PostgreSQL analytics: %d", total_rows)
    log.info("=" * 60)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="load_marts_to_postgres",
    description="Load all dbt mart tables from MySQL into PostgreSQL analytics DB",
    schedule_interval="0 3 * * *",   # 03:00 UTC — after dbt_daily_marts (02:00)
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["postgres", "marts", "daily", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    # One task per mart table — all run in parallel
    load_tasks = []
    for mysql_tbl, pg_tbl, pk_cols in MART_TABLES:
        t = PythonOperator(
            task_id=f"load_{pg_tbl}",
            python_callable=load_mart,
            op_kwargs={
                "mysql_table": mysql_tbl,
                "pg_table":    pg_tbl,
                "pk_cols":     pk_cols,
            },
            doc_md=(
                f"Read **{mysql_tbl}** from MySQL and bulk-load into "
                f"PostgreSQL `analytics.{pg_tbl}`. "
                f"Strategy: TRUNCATE + full reload."
            ),
        )
        load_tasks.append(t)

    verify = PythonOperator(
        task_id="verify_load",
        python_callable=verify_load,
        doc_md="Verify row counts match between MySQL source and PostgreSQL target.",
        trigger_rule="all_done",   # run even if a load task partially fails
    )

    end = EmptyOperator(task_id="end")

    # Dependency graph:
    #   start → [all 4 load tasks in parallel] → verify → end
    start >> load_tasks >> verify >> end
