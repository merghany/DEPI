"""
DAG: dbt_daily_marts
=====================
Runs the full dbt pipeline once per day:
  1. Refresh staging views
  2. Build all mart tables
  3. Run all data quality tests
  4. Generate dbt docs

Schedule: daily at 02:00 UTC (after data has settled)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

DBT_ENV = (f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} DBT_PROJECT_DIR={DBT_PROJECT_DIR} ")

DBT_BASE = "dbt"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"

default_args = {
    "owner":             "airflow",
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure":  False,
}

with DAG(
    dag_id="dbt_daily_marts",
    description="Full dbt run — staging + marts + tests + docs (daily at 02:00)",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "marts", "daily"],
) as dag:

    # ── 1. Connection check ───────────────────────────────────────────────────
    t_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_ENV} {DBT_BASE} debug",
    )

    # ── 2. Staging layer ──────────────────────────────────────────────────────
    t_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_ENV} {DBT_BASE} run --select staging {DBT_FLAGS}",
        doc_md="Materialise all staging views from raw MySQL tables.",
    )

    # ── 3. Mart layer ─────────────────────────────────────────────────────────
    t_mart_daily = BashOperator(
        task_id="dbt_run_mart_daily_summary",
        bash_command=(
            f"{DBT_ENV} {DBT_BASE} run "
            "--select mart_daily_transaction_summary"
        ),
        doc_md="Build daily transaction summary mart.",
    )

    t_mart_merchant = BashOperator(
        task_id="dbt_run_mart_merchant_performance",
        bash_command=(
            f"{DBT_ENV} {DBT_BASE} run "
            "--select mart_merchant_performance"
        ),
        doc_md="Build merchant performance mart.",
    )

    t_mart_client = BashOperator(
        task_id="dbt_run_mart_client_360",
        bash_command=(
            f"{DBT_ENV} {DBT_BASE} run "
            "--select mart_client_360"
        ),
        doc_md="Build client 360 mart.",
    )

    t_mart_fraud = BashOperator(
        task_id="dbt_run_mart_fraud_analysis",
        bash_command=(
            f"{DBT_ENV} {DBT_BASE} run "
            "--select mart_fraud_analysis"
        ),
        doc_md="Build fraud analysis mart.",
    )

    # ── 4. Tests ──────────────────────────────────────────────────────────────
    t_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"{DBT_ENV} {DBT_BASE} test {DBT_FLAGS}",
        doc_md="Run all dbt data quality tests across staging and marts.",
    )

    # ── 5. Generate docs ──────────────────────────────────────────────────────
    t_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT_ENV} {DBT_BASE} docs generate {DBT_FLAGS}",
        doc_md="Regenerate dbt documentation catalog.json.",
    )

    # ── DAG: dependency graph ─────────────────────────────────────────────────
    #
    #   debug → staging → [mart_daily, mart_merchant, mart_client, mart_fraud]
    #                                        └─ (all complete) → test → docs
    #
    t_debug >> t_staging >> [t_mart_daily, t_mart_merchant, t_mart_client, t_mart_fraud]
    [t_mart_daily, t_mart_merchant, t_mart_client, t_mart_fraud] >> t_test >> t_docs
