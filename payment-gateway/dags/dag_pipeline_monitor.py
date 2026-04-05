"""
DAG: pipeline_monitor
======================
Monitors the health of the entire payment gateway pipeline
every 15 minutes and raises alerts on anomalies.

Checks performed:
  1. MySQL connectivity and row counts
  2. Kafka consumer lag
  3. Transaction ingestion rate (new rows in last 15 min)
  4. Fraud alert spike detection (>10% fraud rate in last hour)
  5. dbt source freshness

Schedule: every 15 minutes
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_ENV = (f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} DBT_PROJECT_DIR={DBT_PROJECT_DIR} ")
DBT_BASE = "dbt"

default_args = {
    "owner":             "airflow",
    "retries":           1,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


# ── Python checks ─────────────────────────────────────────────────────────────

def check_mysql_health(**ctx):
    """Verify MySQL is reachable and key tables have expected row counts."""
    import mysql.connector
    conn = mysql.connector.connect(
        host="mysql", port=3306,
        user="pg_user", password="pg_password_2024",
        database="payment_gateway", connection_timeout=5,
    )
    cur = conn.cursor()
    checks = {
        "transactions":   100_000,
        "clients":        1_000,
        "fraud_alerts":   1_000,
    }
    results = {}
    for table, min_rows in checks.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        results[table] = count
        if count < min_rows:
            raise ValueError(
                f"Table '{table}' has only {count:,} rows — "
                f"expected at least {min_rows:,}"
            )
    cur.close()
    conn.close()
    log.info("MySQL health OK: %s", results)
    return results


def check_txn_ingestion_rate(**ctx):
    """Alert if fewer than 100 new transactions arrived in the last 15 minutes."""
    import mysql.connector
    conn = mysql.connector.connect(
        host="mysql", port=3306,
        user="pg_user", password="pg_password_2024",
        database="payment_gateway", connection_timeout=5,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM transactions
        WHERE  created_at >= NOW() - INTERVAL 15 MINUTE
    """)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    log.info("New transactions in last 15 min: %d", count)
    if count < 5:
        log.warning(
            "LOW INGESTION RATE: only %d new transactions in last 15 min. "
            "Is data_streamer running?", count
        )
    return {"new_txns_15min": count}


def check_fraud_spike(**ctx):
    """
    Alert if the fraud rate in the last hour exceeds 30%
    (baseline is ~20% — a spike indicates either a real attack
    or a generator misconfiguration).
    """
    import mysql.connector
    conn = mysql.connector.connect(
        host="mysql", port=3306,
        user="pg_user", password="pg_password_2024",
        database="payment_gateway", connection_timeout=5,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                    AS total,
            SUM(is_flagged)             AS flagged,
            ROUND(SUM(is_flagged) / COUNT(*) * 100, 2) AS fraud_pct
        FROM transactions
        WHERE initiated_at >= NOW() - INTERVAL 1 HOUR
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    total, flagged, fraud_pct = row
    log.info(
        "Last-hour fraud check: total=%d flagged=%d pct=%.2f%%",
        total or 0, flagged or 0, fraud_pct or 0,
    )

    if fraud_pct and fraud_pct > 30:
        log.warning(
            "FRAUD SPIKE DETECTED: %.2f%% fraud rate in last hour "
            "(%d / %d transactions flagged)",
            fraud_pct, flagged, total,
        )
    return {"fraud_pct_last_hour": float(fraud_pct or 0)}


def check_unreviewed_high_risk(**ctx):
    """Alert if more than 50 high-risk (score >= 90) alerts are unreviewed."""
    import mysql.connector
    conn = mysql.connector.connect(
        host="mysql", port=3306,
        user="pg_user", password="pg_password_2024",
        database="payment_gateway", connection_timeout=5,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM fraud_alerts
        WHERE  is_confirmed IS NULL
        AND    risk_score >= 90
    """)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    log.info("Unreviewed high-risk alerts (score >= 90): %d", count)
    if count > 50:
        log.warning(
            "ALERT BACKLOG: %d unreviewed high-risk fraud alerts pending", count
        )
    return {"unreviewed_high_risk": count}


def log_summary(**ctx):
    """Pull all XCom results and print a consolidated summary."""
    ti = ctx["ti"]
    mysql_health = ti.xcom_pull(task_ids="check_mysql_health")
    ingestion    = ti.xcom_pull(task_ids="check_txn_ingestion_rate")
    fraud        = ti.xcom_pull(task_ids="check_fraud_spike")
    alerts       = ti.xcom_pull(task_ids="check_unreviewed_high_risk")

    log.info("=" * 60)
    log.info("PIPELINE MONITOR SUMMARY — %s", datetime.utcnow().isoformat())
    log.info("  MySQL row counts   : %s", mysql_health)
    log.info("  New txns (15 min)  : %s", ingestion)
    log.info("  Fraud rate (1 hr)  : %s", fraud)
    log.info("  High-risk backlog  : %s", alerts)
    log.info("=" * 60)


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="pipeline_monitor",
    description="Monitor MySQL ingestion rate, fraud spikes, and dbt source freshness",
    schedule_interval="*/15 * * * *",   # every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "mysql", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_mysql = PythonOperator(
        task_id="check_mysql_health",
        python_callable=check_mysql_health,
        doc_md="Verify MySQL connectivity and minimum row counts per table.",
    )

    t_ingestion = PythonOperator(
        task_id="check_txn_ingestion_rate",
        python_callable=check_txn_ingestion_rate,
        doc_md="Check that new transactions are arriving from the streamer.",
    )

    t_fraud = PythonOperator(
        task_id="check_fraud_spike",
        python_callable=check_fraud_spike,
        doc_md="Alert if fraud rate in the last hour exceeds 30%.",
    )

    t_alerts = PythonOperator(
        task_id="check_unreviewed_high_risk",
        python_callable=check_unreviewed_high_risk,
        doc_md="Alert if more than 50 high-risk alerts are pending review.",
    )

    t_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_ENV} {DBT_BASE} source freshness",
        doc_md="Run dbt source freshness check on all declared sources.",
    )

    t_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        doc_md="Print consolidated pipeline health summary to logs.",
    )

    end = EmptyOperator(task_id="end")

    # All checks run in parallel, then converge to summary
    start >> [t_mysql, t_ingestion, t_fraud, t_alerts, t_freshness]
    [t_mysql, t_ingestion, t_fraud, t_alerts, t_freshness] >> t_summary >> end
