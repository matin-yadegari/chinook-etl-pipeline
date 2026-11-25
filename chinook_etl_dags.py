
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from scripts.common import DW_TABLE_NAMES, TABLE_NAMES

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="chinook_etl_cdc",
    description="Chinook: SQL Server CDC -> Postgres staging -> pandas -> ClickHouse DWH",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,         
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=30),
    tags=["chinook", "cdc", "etl"],
    doc_md=__doc__,
)
def chinook_etl_cdc():
    @task
    def get_max_lsn() -> str:
        from scripts.cdc_extractor import get_max_lsn as _get

        return _get()

    @task(max_active_tis_per_dag=4)
    def extract(table: str, to_lsn: str) -> dict:
        from scripts.cdc_extractor import extract_table

        return extract_table(table, to_lsn)

    @task(max_active_tis_per_dag=4)
    def clean(table: str) -> dict:
        from scripts.cleaner import clean_table

        return clean_table(table)

    @task
    def load_dw(table: str) -> dict:
        from scripts.clickhouse_loader import load_table

        return load_table(table)

    @task
    def reconcile() -> list:
        from scripts.clickhouse_loader import reconcile as _reconcile

        return _reconcile()

    lsn = get_max_lsn()
    extracted = extract.partial(to_lsn=lsn).expand(table=TABLE_NAMES)
    cleaned = clean.expand(table=TABLE_NAMES)
    loaded = load_dw.expand(table=DW_TABLE_NAMES)

    extracted >> cleaned >> loaded >> reconcile()


@dag(
    dag_id="chinook_etl_full_reload",
    description="DANGER: reset the pipeline state and force a fresh snapshot",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng"},
    tags=["chinook", "maintenance"],
    doc_md=__doc__,
)
def chinook_etl_full_reload():
    @task
    def reset_state() -> None:
        from scripts.common import full_reset

        full_reset()

    reset_state()


chinook_etl_cdc()
chinook_etl_full_reload()
