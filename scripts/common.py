
from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

log = logging.getLogger("chinook_etl")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} is not set")
    return value


# --------------------------------------------------------------------------- #
# Table specifications
# --------------------------------------------------------------------------- #
# key   = target (snake_case) table name used in Postgres (staging / clean)
# source= table name in SQL Server (schema dbo, CDC capture instance dbo_<source>)
# pk    = primary key columns (target names)
# columns = (source column, target column, logical type)
#           logical types: integer | text | timestamp | numeric
TABLE_SPECS: dict[str, dict] = {
    "album": {
        "source": "Album",
        "pk": ["album_id"],
        "columns": [
            ("AlbumId", "album_id", "integer"),
            ("Title", "title", "text"),
            ("ArtistId", "artist_id", "integer"),
        ],
    },
    "artist": {
        "source": "Artist",
        "pk": ["artist_id"],
        "columns": [
            ("ArtistId", "artist_id", "integer"),
            ("Name", "name", "text"),
        ],
    },
    "customer": {
        "source": "Customer",
        "pk": ["customer_id"],
        "columns": [
            ("CustomerId", "customer_id", "integer"),
            ("FirstName", "first_name", "text"),
            ("LastName", "last_name", "text"),
            ("Company", "company", "text"),
            ("Address", "address", "text"),
            ("City", "city", "text"),
            ("State", "state", "text"),
            ("Country", "country", "text"),
            ("PostalCode", "postal_code", "text"),
            ("Phone", "phone", "text"),
            ("Fax", "fax", "text"),
            ("Email", "email", "text"),
            ("SupportRepId", "support_rep_id", "integer"),
        ],
    },
    "employee": {
        "source": "Employee",
        "pk": ["employee_id"],
        "columns": [
            ("EmployeeId", "employee_id", "integer"),
            ("LastName", "last_name", "text"),
            ("FirstName", "first_name", "text"),
            ("Title", "title", "text"),
            ("ReportsTo", "reports_to", "integer"),
            ("BirthDate", "birth_date", "timestamp"),
            ("HireDate", "hire_date", "timestamp"),
            ("Address", "address", "text"),
            ("City", "city", "text"),
            ("State", "state", "text"),
            ("Country", "country", "text"),
            ("PostalCode", "postal_code", "text"),
            ("Phone", "phone", "text"),
            ("Fax", "fax", "text"),
            ("Email", "email", "text"),
        ],
    },
    "genre": {
        "source": "Genre",
        "pk": ["genre_id"],
        "columns": [
            ("GenreId", "genre_id", "integer"),
            ("Name", "name", "text"),
        ],
    },
    "invoice": {
        "source": "Invoice",
        "pk": ["invoice_id"],
        "columns": [
            ("InvoiceId", "invoice_id", "integer"),
            ("CustomerId", "customer_id", "integer"),
            ("InvoiceDate", "invoice_date", "timestamp"),
            ("BillingAddress", "billing_address", "text"),
            ("BillingCity", "billing_city", "text"),
            ("BillingState", "billing_state", "text"),
            ("BillingCountry", "billing_country", "text"),
            ("BillingPostalCode", "billing_postal_code", "text"),
            ("Total", "total", "numeric(10,2)"),
        ],
    },
    "invoice_line": {
        "source": "InvoiceLine",
        "pk": ["invoice_line_id"],
        "columns": [
            ("InvoiceLineId", "invoice_line_id", "integer"),
            ("InvoiceId", "invoice_id", "integer"),
            ("TrackId", "track_id", "integer"),
            ("UnitPrice", "unit_price", "numeric(10,2)"),
            ("Quantity", "quantity", "integer"),
        ],
    },
    "media_type": {
        "source": "MediaType",
        "pk": ["media_type_id"],
        "columns": [
            ("MediaTypeId", "media_type_id", "integer"),
            ("Name", "name", "text"),
        ],
    },
    "playlist": {
        "source": "Playlist",
        "pk": ["playlist_id"],
        "columns": [
            ("PlaylistId", "playlist_id", "integer"),
            ("Name", "name", "text"),
        ],
    },
    "playlist_track": {
        "source": "PlaylistTrack",
        "pk": ["playlist_id", "track_id"],
        "columns": [
            ("PlaylistId", "playlist_id", "integer"),
            ("TrackId", "track_id", "integer"),
        ],
    },
    "track": {
        "source": "Track",
        "pk": ["track_id"],
        "columns": [
            ("TrackId", "track_id", "integer"),
            ("Name", "name", "text"),
            ("AlbumId", "album_id", "integer"),
            ("MediaTypeId", "media_type_id", "integer"),
            ("GenreId", "genre_id", "integer"),
            ("Composer", "composer", "text"),
            ("Milliseconds", "milliseconds", "integer"),
            ("Bytes", "bytes", "integer"),
            ("UnitPrice", "unit_price", "numeric(10,2)"),
        ],
    },
}

TABLE_NAMES: list[str] = list(TABLE_SPECS)
DW_TABLE_NAMES: list[str] = ["dim_customer", "dim_employee", "dim_track", "fact_sales"]


def target_columns(table: str) -> list[str]:
    return [c[1] for c in TABLE_SPECS[table]["columns"]]


# --------------------------------------------------------------------------- #
# Connections
# --------------------------------------------------------------------------- #
@contextmanager
def mssql_conn() -> Iterator:
    """SQL Server connection (autocommit, read side only)."""
    import pymssql

    conn = pymssql.connect(
        server=env("MSSQL_HOST", "sqlserver"),
        port=int(env("MSSQL_PORT", "1433")),
        user=env("MSSQL_USER", "sa"),
        password=env("MSSQL_SA_PASSWORD"),
        database=env("MSSQL_DATABASE", "Chinook"),
        login_timeout=30,
        autocommit=True,
        charset="UTF-8",
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def pg_conn() -> Iterator:
    """Postgres connection: commit on success, rollback on error, always close."""
    conn = psycopg2.connect(
        host=env("PG_HOST", "postgres-staging"),
        port=int(env("PG_PORT", "5432")),
        dbname=env("PG_DB"),
        user=env("PG_USER"),
        password=env("PG_PASSWORD"),
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def pg_engine():
    """SQLAlchemy engine (used by pandas.read_sql)."""
    url = URL.create(
        "postgresql+psycopg2",
        username=env("PG_USER"),
        password=env("PG_PASSWORD"),
        host=env("PG_HOST", "postgres-staging"),
        port=int(env("PG_PORT", "5432")),
        database=env("PG_DB"),
    )
    return create_engine(url, pool_pre_ping=True)


def ch_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=env("CH_HOST", "clickhouse"),
        port=int(env("CH_HTTP_PORT", "8123")),
        username=env("CH_USER"),
        password=env("CH_PASSWORD"),
        database=env("CH_DB", "dw"),
    )


# --------------------------------------------------------------------------- #
# Per-stage watermarks (etl_control.stage_watermark)
# --------------------------------------------------------------------------- #
def get_stage_wm(cur, stage: str, table: str) -> int:
    cur.execute(
        "SELECT last_value FROM etl_control.stage_watermark "
        "WHERE stage = %s AND table_name = %s",
        (stage, table),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def set_stage_wm(cur, stage: str, table: str, value: int) -> None:
    cur.execute(
        """
        INSERT INTO etl_control.stage_watermark (stage, table_name, last_value)
        VALUES (%s, %s, %s)
        ON CONFLICT (stage, table_name)
        DO UPDATE SET last_value = EXCLUDED.last_value, updated_at = now()
        """,
        (stage, table, value),
    )


# --------------------------------------------------------------------------- #
# Prometheus Pushgateway metrics
# --------------------------------------------------------------------------- #
def push_metrics(
    stage: str,
    table: str,
    rows: int | None = None,
    duration: float | None = None,
    success: bool = True,
    extra: dict[str, tuple[str, float]] | None = None,
) -> None:
    """Push gauges for (stage, table). Never raises: monitoring must not break ETL."""
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        labels = ["stage", "table"]
        if rows is not None:
            Gauge("etl_rows_processed", "Rows processed in the last run", labels,
                  registry=registry).labels(stage, table).set(rows)
        if duration is not None:
            Gauge("etl_stage_duration_seconds", "Duration of the last run", labels,
                  registry=registry).labels(stage, table).set(duration)
        if success:
            Gauge("etl_last_success_timestamp_seconds", "Unix time of last success", labels,
                  registry=registry).labels(stage, table).set(time.time())
        for name, (help_text, value) in (extra or {}).items():
            Gauge(name, help_text, labels, registry=registry).labels(stage, table).set(value)
        push_to_gateway(
            env("PUSHGATEWAY_URL", "pushgateway:9091"),
            job="chinook_etl",
            registry=registry,
            grouping_key={"stage": stage, "table": table},
            timeout=5,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not push metrics to Pushgateway: %s", exc)


# --------------------------------------------------------------------------- #
# Maintenance
# --------------------------------------------------------------------------- #
def full_reset() -> None:
    """Wipe staging / clean / watermarks and the ClickHouse DW tables.

    After this the next run of the `chinook_etl_cdc` DAG performs a fresh
    snapshot of every table and then continues with CDC.
    """
    with pg_conn() as conn, conn.cursor() as cur:
        for table in TABLE_NAMES:
            cur.execute(f"TRUNCATE staging.{table}, clean.{table} RESTART IDENTITY")
        cur.execute(
            "TRUNCATE etl_control.cdc_watermark, etl_control.stage_watermark, "
            "etl_control.dq_rejects"
        )
    ch = ch_client()
    for table in DW_TABLE_NAMES:
        ch.command(f"TRUNCATE TABLE dw.{table}")
    log.info("Pipeline state reset. Next DAG run will take a full snapshot.")
