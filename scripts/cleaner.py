
from __future__ import annotations

import json
import sys
import time
from decimal import Decimal
from typing import Callable

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text

from scripts.common import (
    TABLE_NAMES,
    TABLE_SPECS,
    env,
    get_stage_wm,
    log,
    pg_conn,
    pg_engine,
    push_metrics,
    set_stage_wm,
    target_columns,
)

BATCH_SIZE = 100_000

COUNTRY_MAP = {
    "USA": "United States",
    "US": "United States",
    "U.S.A.": "United States",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
}


# --------------------------------------------------------------------------- #
# Generic cleaning helpers
# --------------------------------------------------------------------------- #
def _text(s: pd.Series) -> pd.Series:
    """Trim, collapse whitespace, empty string -> NULL."""
    s = s.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return s.mask(s.eq("").fillna(False), pd.NA)


def _money(v):
    if v is None or pd.isna(v):
        return None
    return Decimal(str(v)).quantize(Decimal("0.01"))


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _generic_clean(table: str, df: pd.DataFrame) -> pd.DataFrame:
    for _src, col, typ in TABLE_SPECS[table]["columns"]:
        if typ == "text":
            df[col] = _text(df[col])
        elif typ == "integer":
            df[col] = _num(df[col]).astype("Int64")
        elif typ == "timestamp":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif typ.startswith("numeric"):
            df[col] = df[col].map(_money).astype(object)
    return df


# --------------------------------------------------------------------------- #
# Table specific rules
# --------------------------------------------------------------------------- #
def _clean_customer(df: pd.DataFrame) -> pd.DataFrame:
    df["email"] = df["email"].str.lower()
    df["country"] = df["country"].replace(COUNTRY_MAP)
    return df


def _clean_employee(df: pd.DataFrame) -> pd.DataFrame:
    df["email"] = df["email"].str.lower()
    df["country"] = df["country"].replace(COUNTRY_MAP)
    return df


def _clean_invoice(df: pd.DataFrame) -> pd.DataFrame:
    df["billing_country"] = df["billing_country"].replace(COUNTRY_MAP)
    return df


CLEANERS: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    "customer": _clean_customer,
    "employee": _clean_employee,
    "invoice": _clean_invoice,
}

# (reason, function returning a boolean mask of INVALID rows)
VALIDATORS: dict[str, list[tuple[str, Callable[[pd.DataFrame], pd.Series]]]] = {
    "customer": [("email_missing", lambda d: d["email"].isna())],
    "invoice": [
        ("customer_id_missing", lambda d: d["customer_id"].isna()),
        ("invoice_date_invalid", lambda d: d["invoice_date"].isna()),
        # ClickHouse DateTime range is 1970-01-01 .. 2106-02-07
        ("invoice_date_out_of_range", lambda d: (d["invoice_date"] < pd.Timestamp("1970-01-02"))
                                                 | (d["invoice_date"] > pd.Timestamp("2106-01-01"))),
        ("total_negative", lambda d: _num(d["total"]) < 0),
    ],
    "invoice_line": [
        ("invoice_id_missing", lambda d: d["invoice_id"].isna()),
        ("track_id_missing", lambda d: d["track_id"].isna()),
        ("quantity_not_positive", lambda d: _num(d["quantity"]) <= 0),
        ("unit_price_negative", lambda d: _num(d["unit_price"]) < 0),
    ],
    "track": [
        ("name_missing", lambda d: d["name"].isna()),
        ("milliseconds_not_positive", lambda d: _num(d["milliseconds"]) <= 0),
        ("unit_price_negative", lambda d: _num(d["unit_price"]) < 0),
    ],
    "employee": [
        (
            "hired_before_birth",
            lambda d: d["hire_date"].notna()
            & d["birth_date"].notna()
            & (d["hire_date"] < d["birth_date"]),
        )
    ],
}


# --------------------------------------------------------------------------- #
# Python value conversion for psycopg2
# --------------------------------------------------------------------------- #
def _py(v):
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and v != v:
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return None if np.isnan(v) else float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def _records(df: pd.DataFrame, cols: list[str]) -> list[tuple]:
    sub = df[cols].astype(object)
    return [tuple(_py(v) for v in row) for row in sub.itertuples(index=False, name=None)]


# --------------------------------------------------------------------------- #
# Core
# --------------------------------------------------------------------------- #
def process_frame(table: str, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean a raw staging frame. Returns (good_rows, rejected_rows[with `_reason`])."""
    spec = TABLE_SPECS[table]
    df = df.sort_values("_stg_id").drop_duplicates(spec["pk"], keep="last").copy()
    df["_is_deleted"] = df["_cdc_op"].eq("D")
    df = _generic_clean(table, df)
    if table in CLEANERS:
        df = CLEANERS[table](df)

    reason = pd.Series([None] * len(df), index=df.index, dtype="object")
    checks = [("pk_missing", lambda d: d[spec["pk"]].isna().any(axis=1))]
    checks += VALIDATORS.get(table, [])
    for name, fn in checks:
        mask = fn(df).fillna(False).astype(bool) & reason.isna() & ~df["_is_deleted"]
        reason[mask] = name
    bad = reason.notna()
    rejected = df[bad].copy()
    rejected["_reason"] = reason[bad]
    return df[~bad].copy(), rejected


def clean_table(table: str, batch_size: int = BATCH_SIZE) -> dict:
    spec = TABLE_SPECS[table]
    cols = target_columns(table)
    pk = spec["pk"]
    started = time.time()
    n_in = n_good = n_bad = 0
    engine = pg_engine()

    try:
        while True:
            with pg_conn() as conn, conn.cursor() as cur:
                wm = get_stage_wm(cur, "clean", table)
                df = pd.read_sql(
                    text(
                        f"SELECT * FROM staging.{table} WHERE _stg_id > :wm "
                        "ORDER BY _stg_id LIMIT :lim"
                    ),
                    engine,
                    params={"wm": wm, "lim": batch_size},
                )
                if df.empty:
                    break
                last_id = int(df["_stg_id"].max())
                n_in += len(df)

                good, rejected = process_frame(table, df)

                if len(rejected):
                    payloads = []
                    for _, r in rejected.iterrows():
                        pk_val = "|".join(str(r[c]) for c in pk)
                        payload = json.dumps(
                            {c: _py(r[c]) for c in cols}, default=str, ensure_ascii=False
                        )
                        payloads.append((table, pk_val, r["_reason"], payload, int(r["_stg_id"])))
                    execute_values(
                        cur,
                        "INSERT INTO etl_control.dq_rejects "
                        "(table_name, pk_value, reason, payload, stg_id) VALUES %s",
                        payloads,
                        template="(%s, %s, %s, %s::jsonb, %s)",
                    )
                    n_bad += len(rejected)

                if len(good):
                    good = good.sort_values("_stg_id")
                    cur.execute(
                        "SELECT nextval('etl_control.version_seq') "
                        "FROM generate_series(1, %s)",
                        (len(good),),
                    )
                    good["_version"] = sorted(r[0] for r in cur.fetchall())
                    all_cols = cols + ["_is_deleted", "_version"]
                    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in all_cols)
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO clean.{table} ({', '.join(all_cols)}) VALUES %s
                        ON CONFLICT ({', '.join(pk)}) DO UPDATE
                           SET {updates}, _cleaned_at = now()
                         WHERE clean.{table}._version < EXCLUDED._version
                        """,
                        _records(good, all_cols),
                        page_size=5000,
                    )
                    n_good += len(good)

                set_stage_wm(cur, "clean", table, last_id)

        # retention: drop already processed staging rows older than N days
        with pg_conn() as conn, conn.cursor() as cur:
            wm = get_stage_wm(cur, "clean", table)
            cur.execute(
                f"DELETE FROM staging.{table} WHERE _stg_id <= %s "
                "AND _extracted_at < now() - make_interval(days => %s)",
                (wm, int(env("STAGING_RETENTION_DAYS", "7"))),
            )
    finally:
        engine.dispose()

    duration = time.time() - started
    log.info(
        "clean   %-15s in=%d good=%d rejected=%d (%.1fs)",
        table, n_in, n_good, n_bad, duration,
    )
    push_metrics(
        "clean", table, rows=n_good, duration=duration,
        extra={"etl_rejected_rows": ("Rows rejected by data quality rules", n_bad)},
    )
    return {"table": table, "in": n_in, "good": n_good, "rejected": n_bad}


def clean_all(tables: list[str] | None = None) -> list[dict]:
    return [clean_table(t) for t in (tables or TABLE_NAMES)]


if __name__ == "__main__":
    clean_all(sys.argv[1:] or None)
