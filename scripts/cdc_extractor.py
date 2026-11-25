
from __future__ import annotations

import sys
import time

from psycopg2.extras import execute_values

from scripts.common import (
    TABLE_NAMES,
    TABLE_SPECS,
    log,
    mssql_conn,
    pg_conn,
    push_metrics,
    target_columns,
)

CHUNK = 10_000
OP_MAP = {1: "D", 2: "I", 4: "U"}  # 3 = "before update" image, not returned by 'all'


def _scalar(cur, sql: str, params: tuple = ()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None


def get_max_lsn() -> str:
    """Current max CDC LSN as hex string ('0x...'). Used as the common upper bound."""
    with mssql_conn() as conn:
        cur = conn.cursor()
        lsn = _scalar(cur, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)")
    if not lsn:
        raise RuntimeError(
            "sys.fn_cdc_get_max_lsn() returned NULL - is CDC enabled and is "
            "SQL Server Agent running?"
        )
    return lsn


def _insert_staging(pg_cur, table: str, rows: list[tuple]) -> None:
    cols = ", ".join(target_columns(table) + ["_cdc_op", "_cdc_lsn", "_cdc_seqval"])
    execute_values(
        pg_cur,
        f"INSERT INTO staging.{table} ({cols}) VALUES %s",
        rows,
        page_size=CHUNK,
    )


def _snapshot(src_cur, pg_cur, table: str) -> int:
    spec = TABLE_SPECS[table]
    cols = ", ".join(f"[{c[0]}]" for c in spec["columns"])
    src_cur.execute(f"SELECT {cols} FROM dbo.[{spec['source']}]")
    total = 0
    while True:
        batch = src_cur.fetchmany(CHUNK)
        if not batch:
            break
        _insert_staging(pg_cur, table, [tuple(r) + ("S", None, None) for r in batch])
        total += len(batch)
    return total


def _changes(src_cur, pg_cur, table: str, from_lsn: str, to_lsn: str) -> int:
    spec = TABLE_SPECS[table]
    cols = ", ".join(f"[{c[0]}]" for c in spec["columns"])
    instance = f"dbo_{spec['source']}"
    src_cur.execute(
        f"""
        SELECT {cols},
               __$operation,
               CONVERT(varchar(22), __$start_lsn, 1),
               CONVERT(varchar(22), __$seqval, 1)
        FROM cdc.fn_cdc_get_all_changes_{instance}(
                 CONVERT(binary(10), %s, 1), CONVERT(binary(10), %s, 1), N'all')
        ORDER BY __$start_lsn, __$seqval
        """,
        (from_lsn, to_lsn),
    )
    n_cols = len(spec["columns"])
    total = 0
    while True:
        batch = src_cur.fetchmany(CHUNK)
        if not batch:
            break
        rows = []
        for r in batch:
            op = OP_MAP.get(r[n_cols])
            if op is None:
                continue
            rows.append(tuple(r[:n_cols]) + (op, r[n_cols + 1], r[n_cols + 2]))
        if rows:
            _insert_staging(pg_cur, table, rows)
            total += len(rows)
    return total


def extract_table(table: str, to_lsn: str | None = None) -> dict:
    """Extract one table. Returns {'table', 'mode', 'rows', 'to_lsn'}."""
    if table not in TABLE_SPECS:
        raise KeyError(f"Unknown table {table}")
    started = time.time()
    spec = TABLE_SPECS[table]
    instance = f"dbo_{spec['source']}"
    rows = 0
    mode = "cdc"

    with mssql_conn() as src, pg_conn() as pg:
        src_cur = src.cursor()
        pg_cur = pg.cursor()
        to_lsn = to_lsn or _scalar(
            src_cur, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)"
        )
        if not to_lsn:
            raise RuntimeError("CDC max LSN is NULL (CDC not ready)")

        pg_cur.execute(
            "SELECT last_lsn FROM etl_control.cdc_watermark WHERE table_name = %s",
            (table,),
        )
        row = pg_cur.fetchone()
        last_lsn = row[0] if row else None

        if last_lsn is None:
            mode = "snapshot"
            rows = _snapshot(src_cur, pg_cur, table)
        elif last_lsn >= to_lsn:
            mode = "no-change"
        else:
            from_lsn = _scalar(
                src_cur,
                "SELECT CONVERT(varchar(22), "
                "sys.fn_cdc_increment_lsn(CONVERT(binary(10), %s, 1)), 1)",
                (last_lsn,),
            )
            min_lsn = _scalar(
                src_cur,
                "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn(%s), 1)",
                (instance,),
            )
            if min_lsn is None:
                raise RuntimeError(f"No CDC capture instance {instance}")
            if from_lsn < min_lsn:
                log.warning(
                    "%s: watermark %s is older than CDC min LSN %s (cleanup job "
                    "removed changes). Changes in the gap are lost - run the "
                    "chinook_etl_full_reload DAG.", table, from_lsn, min_lsn)
                from_lsn = min_lsn
            if from_lsn <= to_lsn:
                rows = _changes(src_cur, pg_cur, table, from_lsn, to_lsn)
            else:
                mode = "no-change"

        pg_cur.execute(
            """
            INSERT INTO etl_control.cdc_watermark (table_name, last_lsn)
            VALUES (%s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET last_lsn = EXCLUDED.last_lsn, updated_at = now()
            """,
            (table, to_lsn),
        )

    duration = time.time() - started
    log.info("extract %-15s mode=%-9s rows=%d (%.1fs)", table, mode, rows, duration)
    push_metrics("extract", table, rows=rows, duration=duration)
    return {"table": table, "mode": mode, "rows": rows, "to_lsn": to_lsn}


def extract_all(tables: list[str] | None = None) -> list[dict]:
    lsn = get_max_lsn()
    return [extract_table(t, lsn) for t in (tables or TABLE_NAMES)]


if __name__ == "__main__":
    extract_all(sys.argv[1:] or None)
