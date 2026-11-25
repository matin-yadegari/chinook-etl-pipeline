
from __future__ import annotations

import sys
import time

from scripts.common import (
    DW_TABLE_NAMES,
    ch_client,
    get_stage_wm,
    log,
    pg_conn,
    push_metrics,
    set_stage_wm,
)

CHUNK = 50_000

DW_TABLES: dict[str, dict] = {
    "dim_customer": {
        "sql": """
            SELECT customer_id,
                   COALESCE(first_name, '')                             AS first_name,
                   COALESCE(last_name, '')                              AS last_name,
                   btrim(concat_ws(' ', first_name, last_name))         AS full_name,
                   COALESCE(company, 'Unknown')                         AS company,
                   COALESCE(address, 'Unknown')                         AS address,
                   COALESCE(city, 'Unknown')                            AS city,
                   COALESCE(state, 'Unknown')                           AS state,
                   COALESCE(country, 'Unknown')                         AS country,
                   COALESCE(postal_code, 'Unknown')                     AS postal_code,
                   COALESCE(phone, 'Unknown')                           AS phone,
                   COALESCE(email, 'Unknown')                           AS email,
                   support_rep_id,
                   _version,
                   _is_deleted::int                                     AS _is_deleted
            FROM clean.customer
            WHERE _version > %(wm)s
            ORDER BY _version
        """,
        "count_pg": "SELECT count(*) FROM clean.customer WHERE NOT _is_deleted",
        "count_ch": "SELECT count() FROM dw.v_dim_customer",
    },
    "dim_employee": {
        "sql": """
            SELECT employee_id,
                   COALESCE(first_name, '')                             AS first_name,
                   COALESCE(last_name, '')                              AS last_name,
                   btrim(concat_ws(' ', first_name, last_name))         AS full_name,
                   COALESCE(title, 'Unknown')                           AS title,
                   reports_to,
                   -- dw.dim_employee uses Date32 (1900-01-01 .. 2299-12-31); anything outside -> NULL
                   CASE WHEN birth_date >= DATE '1900-01-01' AND birth_date < DATE '2300-01-01'
                        THEN birth_date::date END                       AS birth_date,
                   CASE WHEN hire_date  >= DATE '1900-01-01' AND hire_date  < DATE '2300-01-01'
                        THEN hire_date::date END                        AS hire_date,
                   COALESCE(city, 'Unknown')                            AS city,
                   COALESCE(state, 'Unknown')                           AS state,
                   COALESCE(country, 'Unknown')                         AS country,
                   COALESCE(email, 'Unknown')                           AS email,
                   _version,
                   _is_deleted::int                                     AS _is_deleted
            FROM clean.employee
            WHERE _version > %(wm)s
            ORDER BY _version
        """,
        "count_pg": "SELECT count(*) FROM clean.employee WHERE NOT _is_deleted",
        "count_ch": "SELECT count() FROM dw.v_dim_employee",
    },
    "dim_track": {
        # denormalised: track + album + artist + genre + media type
        "sql": """
            SELECT t.track_id,
                   t.name                                   AS track_name,
                   COALESCE(t.album_id, 0)                  AS album_id,
                   COALESCE(al.title, 'Unknown')            AS album_title,
                   COALESCE(ar.artist_id, 0)                AS artist_id,
                   COALESCE(ar.name, 'Unknown')             AS artist_name,
                   COALESCE(t.genre_id, 0)                  AS genre_id,
                   COALESCE(g.name, 'Unknown')              AS genre_name,
                   COALESCE(t.media_type_id, 0)             AS media_type_id,
                   COALESCE(m.name, 'Unknown')              AS media_type_name,
                   COALESCE(t.composer, 'Unknown')          AS composer,
                   t.milliseconds,
                   COALESCE(t.bytes, 0)                     AS bytes,
                   t.unit_price,
                   GREATEST(t._version, COALESCE(al._version, 0), COALESCE(ar._version, 0),
                            COALESCE(g._version, 0), COALESCE(m._version, 0)) AS _version,
                   t._is_deleted::int                       AS _is_deleted
            FROM clean.track t
            LEFT JOIN clean.album      al ON al.album_id      = t.album_id
            LEFT JOIN clean.artist     ar ON ar.artist_id     = al.artist_id
            LEFT JOIN clean.genre      g  ON g.genre_id       = t.genre_id
            LEFT JOIN clean.media_type m  ON m.media_type_id  = t.media_type_id
            WHERE GREATEST(t._version, COALESCE(al._version, 0), COALESCE(ar._version, 0),
                           COALESCE(g._version, 0), COALESCE(m._version, 0)) > %(wm)s
            ORDER BY _version
        """,
        "count_pg": "SELECT count(*) FROM clean.track WHERE NOT _is_deleted",
        "count_ch": "SELECT count() FROM dw.v_dim_track",
    },
    "fact_sales": {
        # grain: one invoice line
        "sql": """
            SELECT il.invoice_line_id,
                   il.invoice_id,
                   i.customer_id,
                   il.track_id,
                   i.invoice_date,
                   to_char(i.invoice_date, 'YYYYMMDD')::int           AS date_key,
                   COALESCE(i.billing_country, 'Unknown')             AS billing_country,
                   COALESCE(i.billing_city, 'Unknown')                AS billing_city,
                   il.unit_price,
                   il.quantity,
                   il.unit_price * il.quantity                        AS line_total,
                   GREATEST(il._version, i._version)                  AS _version,
                   (il._is_deleted OR i._is_deleted)::int             AS _is_deleted
            FROM clean.invoice_line il
            JOIN clean.invoice i ON i.invoice_id = il.invoice_id
            WHERE GREATEST(il._version, i._version) > %(wm)s
            ORDER BY _version
        """,
        "count_pg": """
            SELECT count(*)
            FROM clean.invoice_line il
            JOIN clean.invoice i ON i.invoice_id = il.invoice_id
            WHERE NOT il._is_deleted AND NOT i._is_deleted
        """,
        "count_ch": "SELECT count() FROM dw.v_fact_sales",
    },
}


def load_table(name: str) -> dict:
    cfg = DW_TABLES[name]
    started = time.time()
    ch = ch_client()
    total = 0

    with pg_conn() as conn, conn.cursor() as cur:
        wm = get_stage_wm(cur, "dw", name)
        max_version = wm
        cur.execute(cfg["sql"], {"wm": wm})
        columns = [d[0] for d in cur.description]
        v_idx = columns.index("_version")
        while True:
            rows = cur.fetchmany(CHUNK)
            if not rows:
                break
            ch.insert(name, rows, column_names=columns, database="dw")
            total += len(rows)
            max_version = max(max_version, max(r[v_idx] for r in rows))

        if total:
            set_stage_wm(cur, "dw", name, max_version)

    duration = time.time() - started
    log.info("load    %-15s rows=%d (%.1fs)", name, total, duration)
    push_metrics("load_dw", name, rows=total, duration=duration)
    return {"table": name, "rows": total}


def reconcile() -> list[dict]:
    """Compare live row counts: Postgres clean layer vs ClickHouse (FINAL views)."""
    ch = ch_client()
    results, mismatches = [], []
    with pg_conn() as conn, conn.cursor() as cur:
        for name, cfg in DW_TABLES.items():
            cur.execute(cfg["count_pg"])
            pg_count = int(cur.fetchone()[0])
            ch_count = int(ch.query(cfg["count_ch"]).result_rows[0][0])
            diff = ch_count - pg_count
            results.append({"table": name, "postgres": pg_count, "clickhouse": ch_count, "diff": diff})
            push_metrics(
                "reconcile", name, rows=ch_count,
                extra={"etl_reconciliation_diff": ("ClickHouse count minus Postgres count", diff)},
            )
            log.info("reconcile %-14s pg=%d ch=%d diff=%d", name, pg_count, ch_count, diff)
            if diff != 0:
                mismatches.append(name)
    if mismatches:
        raise ValueError(f"Row count mismatch between clean layer and DW: {mismatches}")
    return results


def load_all(tables: list[str] | None = None) -> list[dict]:
    return [load_table(t) for t in (tables or DW_TABLE_NAMES)]


if __name__ == "__main__":
    load_all(sys.argv[1:] or None)
    reconcile()
