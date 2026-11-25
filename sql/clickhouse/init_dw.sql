-- =============================================================================
-- ClickHouse data warehouse for Chinook  (star schema)
-- Engine: ReplacingMergeTree(_version, _is_deleted)  -> CDC updates / deletes.
-- =============================================================================
CREATE DATABASE IF NOT EXISTS dw;

-- ------------------------------------------------------------------ dimensions
CREATE TABLE IF NOT EXISTS dw.dim_customer
(
    customer_id     UInt32,
    first_name      String,
    last_name       String,
    full_name       String,
    company         String,
    address         String,
    city            String,
    state           String,
    country         String,
    postal_code     String,
    phone           String,
    email           String,
    support_rep_id  Nullable(UInt32),
    _version        UInt64,
    _is_deleted     UInt8
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dw.dim_employee
(
    employee_id     UInt32,
    first_name      String,
    last_name       String,
    full_name       String,
    title           String,
    reports_to      Nullable(UInt32),
    birth_date      Nullable(Date32),  
    hire_date       Nullable(Date32),
    city            String,
    state           String,
    country         String,
    email           String,
    _version        UInt64,
    _is_deleted     UInt8
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY employee_id;

CREATE TABLE IF NOT EXISTS dw.dim_track
(
    track_id          UInt32,
    track_name        String,
    album_id          UInt32,
    album_title       String,
    artist_id         UInt32,
    artist_name       String,
    genre_id          UInt32,
    genre_name        String,
    media_type_id     UInt32,
    media_type_name   String,
    composer          String,
    milliseconds      UInt32,
    bytes             UInt64,
    unit_price        Decimal(10, 2),
    _version          UInt64,
    _is_deleted       UInt8
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY track_id;

CREATE TABLE IF NOT EXISTS dw.dim_date
(
    date_key     UInt32,
    date         Date,
    year         UInt16,
    quarter      UInt8,
    month        UInt8,
    month_name   String,
    day          UInt8,
    day_of_week  UInt8,
    day_name     String,
    is_weekend   UInt8
)
ENGINE = MergeTree
ORDER BY date_key;

INSERT INTO dw.dim_date
SELECT toYYYYMMDD(d), d, toYear(d), toQuarter(d), toMonth(d), monthName(d),
       toDayOfMonth(d), toDayOfWeek(d), dateName('weekday', d), toDayOfWeek(d) >= 6
FROM (SELECT toDate('2000-01-01') + number AS d FROM numbers(14610));  

-- ----------------------------------------------------------------------- fact
-- grain: one invoice line
CREATE TABLE IF NOT EXISTS dw.fact_sales
(
    invoice_line_id  UInt32,
    invoice_id       UInt32,
    customer_id      UInt32,
    track_id         UInt32,
    invoice_date     DateTime,
    date_key         UInt32,
    billing_country  String,
    billing_city     String,
    unit_price       Decimal(10, 2),
    quantity         UInt32,
    line_total       Decimal(18, 2),
    _version         UInt64,
    _is_deleted      UInt8
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY invoice_line_id;

-- ----------------------------------------------------------------------- views
CREATE OR REPLACE VIEW dw.v_dim_customer AS SELECT * FROM dw.dim_customer FINAL WHERE _is_deleted = 0;
CREATE OR REPLACE VIEW dw.v_dim_employee AS SELECT * FROM dw.dim_employee FINAL WHERE _is_deleted = 0;
CREATE OR REPLACE VIEW dw.v_dim_track    AS SELECT * FROM dw.dim_track    FINAL WHERE _is_deleted = 0;
CREATE OR REPLACE VIEW dw.v_fact_sales   AS SELECT * FROM dw.fact_sales   FINAL WHERE _is_deleted = 0;

-- Wide, dashboard-friendly view (fact + customer + track + support rep + date)
CREATE OR REPLACE VIEW dw.v_sales AS
SELECT
    f.invoice_line_id  AS invoice_line_id,
    f.invoice_id       AS invoice_id,
    f.invoice_date     AS invoice_date,
    f.date_key         AS date_key,
    f.billing_country  AS billing_country,
    f.billing_city     AS billing_city,
    f.unit_price       AS unit_price,
    f.quantity         AS quantity,
    f.line_total       AS line_total,
    f.customer_id      AS customer_id,
    c.full_name        AS customer_name,
    c.country          AS customer_country,
    e.full_name        AS support_rep,
    f.track_id         AS track_id,
    t.track_name       AS track_name,
    t.album_title      AS album_title,
    t.artist_name      AS artist_name,
    t.genre_name       AS genre_name,
    t.media_type_name  AS media_type_name,
    d.year             AS year,
    d.month_name       AS month_name,
    d.day_name         AS day_name
FROM dw.v_fact_sales AS f
LEFT JOIN dw.v_dim_customer AS c ON f.customer_id = c.customer_id
LEFT JOIN dw.v_dim_employee AS e ON c.support_rep_id = e.employee_id
LEFT JOIN dw.v_dim_track    AS t ON f.track_id = t.track_id
LEFT JOIN dw.dim_date       AS d ON f.date_key = d.date_key;
