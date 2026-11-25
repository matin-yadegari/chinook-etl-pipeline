-- =============================================================================
-- Postgres staging database bootstrap (runs once, on first container start)
--   staging     : append-only CDC change log, one table per source table
--   clean       : cleaned *current state* tables (soft deletes), input of the DW load
--   etl_control : watermarks, global version sequence, data-quality rejects
-- Generated from scripts/common.py::TABLE_SPECS - keep both in sync.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS etl_control;

-- ---------------------------------------------------------------- control ----
CREATE TABLE IF NOT EXISTS etl_control.cdc_watermark (
    table_name  text PRIMARY KEY,
    last_lsn    varchar(22) NOT NULL,          -- last SQL Server LSN extracted (hex)
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_control.stage_watermark (
    stage       text   NOT NULL,               -- 'clean' | 'dw'
    table_name  text   NOT NULL,
    last_value  bigint NOT NULL DEFAULT 0,     -- clean: last _stg_id, dw: last _version
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stage, table_name)
);

CREATE SEQUENCE IF NOT EXISTS etl_control.version_seq;

CREATE TABLE IF NOT EXISTS etl_control.dq_rejects (
    id          bigserial PRIMARY KEY,
    table_name  text   NOT NULL,
    pk_value    text,
    reason      text   NOT NULL,
    payload     jsonb,
    stg_id      bigint,
    rejected_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_rejects_table ON etl_control.dq_rejects (table_name, rejected_at DESC);

-- ======================================================= album (Album) ===
CREATE TABLE IF NOT EXISTS staging.album (
    _stg_id         bigserial PRIMARY KEY,
    album_id              integer,
    title                 text,
    artist_id             integer,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_album_pk ON staging.album (album_id);

CREATE TABLE IF NOT EXISTS clean.album (
    album_id              integer,
    title                 text,
    artist_id             integer,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (album_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_album_version ON clean.album (_version);

-- ======================================================= artist (Artist) ===
CREATE TABLE IF NOT EXISTS staging.artist (
    _stg_id         bigserial PRIMARY KEY,
    artist_id             integer,
    name                  text,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_artist_pk ON staging.artist (artist_id);

CREATE TABLE IF NOT EXISTS clean.artist (
    artist_id             integer,
    name                  text,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (artist_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_artist_version ON clean.artist (_version);

-- ======================================================= customer (Customer) ===
CREATE TABLE IF NOT EXISTS staging.customer (
    _stg_id         bigserial PRIMARY KEY,
    customer_id           integer,
    first_name            text,
    last_name             text,
    company               text,
    address               text,
    city                  text,
    state                 text,
    country               text,
    postal_code           text,
    phone                 text,
    fax                   text,
    email                 text,
    support_rep_id        integer,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_customer_pk ON staging.customer (customer_id);

CREATE TABLE IF NOT EXISTS clean.customer (
    customer_id           integer,
    first_name            text,
    last_name             text,
    company               text,
    address               text,
    city                  text,
    state                 text,
    country               text,
    postal_code           text,
    phone                 text,
    fax                   text,
    email                 text,
    support_rep_id        integer,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_customer_version ON clean.customer (_version);

-- ======================================================= employee (Employee) ===
CREATE TABLE IF NOT EXISTS staging.employee (
    _stg_id         bigserial PRIMARY KEY,
    employee_id           integer,
    last_name             text,
    first_name            text,
    title                 text,
    reports_to            integer,
    birth_date            timestamp,
    hire_date             timestamp,
    address               text,
    city                  text,
    state                 text,
    country               text,
    postal_code           text,
    phone                 text,
    fax                   text,
    email                 text,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_employee_pk ON staging.employee (employee_id);

CREATE TABLE IF NOT EXISTS clean.employee (
    employee_id           integer,
    last_name             text,
    first_name            text,
    title                 text,
    reports_to            integer,
    birth_date            timestamp,
    hire_date             timestamp,
    address               text,
    city                  text,
    state                 text,
    country               text,
    postal_code           text,
    phone                 text,
    fax                   text,
    email                 text,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (employee_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_employee_version ON clean.employee (_version);

-- ======================================================= genre (Genre) ===
CREATE TABLE IF NOT EXISTS staging.genre (
    _stg_id         bigserial PRIMARY KEY,
    genre_id              integer,
    name                  text,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_genre_pk ON staging.genre (genre_id);

CREATE TABLE IF NOT EXISTS clean.genre (
    genre_id              integer,
    name                  text,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (genre_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_genre_version ON clean.genre (_version);

-- ======================================================= invoice (Invoice) ===
CREATE TABLE IF NOT EXISTS staging.invoice (
    _stg_id         bigserial PRIMARY KEY,
    invoice_id            integer,
    customer_id           integer,
    invoice_date          timestamp,
    billing_address       text,
    billing_city          text,
    billing_state         text,
    billing_country       text,
    billing_postal_code   text,
    total                 numeric(10,2),
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_invoice_pk ON staging.invoice (invoice_id);

CREATE TABLE IF NOT EXISTS clean.invoice (
    invoice_id            integer,
    customer_id           integer,
    invoice_date          timestamp,
    billing_address       text,
    billing_city          text,
    billing_state         text,
    billing_country       text,
    billing_postal_code   text,
    total                 numeric(10,2),
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (invoice_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_invoice_version ON clean.invoice (_version);

-- ======================================================= invoice_line (InvoiceLine) ===
CREATE TABLE IF NOT EXISTS staging.invoice_line (
    _stg_id         bigserial PRIMARY KEY,
    invoice_line_id       integer,
    invoice_id            integer,
    track_id              integer,
    unit_price            numeric(10,2),
    quantity              integer,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_invoice_line_pk ON staging.invoice_line (invoice_line_id);

CREATE TABLE IF NOT EXISTS clean.invoice_line (
    invoice_line_id       integer,
    invoice_id            integer,
    track_id              integer,
    unit_price            numeric(10,2),
    quantity              integer,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (invoice_line_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_invoice_line_version ON clean.invoice_line (_version);

-- ======================================================= media_type (MediaType) ===
CREATE TABLE IF NOT EXISTS staging.media_type (
    _stg_id         bigserial PRIMARY KEY,
    media_type_id         integer,
    name                  text,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_media_type_pk ON staging.media_type (media_type_id);

CREATE TABLE IF NOT EXISTS clean.media_type (
    media_type_id         integer,
    name                  text,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (media_type_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_media_type_version ON clean.media_type (_version);

-- ======================================================= playlist (Playlist) ===
CREATE TABLE IF NOT EXISTS staging.playlist (
    _stg_id         bigserial PRIMARY KEY,
    playlist_id           integer,
    name                  text,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_playlist_pk ON staging.playlist (playlist_id);

CREATE TABLE IF NOT EXISTS clean.playlist (
    playlist_id           integer,
    name                  text,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (playlist_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_playlist_version ON clean.playlist (_version);

-- ======================================================= playlist_track (PlaylistTrack) ===
CREATE TABLE IF NOT EXISTS staging.playlist_track (
    _stg_id         bigserial PRIMARY KEY,
    playlist_id           integer,
    track_id              integer,
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_playlist_track_pk ON staging.playlist_track (playlist_id, track_id);

CREATE TABLE IF NOT EXISTS clean.playlist_track (
    playlist_id           integer,
    track_id              integer,
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (playlist_id, track_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_playlist_track_version ON clean.playlist_track (_version);

-- ======================================================= track (Track) ===
CREATE TABLE IF NOT EXISTS staging.track (
    _stg_id         bigserial PRIMARY KEY,
    track_id              integer,
    name                  text,
    album_id              integer,
    media_type_id         integer,
    genre_id              integer,
    composer              text,
    milliseconds          integer,
    bytes                 integer,
    unit_price            numeric(10,2),
    _cdc_op         char(1)     NOT NULL,       -- S=snapshot I=insert U=update D=delete
    _cdc_lsn        varchar(22),
    _cdc_seqval     varchar(22),
    _extracted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_track_pk ON staging.track (track_id);

CREATE TABLE IF NOT EXISTS clean.track (
    track_id              integer,
    name                  text,
    album_id              integer,
    media_type_id         integer,
    genre_id              integer,
    composer              text,
    milliseconds          integer,
    bytes                 integer,
    unit_price            numeric(10,2),
    _is_deleted     boolean     NOT NULL DEFAULT false,
    _version        bigint      NOT NULL,
    _cleaned_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (track_id)
);
CREATE INDEX IF NOT EXISTS ix_clean_track_version ON clean.track (_version);
