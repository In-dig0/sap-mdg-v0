/* @bruin
name: setup.init_db
type: pg.sql
description: >
  Inizializza il database MDG ad ogni run della pipeline.
  Crea schemi, tabelle e sequence se non esistono già.
  Completamente idempotente grazie a IF NOT EXISTS e ON CONFLICT.
  Deve essere il primo asset ad eseguire, prima di pipeline_run_open.
connection: mdg_postgres
@bruin */

-- =============================================================================
-- Schemi
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS stg;

-- =============================================================================
-- stg.check_catalog
-- =============================================================================
CREATE TABLE IF NOT EXISTS stg.check_catalog (
    check_id        VARCHAR(20)  NOT NULL,
    check_desc      TEXT         NOT NULL,
    category        VARCHAR(50)  NOT NULL,
    target_table    VARCHAR(100) NOT NULL,
    target_field    VARCHAR(100) NOT NULL,
    ref_table       VARCHAR(100),
    check_type      VARCHAR(20)  NOT NULL DEFAULT 'EXISTENCE',
    severity        VARCHAR(10)  NOT NULL DEFAULT 'Error',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_check_catalog PRIMARY KEY (check_id)
);

-- =============================================================================
-- stg.pipeline_run_seq — sequence progressiva per run_id
-- =============================================================================
CREATE SEQUENCE IF NOT EXISTS stg.pipeline_run_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- =============================================================================
-- stg.pipeline_runs
-- =============================================================================
CREATE TABLE IF NOT EXISTS stg.pipeline_runs (
    run_id          INTEGER      NOT NULL DEFAULT nextval('stg.pipeline_run_seq'),
    pipeline_name   VARCHAR(100) NOT NULL,
    started_at      TIMESTAMP    NOT NULL,
    finished_at     TIMESTAMP,
    status          VARCHAR(30)  NOT NULL DEFAULT 'running',
    records_loaded  INTEGER,
    checks_run      INTEGER,
    checks_error    INTEGER,
    checks_warning  INTEGER,
    notes           TEXT,
    CONSTRAINT pk_pipeline_runs PRIMARY KEY (run_id)
);

-- =============================================================================
-- stg.check_results
-- =============================================================================
CREATE TABLE IF NOT EXISTS stg.check_results (
    id              BIGSERIAL    NOT NULL,
    source_table    VARCHAR(100) NOT NULL,
    category        VARCHAR(50)  NOT NULL,
    object_key      VARCHAR(100) NOT NULL,
    check_id        VARCHAR(20)  NOT NULL,
    message         TEXT         NOT NULL,
    status          VARCHAR(10)  NOT NULL,
    run_id          INTEGER,
    zip_source      TEXT,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_check_results PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_check_results_status
    ON stg.check_results (status);
CREATE INDEX IF NOT EXISTS idx_check_results_category
    ON stg.check_results (category);
CREATE INDEX IF NOT EXISTS idx_check_results_check_id
    ON stg.check_results (check_id);
CREATE INDEX IF NOT EXISTS idx_check_results_run_id
    ON stg.check_results (run_id);
CREATE INDEX IF NOT EXISTS idx_check_results_created_at
    ON stg.check_results (created_at DESC);
