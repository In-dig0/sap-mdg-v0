-- =============================================================================
-- MDG - Migration Data Governance
-- 01_init_schemas.sql
-- Crea gli schemi e le tabelle di infrastruttura del database MDG.
-- Eseguito automaticamente da PostgreSQL al primo avvio del container.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schemi
-- ---------------------------------------------------------------------------

-- raw: dati grezzi dall'ERP (un CSV = una tabella)
CREATE SCHEMA IF NOT EXISTS raw;

-- ref: tabelle di controllo SAP (codici paese, banche, regioni, ecc.)
--      Dati stabili di riferimento, separati dai dati ERP.
CREATE SCHEMA IF NOT EXISTS ref;

-- stg: staging area — risultati dei check, catalogo controlli, trasformazioni
CREATE SCHEMA IF NOT EXISTS stg;

-- ---------------------------------------------------------------------------
-- stg.check_catalog — catalogo dei controlli disponibili
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.check_catalog (
    check_id        VARCHAR(20)  NOT NULL,          -- es. CHK01
    check_desc      TEXT         NOT NULL,          -- descrizione leggibile
    category        VARCHAR(50)  NOT NULL,          -- es. BP, MATERIAL, VENDOR
    target_table    VARCHAR(100) NOT NULL,          -- tabella raw da controllare
    target_field    VARCHAR(100) NOT NULL,          -- campo oggetto del check
    ref_table       VARCHAR(100),                   -- tabella ref usata per il check
    severity        VARCHAR(10)  NOT NULL DEFAULT 'Error',  -- Error | Warning | Info
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_check_catalog PRIMARY KEY (check_id)
);

COMMENT ON TABLE stg.check_catalog IS
    'Catalogo dei controlli di qualità dati configurabili per la migrazione MDG.';

-- ---------------------------------------------------------------------------
-- stg.check_results — risultati di ogni esecuzione dei controlli
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.check_results (
    id              BIGSERIAL    NOT NULL,
    source_table    VARCHAR(100) NOT NULL,          -- es. ZBP_Vettori_DatiGenerali
    category        VARCHAR(50)  NOT NULL,          -- es. BP
    object_key      VARCHAR(100) NOT NULL,          -- chiave business (es. cod. fornitore)
    check_id        VARCHAR(20)  NOT NULL,          -- es. CHK01
    message         TEXT         NOT NULL,          -- messaggio descrittivo dell'esito
    status          VARCHAR(10)  NOT NULL,          -- Error | Warning | OK
    run_id          VARCHAR(50),                    -- identificativo del run Bruin
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_check_results  PRIMARY KEY (id),
    CONSTRAINT fk_check_catalog  FOREIGN KEY (check_id)
        REFERENCES stg.check_catalog (check_id)
);

CREATE INDEX IF NOT EXISTS idx_check_results_status
    ON stg.check_results (status);
CREATE INDEX IF NOT EXISTS idx_check_results_category
    ON stg.check_results (category);
CREATE INDEX IF NOT EXISTS idx_check_results_check_id
    ON stg.check_results (check_id);
CREATE INDEX IF NOT EXISTS idx_check_results_created_at
    ON stg.check_results (created_at DESC);

COMMENT ON TABLE stg.check_results IS
    'Risultati di ogni run dei controlli di qualità dati MDG.';
COMMENT ON COLUMN stg.check_results.object_key IS
    'Chiave business dell''oggetto in errore (es. codice BP, codice materiale).';
COMMENT ON COLUMN stg.check_results.run_id IS
    'Identificativo del run Bruin che ha generato il risultato.';

-- ---------------------------------------------------------------------------
-- stg.pipeline_runs — log dei run di pipeline Bruin
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.pipeline_runs (
    run_id          VARCHAR(50)  NOT NULL,
    pipeline_name   VARCHAR(100) NOT NULL,
    started_at      TIMESTAMP    NOT NULL,
    finished_at     TIMESTAMP,
    status          VARCHAR(20)  NOT NULL DEFAULT 'running',  -- running|success|failed
    records_loaded  INTEGER,
    checks_run      INTEGER,
    checks_error    INTEGER,
    checks_warning  INTEGER,
    notes           TEXT,
    CONSTRAINT pk_pipeline_runs PRIMARY KEY (run_id)
);

COMMENT ON TABLE stg.pipeline_runs IS
    'Log dei run della pipeline Bruin MDG.';

-- ---------------------------------------------------------------------------
-- Dati di esempio nel catalogo check (primi check previsti)
-- ---------------------------------------------------------------------------
INSERT INTO stg.check_catalog
    (check_id, check_desc, category, target_table, target_field, ref_table, severity)
VALUES
    ('CHK01', 'Codice paese non presente nella tabella SAP_COUNTRY',
     'BP', 'zbp_vettori_datigenerali', 'country', 'ref.sap_country', 'Error'),
    ('CHK02', 'Codice regione non coerente con il paese',
     'BP', 'zbp_vettori_datigenerali', 'region', 'ref.sap_region', 'Error'),
    ('CHK03', 'Partita IVA mancante per soggetti UE',
     'BP', 'zbp_vettori_datigenerali', 'taxnumxl', NULL, 'Error'),
    ('CHK04', 'Codice banca non presente nella tabella SAP_BANKL',
     'BP', 'zbp_vettori_datibancari', 'bankl', 'ref.sap_bankl', 'Warning')
ON CONFLICT (check_id) DO NOTHING;
