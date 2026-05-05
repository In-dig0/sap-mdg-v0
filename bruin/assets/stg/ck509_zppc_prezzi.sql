/* @bruin
name: stg.ck509_zppc_prezzi
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK509 — CROSS_SOURCE: Pricing vendite (BO-ZPPC-PrezziPianiConsegna).
  Tre verifiche:
  1. Material (obbligatorio) deve essere presente in raw.S_MVKE (PRODUCT(k/*))
  2. Customer (se valorizzato) deve essere presente in raw.S_CUST_GEN#ZBP-DatiGenerali (KUNNR(k/*))
  3. ShipToParty (se valorizzato) deve essere presente in raw.S_CUST_GEN#ZDM-DatiGenerali (KUNNR(k/*))
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
WITH results AS (

    -- ── 1. Material (obbligatorio) → S_MVKE ──────────────────────────────────
    SELECT
        raw."Material"                               AS object_key,
        CASE
            WHEN raw."Material" IS NULL OR raw."Material" = ''
                THEN 'Material obbligatorio mancante'
            WHEN NOT EXISTS (
                SELECT 1 FROM raw."S_MVKE" mvke
                WHERE mvke."PRODUCT(k/*)" = raw."Material"
            )
                THEN 'Material [' || raw."Material" || '] non presente in S_MVKE (PRODUCT(k/*))'
            ELSE 'Material [' || raw."Material" || '] valido'
        END                                          AS message,
        CASE
            WHEN raw."Material" IS NULL OR raw."Material" = ''
                THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK509')
            WHEN NOT EXISTS (
                SELECT 1 FROM raw."S_MVKE" mvke
                WHERE mvke."PRODUCT(k/*)" = raw."Material"
            )
                THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK509')
            ELSE 'Ok'
        END                                          AS status,
        raw."_source"                                AS zip_source
    FROM raw."BO-ZPPC-PrezziPianiConsegna" raw

    UNION ALL

    -- ── 2. Customer (opzionale) → S_CUST_GEN#ZBP-DatiGenerali ───────────────
    SELECT
        raw."Customer"                               AS object_key,
        CASE
            WHEN NOT EXISTS (
                SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" zbp
                WHERE zbp."KUNNR(k/*)" = raw."Customer"
            )
                THEN 'Customer [' || raw."Customer" || '] non presente in S_CUST_GEN#ZBP-DatiGenerali (KUNNR(k/*))'
            ELSE 'Customer [' || raw."Customer" || '] valido'
        END                                          AS message,
        CASE
            WHEN NOT EXISTS (
                SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" zbp
                WHERE zbp."KUNNR(k/*)" = raw."Customer"
            )
                THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK509')
            ELSE 'Ok'
        END                                          AS status,
        raw."_source"                                AS zip_source
    FROM raw."BO-ZPPC-PrezziPianiConsegna" raw
    WHERE raw."Customer" IS NOT NULL AND raw."Customer" <> ''

    UNION ALL

    -- ── 3. ShipToParty (opzionale) → S_CUST_GEN#ZDM-DatiGenerali ────────────
    SELECT
        raw."ShipToParty"                            AS object_key,
        CASE
            WHEN NOT EXISTS (
                SELECT 1 FROM raw."S_CUST_GEN#ZDM-DatiGenerali" zdm
                WHERE zdm."KUNNR(k/*)" = raw."ShipToParty"
            )
                THEN 'ShipToParty [' || raw."ShipToParty" || '] non presente in S_CUST_GEN#ZDM-DatiGenerali (KUNNR(k/*))'
            ELSE 'ShipToParty [' || raw."ShipToParty" || '] valido'
        END                                          AS message,
        CASE
            WHEN NOT EXISTS (
                SELECT 1 FROM raw."S_CUST_GEN#ZDM-DatiGenerali" zdm
                WHERE zdm."KUNNR(k/*)" = raw."ShipToParty"
            )
                THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK509')
            ELSE 'Ok'
        END                                          AS status,
        raw."_source"                                AS zip_source
    FROM raw."BO-ZPPC-PrezziPianiConsegna" raw
    WHERE raw."ShipToParty" IS NOT NULL AND raw."ShipToParty" <> ''

)
SELECT
    'BO-ZPPC-PrezziPianiConsegna'                AS source_table,
    'MAT'                                        AS category,
    r.object_key,
    'CK509'                                      AS check_id,
    r.message,
    r.status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    r.zip_source,
    NOW()                                        AS created_at
FROM results r
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK509'
)
;
