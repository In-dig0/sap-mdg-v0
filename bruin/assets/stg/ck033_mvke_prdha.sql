/* @bruin
name: stg.ck033_mvke_prdha
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK033 — SAP_REF: Materiali (S_MVKE): campo PRDHA (Gerarchia prodotto)
  obbligatorio e presente nella tabella di riferimento SAP_EXPORT_PRDHA (PRDHA).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MVKE'                                     AS source_table,
    'MAT'                                        AS category,
    raw."PRODUCT(k/*)"                           AS object_key,
    'CK033'                                      AS check_id,
    CASE
        WHEN raw."PRODH" IS NULL OR raw."PRODH" = ''
            THEN 'PRODH obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_PRDHA" ref
            WHERE ref."PRDHA" = raw."PRODH"
        )
            THEN 'Gerarchia prodotto [' || raw."PRODH" || '] non presente in SAP (SAP_EXPORT_PRDHA.PRDHA)'
        ELSE 'Gerarchia prodotto [' || raw."PRODH" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."PRODH" IS NULL OR raw."PRODH" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_PRDHA" ref
            WHERE ref."PRDHA" = raw."PRODH"
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MVKE" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK033'
)
;
