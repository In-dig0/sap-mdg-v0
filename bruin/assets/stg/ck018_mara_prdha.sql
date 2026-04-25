/* @bruin
name: stg.ck018_mara_prdha
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK018 — SAP_REF: Materiali (S_MARA): campo PRDHA (Gerarchia prodotto)
  se valorizzato, deve essere presente in SAP_EXPORT_PRDHA (PRDHA).
  Se il campo è vuoto o NULL, il record viene ignorato.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARA'                                     AS source_table,
    'MAT'                                        AS category,
    raw."PRODUCT(k/*)"                           AS object_key,
    'CK018'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_PRDHA" ref
            WHERE ref."PRDHA" = raw."PRDHA"
        )
            THEN 'Gerarchia prodotto [' || raw."PRDHA" || '] non presente in SAP (SAP_EXPORT_PRDHA.PRDHA)'
        ELSE 'Gerarchia prodotto [' || raw."PRDHA" || '] valida'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_PRDHA" ref
            WHERE ref."PRDHA" = raw."PRDHA"
        )                                        THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MARA" raw
WHERE
    raw."PRDHA" IS NOT NULL AND raw."PRDHA" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK018'
    )
;
