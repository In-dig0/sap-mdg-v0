/* @bruin
name: stg.ck514_mapl_bom_header
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK514 — CROSS_SOURCE: Cicli di lavoro: la coppia MATNR(k/*)+WERKS_MAT(k/*)
  in S_MAPL deve essere presente in S_BOM_HEADER come coppia MATNR(k/*)+WERKS(k).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MAPL'                                        AS source_table,
    'MAT'                                           AS category,
    mapl."MATNR(k/*)"                               AS object_key,
    'CK514'                                         AS check_id,
    CASE
        WHEN mapl."MATNR(k/*)" IS NULL OR mapl."MATNR(k/*)" = ''
            THEN 'MATNR obbligatorio mancante'
        WHEN mapl."WERKS_MAT(k/*)" IS NULL OR mapl."WERKS_MAT(k/*)" = ''
            THEN 'WERKS_MAT obbligatorio mancante (MATNR=' || mapl."MATNR(k/*)" || ')'
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_BOM_HEADER" bom
            WHERE bom."MATNR(k/*)" = mapl."MATNR(k/*)"
              AND bom."WERKS(k)"   = mapl."WERKS_MAT(k/*)"
        )
            THEN 'Coppia [' || mapl."MATNR(k/*)" || '/' || mapl."WERKS_MAT(k/*)" || '] '
                 || 'presente in S_MAPL ma assente in S_BOM_HEADER (MATNR+WERKS)'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN mapl."MATNR(k/*)"     IS NULL OR mapl."MATNR(k/*)"     = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK514')
        WHEN mapl."WERKS_MAT(k/*)" IS NULL OR mapl."WERKS_MAT(k/*)" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK514')
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_BOM_HEADER" bom
            WHERE bom."MATNR(k/*)" = mapl."MATNR(k/*)"
              AND bom."WERKS(k)"   = mapl."WERKS_MAT(k/*)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK514')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    mapl."_source"                                  AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_MAPL" mapl
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK514'
)
;
