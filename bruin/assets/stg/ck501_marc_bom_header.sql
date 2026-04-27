/* @bruin
name: stg.ck501_marc_bom_header
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK501 — CROSS_SOURCE: Materiali: distinta base obbligatoria per
  produzione interna o mista (BESKZ in 'E', 'X').
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARC'                                        AS source_table,
    'MAT'                                           AS category,
    marc."PRODUCT(k/*)"                             AS object_key,
    'CK501'                                         AS check_id,
    CASE WHEN NOT EXISTS (
             SELECT 1 FROM raw."S_BOM_HEADER" bom
             WHERE bom."MATNR(k/*)" = marc."PRODUCT(k/*)"
               AND bom."WERKS(k)"   = marc."WERKS(k/*)"
         )
         THEN 'Materiale [' || marc."PRODUCT(k/*)" || '] '
              || 'plant [' || marc."WERKS(k/*)" || '] '
              || 'ha BESKZ=[' || marc."BESKZ" || '] '
              || '(produzione ' || CASE marc."BESKZ"
                  WHEN 'E' THEN 'interna'
                  WHEN 'X' THEN 'mista interna/esterna'
                 END || ') '
              || 'ma non ha una distinta base in S_BOM_HEADER'
         ELSE 'Ok'
    END                                             AS message,
    CASE WHEN NOT EXISTS (
             SELECT 1 FROM raw."S_BOM_HEADER" bom
             WHERE bom."MATNR(k/*)" = marc."PRODUCT(k/*)"
               AND bom."WERKS(k)"   = marc."WERKS(k/*)"
         )
         THEN 'Error'
         ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    marc."_source"                                  AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_MARC" marc
WHERE marc."BESKZ" IN ('E', 'X')
  AND (
      SELECT COALESCE(is_active, FALSE)
      FROM stg.check_catalog
      WHERE check_id = 'CK501'
  )
;
