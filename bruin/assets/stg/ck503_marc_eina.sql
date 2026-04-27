/* @bruin
name: stg.ck503_marc_eina
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK503 — CROSS_SOURCE: Materiali: inforecord acquisti obbligatorio
  per materiali di acquisto esterno puro (BESKZ = 'F' e SOBSL vuoto).
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
    'CK503'                                         AS check_id,
    CASE WHEN NOT EXISTS (
             SELECT 1 FROM raw."S_EINA#INFORMATFOR" eina
             WHERE eina."MATNR" = marc."PRODUCT(k/*)"
         )
         THEN 'Materiale [' || marc."PRODUCT(k/*)" || '] '
              || 'plant [' || marc."WERKS(k/*)" || '] '
              || 'ha BESKZ=[F] SOBSL=[ ] (acquisto esterno puro) '
              || 'ma non ha un inforecord acquisti in S_EINA#INFORMATFOR'
         ELSE 'Ok'
    END                                             AS message,
    CASE WHEN NOT EXISTS (
             SELECT 1 FROM raw."S_EINA#INFORMATFOR" eina
             WHERE eina."MATNR" = marc."PRODUCT(k/*)"
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
WHERE marc."BESKZ" = 'F'
  AND COALESCE(marc."SOBSL", '') = ''
  AND (
      SELECT COALESCE(is_active, FALSE)
      FROM stg.check_catalog
      WHERE check_id = 'CK503'
  )
;
