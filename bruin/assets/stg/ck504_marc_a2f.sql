/* @bruin
name: stg.ck504_marc_a2f
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK504 — CROSS_SOURCE: Materiali S_MARC devono essere presenti in A2F
  a parità di articolo (PRODUCT = CODART) e divisione (WERKS).
  WERKS = 'IT11' → ref.A2F_BO  |  WERKS = 'IT12' → ref.A2F_FA
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARC'                                         AS source_table,
    'MAT'                                            AS category,
    marc."PRODUCT(k/*)"                              AS object_key,
    'CK504'                                          AS check_id,
    CASE
        WHEN marc."WERKS(k/*)" = 'IT11' AND NOT EXISTS (
                 SELECT 1 FROM ref."A2F_BO" a2f
                 WHERE a2f."CODART" = marc."PRODUCT(k/*)"
             )
             THEN 'Materiale [' || marc."PRODUCT(k/*)" || '] plant [IT11] non presente in ref.A2F_BO (CODART)'
        WHEN marc."WERKS(k/*)" = 'IT12' AND NOT EXISTS (
                 SELECT 1 FROM ref."A2F_FA" a2f
                 WHERE a2f."CODART" = marc."PRODUCT(k/*)"
             )
             THEN 'Materiale [' || marc."PRODUCT(k/*)" || '] plant [IT12] non presente in ref.A2F_FA (CODART)'
        ELSE 'Ok'
    END                                              AS message,
    CASE
        WHEN marc."WERKS(k/*)" = 'IT11' AND NOT EXISTS (
                 SELECT 1 FROM ref."A2F_BO" a2f
                 WHERE a2f."CODART" = marc."PRODUCT(k/*)"
             )
             THEN 'Error'
        WHEN marc."WERKS(k/*)" = 'IT12' AND NOT EXISTS (
                 SELECT 1 FROM ref."A2F_FA" a2f
                 WHERE a2f."CODART" = marc."PRODUCT(k/*)"
             )
             THEN 'Error'
        ELSE 'Ok'
    END                                              AS status,
    (SELECT run_id FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)               AS run_id,
    marc."_source"                                   AS zip_source,
    NOW()                                            AS created_at
FROM raw."S_MARC" marc
WHERE marc."WERKS(k/*)" IN ('IT11', 'IT12')
  AND (
      SELECT COALESCE(is_active, FALSE)
      FROM stg.check_catalog
      WHERE check_id = 'CK504'
  )
;
