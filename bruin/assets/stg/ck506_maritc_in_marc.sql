/* @bruin
name: stg.ck506_maritc_in_marc
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK506 — CROSS_SOURCE: Codici doganali (S_MARITC): la coppia
  MATNR(k/*) + PLANT(k/*) deve essere presente in S_MARC
  come coppia PRODUCT(k/*) + WERKS(k/*).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARITC'                                        AS source_table,
    'MAT'                                             AS category,
    raw."MATNR(k/*)" || '|' || raw."PLANT(k/*)"      AS object_key,
    'CK506'                                           AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARC" marc
            WHERE marc."PRODUCT(k/*)" = raw."MATNR(k/*)"
              AND marc."WERKS(k/*)"   = raw."PLANT(k/*)"
        )
            THEN 'Coppia [MATNR=' || raw."MATNR(k/*)" || ' / PLANT=' || raw."PLANT(k/*)" || '] non presente in S_MARC'
        ELSE 'Ok'
    END                                               AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARC" marc
            WHERE marc."PRODUCT(k/*)" = raw."MATNR(k/*)"
              AND marc."WERKS(k/*)"   = raw."PLANT(k/*)"
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                               AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)               AS run_id,
    raw."_source"                                     AS zip_source,
    NOW()                                             AS created_at
FROM raw."S_MARITC" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK506'
)
;
