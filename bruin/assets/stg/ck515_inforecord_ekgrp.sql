/* @bruin
name: stg.ck515_inforecord_ekgrp
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK515 — CROSS_SOURCE: Inforecord acquisti:
  - Il materiale corrispondente (MATNR) in S_MARC deve avere il campo
    EKGRP valorizzato.
  - I record in cui MATNR non è presente in S_MARC sono esclusi
    (già coperti da CK512).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_EINA#INFORMATFOR'                            AS source_table,
    'MAT'                                           AS category,
    raw."INFNR(k/*)"                                AS object_key,
    'CK515'                                         AS check_id,
    CASE
        WHEN marc."EKGRP" IS NULL OR marc."EKGRP" = ''
            THEN 'EKGRP non valorizzato in S_MARC per il materiale [' || raw."MATNR" || ']'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN marc."EKGRP" IS NULL OR marc."EKGRP" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK515')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_EINA#INFORMATFOR" raw
JOIN raw."S_MARC" marc
    ON marc."PRODUCT(k/*)" = raw."MATNR"
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK515'
)
;
