/* @bruin
name: stg.ck517_inforecord_matnr_duplicati
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK517 — DUPLICATE: Inforecord acquisti (S_EINA#INFORMATFOR):
  non devono esistere record multipli con lo stesso MATNR.
  Ogni INFNR coinvolto in un gruppo duplicato viene segnalato
  come record distinto.
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
    'CK517'                                         AS check_id,
    'MATNR [' || raw."MATNR" || '] duplicato in S_EINA#INFORMATFOR'
        || ' (' || COUNT(*) OVER (PARTITION BY raw."MATNR") || ' occorrenze)'
                                                    AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK517')
                                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_EINA#INFORMATFOR" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK517'
)
  AND (
    SELECT COUNT(*)
    FROM raw."S_EINA#INFORMATFOR" inner_raw
    WHERE inner_raw."MATNR" = raw."MATNR"
  ) > 1
;
