/* @bruin
name: stg.ck206_inforecord_kbetr
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK206 — EXISTENCE: Inforecord acquisti (S_SCALES#INFORSCALES):
  il campo KBETR deve essere sempre valorizzato (non nullo, non vuoto
  e diverso da zero).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SCALES#INFORSCALES'                          AS source_table,
    'MAT'                                           AS category,
    raw."INFNR(k/*)"                                AS object_key,
    'CK206'                                         AS check_id,
    CASE
        WHEN raw."KBETR" IS NULL OR raw."KBETR" = ''
            THEN 'KBETR obbligatorio mancante per INFNR [' || raw."INFNR(k/*)" || ']'
        WHEN REPLACE(raw."KBETR", ',', '.')::numeric = 0
            THEN 'KBETR uguale a zero per INFNR [' || raw."INFNR(k/*)" || ']'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN raw."KBETR" IS NULL OR raw."KBETR" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK206')
        WHEN REPLACE(raw."KBETR", ',', '.')::numeric = 0
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK206')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_SCALES#INFORSCALES" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK206'
)
;
