/* @bruin
name: stg.ck053_inforecord_kbetr_ext
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK053 — EXISTENCE: Inforecord acquisti (S_COND#INFORCOND):
  il campo KBETR_EXT deve essere sempre valorizzato (non nullo e non vuoto).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_COND#INFORCOND'                              AS source_table,
    'MAT'                                           AS category,
    raw."INFNR(k/*)"                                AS object_key,
    'CK053'                                         AS check_id,
    CASE
        WHEN raw."KBETR_EXT" IS NULL OR raw."KBETR_EXT" = ''
            THEN 'KBETR_EXT obbligatorio mancante per INFNR [' || raw."INFNR(k/*)" || ']'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN raw."KBETR_EXT" IS NULL OR raw."KBETR_EXT" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK053')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_COND#INFORCOND" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK053'
)
;
