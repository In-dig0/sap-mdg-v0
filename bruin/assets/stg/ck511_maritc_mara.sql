/* @bruin
name: stg.ck511_maritc_mara
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK511 — CROSS_SOURCE: Codici doganali: ogni MATNR(k/*) in S_MARITC
  deve essere presente in S_MARA come PRODUCT(k/*).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARITC'                                      AS source_table,
    'MAT'                                           AS category,
    maritc."MATNR(k/*)"                             AS object_key,
    'CK511'                                         AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARA" mara
            WHERE mara."PRODUCT(k/*)" = maritc."MATNR(k/*)"
        )
            THEN 'MATNR [' || maritc."MATNR(k/*)" || '] '
                 || 'presente in S_MARITC ma assente in S_MARA (PRODUCT(k/*))'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARA" mara
            WHERE mara."PRODUCT(k/*)" = maritc."MATNR(k/*)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK511')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    maritc."_source"                                AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_MARITC" maritc
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK511'
)
;
