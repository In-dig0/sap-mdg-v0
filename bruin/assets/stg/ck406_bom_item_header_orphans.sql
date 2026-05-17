/* @bruin
name: stg.ck406_bom_item_header_orphans
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK406 — CROSS_TABLE: Distinte base: ogni MATNR(k/*) in S_BOM_ITEM
  deve essere presente in S_BOM_HEADER.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_BOM_ITEM'                                    AS source_table,
    'MAT'                                           AS category,
    item."MATNR(k/*)"                               AS object_key,
    'CK406'                                         AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_BOM_HEADER" hdr
            WHERE hdr."MATNR(k/*)" = item."MATNR(k/*)"
              AND hdr."WERKS(k)"   = item."WERKS(k)"
        )
            THEN 'MATNR [' || item."MATNR(k/*)" || '] / WERKS [' || item."WERKS(k)" || '] '
                 || 'presente in S_BOM_ITEM ma assente in S_BOM_HEADER'
        ELSE 'Ok'
    END                                             AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_BOM_HEADER" hdr
            WHERE hdr."MATNR(k/*)" = item."MATNR(k/*)"
              AND hdr."WERKS(k)"   = item."WERKS(k)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK406')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    item."_source"                                  AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_BOM_ITEM" item
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK406'
)
;
