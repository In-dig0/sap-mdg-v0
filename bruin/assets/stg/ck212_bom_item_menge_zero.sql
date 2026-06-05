/* @bruin
name: stg.ck212_bom_item_menge_zero
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK212 — EXISTENCE: Distinte base (S_BOM_ITEM):
  nessun componente può avere quantità pari a zero.
  Il campo MENGE(*) (tipo text, formato '000000,000') non deve essere '000000,000'.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_BOM_ITEM'                                 AS source_table,
    'MAT'                                        AS category,
    raw."MATNR(k/*)" || '/' || raw."WERKS(k)"
    || '/' || raw."POSNR(*)"                     AS object_key,
    'CK212'                                      AS check_id,
    'Componente con quantità zero: MATNR [' || raw."MATNR(k/*)" || ']'
    || ' WERKS [' || raw."WERKS(k)" || ']'
    || ' POSNR [' || raw."POSNR(*)" || ']'
    || ' IDNRK [' || COALESCE(raw."IDNRK", '') || ']'
                                                 AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK212') AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_BOM_ITEM" raw
WHERE raw."MENGE(*)" = '000000,000'
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK212'
)
;
