/* @bruin
name: stg.ck209_supplier_intercompany_vbund
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK209 — EXISTENCE: Fornitori intercompany (BU_GROUP = 'ZIC'):
  il campo VBUND deve essere obbligatoriamente valorizzato.
  Tabella: S_SUPPL_GEN#ZBP_DatiGenerali.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'              AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CK209'                                      AS check_id,
    'Fornitore intercompany (BU_GROUP=ZIC): VBUND obbligatorio mancante'
    || ' per LIFNR [' || raw."LIFNR(k/*)" || ']' AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK209') AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE raw."BU_GROUP" = 'ZIC'
  AND (raw."VBUND" IS NULL OR raw."VBUND" = '')
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK209'
)
;
