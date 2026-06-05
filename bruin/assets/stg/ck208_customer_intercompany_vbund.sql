/* @bruin
name: stg.ck208_customer_intercompany_vbund
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK208 — EXISTENCE: Clienti intercompany (BU_GROUP(*) = 'ZIC'):
  il campo VBUND deve essere obbligatoriamente valorizzato.
  Tabella: S_CUST_GEN#ZBP-DatiGenerali.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_GEN#ZBP-DatiGenerali'               AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                             AS object_key,
    'CK208'                                      AS check_id,
    'Cliente intercompany (BU_GROUP=ZIC): VBUND obbligatorio mancante'
    || ' per KUNNR [' || raw."KUNNR(k/*)" || ']' AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK208') AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_GEN#ZBP-DatiGenerali" raw
WHERE raw."BU_GROUP(*)" = 'ZIC'
  AND (raw."VBUND" IS NULL OR raw."VBUND" = '')
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK208'
)
;
