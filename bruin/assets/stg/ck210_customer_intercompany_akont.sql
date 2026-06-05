/* @bruin
name: stg.ck210_customer_intercompany_akont
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK210 — EXISTENCE: Clienti intercompany (BU_GROUP(*) = 'ZIC'):
  il campo AKONT(*) non può contenere un conto di riconciliazione
  riservato ai clienti terzi (0012210001, 0012210011, 0012210021).
  Tabelle: S_CUST_COMPANY#ZBP-DatiSocieta JOIN S_CUST_GEN#ZBP-DatiGenerali.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_COMPANY#ZBP-DatiSocieta'             AS source_table,
    'BP'                                         AS category,
    soc."KUNNR(k/*)"                             AS object_key,
    'CK210'                                      AS check_id,
    'Cliente intercompany (BU_GROUP=ZIC): conto riconciliazione ['
    || soc."AKONT(*)" || '] non ammesso per ZIC'
    || ' per KUNNR [' || soc."KUNNR(k/*)" || ']' AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK210') AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    soc."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_COMPANY#ZBP-DatiSocieta" soc
JOIN raw."S_CUST_GEN#ZBP-DatiGenerali" gen
  ON gen."KUNNR(k/*)" = soc."KUNNR(k/*)"
WHERE gen."BU_GROUP(*)" = 'ZIC'
  AND soc."AKONT(*)" IN ('0012210001', '0012210011', '0012210021')
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK210'
)
;
