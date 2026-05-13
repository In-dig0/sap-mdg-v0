/* @bruin
name: stg.ck047_supplier_bankl_banche_it
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK047 — SAP_REF: Fornitori (ZBP_AppoggioBanca): campo BANKL(k)
  deve essere presente nella tabella di riferimento ref."SAP_Banche"
  (campo "Numero ABI/CAB"), ma solo per i record con BANKS(k/*)='IT'.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPP_BANK#ZBP_AppoggioBanca'              AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CK047'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_Banche" ref
            WHERE ref."Numero ABI/CAB" = raw."BANKL(k)"
        )
            THEN 'Chiave banca IT [' || raw."BANKL(k)" || '] non presente in SAP (SAP_Banche)'
        ELSE 'Chiave banca IT [' || raw."BANKL(k)" || '] valida'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_Banche" ref
            WHERE ref."Numero ABI/CAB" = raw."BANKL(k)"
        )                                        THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK047')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPP_BANK#ZBP_AppoggioBanca" raw
WHERE raw."BANKS(k/*)" = 'IT'
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK047'
)
;
