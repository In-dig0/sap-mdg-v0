/* @bruin
name: stg.ck030_customer_zterm
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK030 — SAP_REF: Clienti (S_CUST_COMPANY#ZBP-DatiSocieta): condizione
  di pagamento (ZTERM) obbligatoria e presente nella tabella SAP T052 (ZTERM).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_COMPANY#ZBP-DatiSocieta'              AS source_table,
    'BP'                                          AS category,
    raw."KUNNR(k/*)"                              AS object_key,
    'CK030'                                       AS check_id,
    CASE
        WHEN raw."ZTERM" IS NULL OR raw."ZTERM" = ''
            THEN 'ZTERM obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T052" ref
            WHERE ref."ZTERM" = raw."ZTERM"
        )
            THEN 'Condizione pagamento [' || raw."ZTERM" || '] non presente in SAP (T052.ZTERM)'
        ELSE 'Ok'
    END                                           AS message,
    CASE
        WHEN raw."ZTERM" IS NULL OR raw."ZTERM" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T052" ref
            WHERE ref."ZTERM" = raw."ZTERM"
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                           AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)            AS run_id,
    raw."_source"                                 AS zip_source,
    NOW()                                         AS created_at
FROM raw."S_CUST_COMPANY#ZBP-DatiSocieta" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK030'
)
;
