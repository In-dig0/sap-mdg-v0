/* @bruin
name: stg.ck007_customer_zterm
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK007 — SAP_REF: Clienti (ZBP-DatiSocieta): campo ZTERM
  valorizzato e presente nella tabella di riferimento SAP TVZB (ZTERM).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_COMPANY#ZBP-DatiSocieta'             AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                             AS object_key,
    'CK007'                                      AS check_id,
    CASE
        WHEN raw."ZTERM" IS NULL OR raw."ZTERM" = ''
            THEN 'ZTERM obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TVZB" ref
            WHERE ref."ZTERM" = raw."ZTERM"
        )
            THEN 'Condizione pagamento [' || raw."ZTERM" || '] non presente in SAP (TVZB.ZTERM)'
        ELSE 'Condizione pagamento [' || raw."ZTERM" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."ZTERM" IS NULL OR raw."ZTERM" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TVZB" ref
            WHERE ref."ZTERM" = raw."ZTERM"
        )                                               THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_COMPANY#ZBP-DatiSocieta" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK007'
)
;
