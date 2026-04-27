/* @bruin
name: stg.ck032_customer_zwels
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK032 — SAP_REF: Clienti (S_CUST_COMPANY#ZBP-DatiSocieta): modalità
  di pagamento (ZWELS_01) obbligatoria e presente nella tabella SAP T042Z (ZLSCH).
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
    'CK032'                                       AS check_id,
    CASE
        WHEN raw."ZWELS_01" IS NULL OR raw."ZWELS_01" = ''
            THEN 'ZWELS_01 obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T042Z" ref
            WHERE ref."ZLSCH" = raw."ZWELS_01"
        )
            THEN 'Modalità pagamento [' || raw."ZWELS_01" || '] non presente in SAP (T042Z.ZLSCH)'
        ELSE 'Ok'
    END                                           AS message,
    CASE
        WHEN raw."ZWELS_01" IS NULL OR raw."ZWELS_01" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T042Z" ref
            WHERE ref."ZLSCH" = raw."ZWELS_01"
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
    FROM stg.check_catalog WHERE check_id = 'CK032'
)
;
