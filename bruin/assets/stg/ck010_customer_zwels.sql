/* @bruin
name: stg.ck010_customer_zwels
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK010 — SAP_REF: Clienti (ZBP-DatiSocieta): campo ZWELS_01
  valorizzato e presente nella tabella di riferimento SAP_Mod_Pagamento (Cod_Mod_Pag).
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
    'CK010'                                      AS check_id,
    CASE
        WHEN raw."ZWELS_01" IS NULL OR raw."ZWELS_01" = ''
            THEN 'ZWELS_01 obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_Mod_Pagamento" ref
            WHERE ref."Cod_Mod_Pag" = raw."ZWELS_01"
        )
            THEN 'Modalità pagamento [' || raw."ZWELS_01" || '] non presente in SAP (SAP_Mod_Pagamento)'
        ELSE 'Modalità pagamento [' || raw."ZWELS_01" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."ZWELS_01" IS NULL OR raw."ZWELS_01" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_Mod_Pagamento" ref
            WHERE ref."Cod_Mod_Pag" = raw."ZWELS_01"
        )                                                    THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_COMPANY#ZBP-DatiSocieta" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK010'
)
;
