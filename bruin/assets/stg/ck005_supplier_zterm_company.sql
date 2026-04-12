/* @bruin
name: stg.ck005_supplier_zterm_company
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK005 — SAP_REF: Fornitori (ZBP_AddDatiSocieta): campo ZTERM1
  valorizzato e presente nella tabella di riferimento SAP TVZB (ZTERM).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_COMPANY#ZBP_AddDatiSocieta'         AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CK005'                                      AS check_id,
    CASE
        WHEN raw."ZTERM1" IS NULL OR raw."ZTERM1" = ''
            THEN 'ZTERM1 obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TVZB" ref
            WHERE ref."ZTERM" = raw."ZTERM1"
        )
            THEN 'Condizione pagamento [' || raw."ZTERM1" || '] non presente in SAP (TVZB.ZTERM)'
        ELSE 'Condizione pagamento [' || raw."ZTERM1" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."ZTERM1" IS NULL OR raw."ZTERM1" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TVZB" ref
            WHERE ref."ZTERM" = raw."ZTERM1"
        )                                               THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_zip_source"                            AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_COMPANY#ZBP_AddDatiSocieta" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK005'
)
;
