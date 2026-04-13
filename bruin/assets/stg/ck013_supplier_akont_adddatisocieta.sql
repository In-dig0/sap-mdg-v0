/* @bruin
name: stg.ck013_supplier_akont_adddatisocieta
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK013 — SAP_REF: Fornitori (ZBP_AddDatiSocieta): campo AKONT(*)
  valorizzato e presente nella tabella SAP_Conto_Riconciliazione_Fornitori (Cod_Conto).
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
    'CK013'                                      AS check_id,
    CASE
        WHEN raw."AKONT(*)" IS NULL OR raw."AKONT(*)" = ''
            THEN 'AKONT(*) obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_Conto_Riconciliazione_Fornitori" ref
            WHERE ref."Cod_Conto" = raw."AKONT(*)"
        )
            THEN 'Conto riconciliazione [' || raw."AKONT(*)" || '] non presente in SAP (SAP_Conto_Riconciliazione_Fornitori)'
        ELSE 'Conto riconciliazione [' || raw."AKONT(*)" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."AKONT(*)" IS NULL OR raw."AKONT(*)" = ''  THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_Conto_Riconciliazione_Fornitori" ref
            WHERE ref."Cod_Conto" = raw."AKONT(*)"
        )                                                    THEN 'Error'
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
    FROM stg.check_catalog WHERE check_id = 'CK013'
)
;
