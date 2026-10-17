/* @bruin
name: stg.ck047_supplier_bankl_banche_it
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK047 — SAP_REF: Fornitori (ZBP_AppoggioBanca): campo BANKL(k)
  deve essere presente nella tabella di riferimento SAP BNKA ref."SAP_EXPORT_BNKA_IT"
  (campo BANKL = ABI+CAB, banca non cancellata: LOEVM <> 'X'),
  ma solo per i record con BANKS(k/*)='IT'.
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
        WHEN raw."BANKL(k)" IS NULL OR raw."BANKL(k)" = ''
            THEN 'BANKL obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" ref
            WHERE ref."BANKS" = 'IT'
              AND ref."BANKL" = raw."BANKL(k)"
        )
            THEN 'Chiave banca IT [' || raw."BANKL(k)" || '] non presente in SAP (BNKA.BANKL)'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" ref
            WHERE ref."BANKS" = 'IT'
              AND ref."BANKL" = raw."BANKL(k)"
              AND COALESCE(ref."LOEVM", '') <> 'X'
        )
            THEN 'Chiave banca IT [' || raw."BANKL(k)" || '] cancellata in SAP (BNKA.LOEVM = X)'
        ELSE 'Chiave banca IT [' || raw."BANKL(k)" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."BANKL(k)" IS NULL OR raw."BANKL(k)" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK047')
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_BNKA_IT" ref
            WHERE ref."BANKS" = 'IT'
              AND ref."BANKL" = raw."BANKL(k)"
              AND COALESCE(ref."LOEVM", '') <> 'X'
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK047')
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
