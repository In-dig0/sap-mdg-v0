/* @bruin
name: stg.ck056_customer_bankl_banche_ue
type: pg.sql
depends:
  - stg.clean_check_results
  - setup.fn_banca_estera
description: >
  CK056 — SAP_REF: Clienti (ZBP-AppoggioBanca): per le coordinate bancarie estere
  (BANKS diverso da IT) la chiave banca SAP deve essere determinabile e non
  cancellata in ref."SAP_EXPORT_BNKA_UE", con la regola di stg.fn_banca_estera:
    1. BANKL valorizzato → confronto esatto (zeri iniziali significativi);
    2. BANKL vuoto → codice banca dall'IBAN (inizio del BBAN);
    3. altrimenti SWIFT indicato dall'ERP nel campo BKREF (deve individuare una
       sola chiave banca).
  La chiave determinata viene scritta in prd dal merge delle coordinate
  bancarie (e BKREF svuotato). Le coordinate con BANKS = IT sono verificate
  da CK048.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_BANK_DATA#ZBP-AppoggioBanca'         AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                                  AS object_key,
    'CK056'                                      AS check_id,
    CASE r.esito
        WHEN 'OK' THEN
            'Banca estera [' || raw."BANKS(k)" || '/' || r.bankl || '] valida'
            || CASE r.metodo WHEN 'IBAN'  THEN ' (BANKL ricavato dall''IBAN)'
                             WHEN 'SWIFT' THEN ' (BANKL ricavato dallo SWIFT ' || UPPER(TRIM(raw."BKREF")) || ')'
                             ELSE '' END
        WHEN 'CANCELLATA' THEN
            'Banca [' || raw."BANKS(k)" || '/' || TRIM(raw."BANKL(k)") || '] cancellata in SAP (BNKA.LOEVM = X)'
        WHEN 'SWIFT_AMBIGUO' THEN
            'Banca estera [' || raw."BANKS(k)" || '] non determinabile: ' || r.dettaglio
            || ' (indicare BANKL o un IBAN riconoscibile)'
        WHEN 'DATI_MANCANTI' THEN
            'Banca estera [' || COALESCE(raw."BANKS(k)", '') || ']: BANKL, IBAN e SWIFT (BKREF) mancanti'
        ELSE
            'Banca estera [' || COALESCE(raw."BANKS(k)", '') || '] non presente in SAP (BNKA UE): '
            || COALESCE(r.dettaglio, '')
    END                                          AS message,
    CASE WHEN r.esito = 'OK' THEN 'Ok'
         ELSE (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK056')
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_BANK_DATA#ZBP-AppoggioBanca" raw
CROSS JOIN LATERAL stg.fn_banca_estera(
    raw."BANKS(k)", raw."BANKL(k)", raw."IBAN(k)", raw."BKREF"
) r
WHERE COALESCE(raw."BANKS(k)", '') <> 'IT'
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK056'
)
;
