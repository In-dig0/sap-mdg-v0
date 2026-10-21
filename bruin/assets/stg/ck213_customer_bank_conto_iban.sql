/* @bruin
name: stg.ck213_customer_bank_conto_iban
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK213 — EXISTENCE: Clienti (AppoggioBanca): se la chiave banca BANKL(k) (ABI+CAB
  per le banche italiane) è valorizzata, devono essere valorizzati anche il
  numero di conto BANKN(k) e l'IBAN IBAN(k). Un esito per coordinata bancaria.
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
    'CK213'                                      AS check_id,
    CASE
        WHEN m.manca_bankn AND m.manca_iban
            THEN 'Banca [' || raw."BANKS(k)" || '/' || TRIM(raw."BANKL(k)") || '] senza numero di conto (BANKN) e senza IBAN'
        WHEN m.manca_bankn
            THEN 'Banca [' || raw."BANKS(k)" || '/' || TRIM(raw."BANKL(k)") || '] senza numero di conto (BANKN)'
        WHEN m.manca_iban
            THEN 'Banca [' || raw."BANKS(k)" || '/' || TRIM(raw."BANKL(k)") || '] senza IBAN'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN m.manca_bankn OR m.manca_iban
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK213')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_BANK_DATA#ZBP-AppoggioBanca" raw
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(raw."BANKN(k)"), '') = '' AS manca_bankn,
           COALESCE(TRIM(raw."IBAN(k)"),  '') = '' AS manca_iban
) m
WHERE COALESCE(TRIM(raw."BANKL(k)"), '') <> ''
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK213'
)
;
