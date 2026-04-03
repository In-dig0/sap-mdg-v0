/* @bruin
name: stg.chk03_partita_iva
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK03 — Verifica che ogni BP presente in S_SUPPL_GEN#ZBP_DatiGenerali
  abbia almeno un codice fiscale valorizzato in S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc
  (campo TAXNUM(*) non vuoto).
  Partita IVA mancante per soggetti UE/ExtraUE.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table,
    category,
    object_key,
    check_id,
    message,
    status,
    run_id,
    created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'              AS source_table,
    'BP'                                         AS category,
    gen."LIFNR(k/*)"                             AS object_key,
    'CHK03'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc" tax
            WHERE tax."LIFNR(k/*)" = gen."LIFNR(k/*)"
        )
            THEN 'Nessun codice fiscale presente in ZBP_CodiciFisc'
        WHEN NOT EXISTS (
            SELECT 1
            FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc" tax
            WHERE tax."LIFNR(k/*)" = gen."LIFNR(k/*)"
              AND tax."TAXNUM(*)"  IS NOT NULL
              AND tax."TAXNUM(*)"  <> ''
        )
            THEN 'Codici fiscali presenti ma tutti vuoti (TAXNUM* non valorizzato)'
        ELSE
            'Almeno un codice fiscale valorizzato presente'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc" tax
            WHERE tax."LIFNR(k/*)" = gen."LIFNR(k/*)"
        )
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1
            FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc" tax
            WHERE tax."LIFNR(k/*)" = gen."LIFNR(k/*)"
              AND tax."TAXNUM(*)"  IS NOT NULL
              AND tax."TAXNUM(*)"  <> ''
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS')          AS run_id,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" gen
;
