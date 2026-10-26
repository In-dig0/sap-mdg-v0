/* @bruin
name: stg.ck221_stg_street_lunghezza
type: pg.sql
depends:
  - stg.clean_check_results
  - stg.add_str_suppl3
description: >
  CK221 — EXISTENCE: Dati generali STG di clienti, destinatari merci e fornitori:
  STREET non può superare i 60 caratteri e STR_SUPPL3 i 40 (lunghezze dei campi SAP).
  Gli indirizzi più lunghi vanno spezzati a mano tra STREET e STR_SUPPL3 dalla pagina
  Modifica Tabelle. Un esito per record STG.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
WITH indirizzi AS (
    SELECT 'S_CUST_GEN#ZBP_DatiGenerali_STG' AS source_table, "KUNNR(k/*)" AS chiave,
           COALESCE("STREET", '') AS street, COALESCE("STR_SUPPL3", '') AS str_suppl3, "_source" AS zip_source
    FROM stg."S_CUST_GEN#ZBP_DatiGenerali_STG"
    UNION ALL
    SELECT 'S_CUST_GEN#ZDM_DatiGenerali_STG', "KUNNR(k/*)",
           COALESCE("STREET", ''), COALESCE("STR_SUPPL3", ''), "_source"
    FROM stg."S_CUST_GEN#ZDM_DatiGenerali_STG"
    UNION ALL
    SELECT 'S_SUPPL_GEN#ZBP_DatiGenerali_STG', "LIFNR(k/*)",
           COALESCE("STREET", ''), COALESCE("STR_SUPPL3", ''), "_source"
    FROM stg."S_SUPPL_GEN#ZBP_DatiGenerali_STG"
)
SELECT
    i.source_table                               AS source_table,
    'BP'                                         AS category,
    i.chiave                                     AS object_key,
    'CK221'                                      AS check_id,
    CASE
        WHEN LENGTH(i.street) > 60 AND LENGTH(i.str_suppl3) > 40
            THEN 'STREET di ' || LENGTH(i.street) || ' caratteri (max 60) e STR_SUPPL3 di '
                 || LENGTH(i.str_suppl3) || ' caratteri (max 40)'
        WHEN LENGTH(i.street) > 60
            THEN 'STREET di ' || LENGTH(i.street) || ' caratteri (max 60): spezzare l''indirizzo tra STREET e STR_SUPPL3'
        WHEN LENGTH(i.str_suppl3) > 40
            THEN 'STR_SUPPL3 di ' || LENGTH(i.str_suppl3) || ' caratteri (max 40)'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN LENGTH(i.street) > 60 OR LENGTH(i.str_suppl3) > 40
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK221')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    i.zip_source                                 AS zip_source,
    NOW()                                        AS created_at
FROM indirizzi i
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK221'
)
;
