/* @bruin
name: stg.ck216_supplier_ragione_sociale
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK216 — EXISTENCE: Fornitori e vettori (ZBP_DatiGenerali): la ragione sociale è obbligatoria, i campi NAME_FIRST,
  NAME_LAST, NAME_FIRST_P e NAME_LAST_P non possono essere tutti vuoti (le stringhe di soli spazi
  valgono come vuote). Un esito per fornitore.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'               AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                                  AS object_key,
    'CK216'                                      AS check_id,
    CASE
        WHEN m.manca_nome
            THEN 'Ragione sociale mancante: NAME_FIRST, NAME_LAST, NAME_FIRST_P e NAME_LAST_P tutti vuoti'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN m.manca_nome
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK216')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(raw."NAME_FIRST"), '') = ''
       AND COALESCE(TRIM(raw."NAME_LAST"), '') = ''
       AND COALESCE(TRIM(raw."NAME_FIRST_P"), '') = ''
       AND COALESCE(TRIM(raw."NAME_LAST_P"), '') = '' AS manca_nome
) m
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK216'
)
;
