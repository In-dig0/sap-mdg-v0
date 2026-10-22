/* @bruin
name: stg.ck217_zdm_ragione_sociale
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK217 — EXISTENCE: Destinatari merci (ZDM-DatiGenerali): la ragione sociale è obbligatoria, i campi NAMORG1,
  NAMORG2, NAME_FIRST e NAME_LAST non possono essere tutti vuoti (le stringhe di soli spazi
  valgono come vuote). Un esito per destinatario merci.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_GEN#ZDM-DatiGenerali'                AS source_table,
    'BP'                                         AS category,
    raw."KUNNR(k/*)"                                  AS object_key,
    'CK217'                                      AS check_id,
    CASE
        WHEN m.manca_nome
            THEN 'Ragione sociale mancante: NAMORG1, NAMORG2, NAME_FIRST e NAME_LAST tutti vuoti'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN m.manca_nome
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK217')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_CUST_GEN#ZDM-DatiGenerali" raw
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(raw."NAMORG1"), '') = ''
       AND COALESCE(TRIM(raw."NAMORG2"), '') = ''
       AND COALESCE(TRIM(raw."NAME_FIRST"), '') = ''
       AND COALESCE(TRIM(raw."NAME_LAST"), '') = '' AS manca_nome
) m
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK217'
)
;
