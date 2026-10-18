/* @bruin
name: stg.ck518_add_interlocutore_cli_customer
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK518 — CROSS_SOURCE: Clienti, dati aggiuntivi e interlocutori (file ZIP il cui nome contiene "ZBP-AddInterlocutoreCli"):
  ogni KUNNR(k/*) delle tabelle S_CUST_GEN#AddDatiGenerali, S_CUST_CONT#ZBP-AddInterlocutore, S_UNL_POINT#ZBP-AddPuntoScarico
  deve essere presente in almeno una delle anagrafiche clienti
  raw.S_CUST_GEN#ZBP-DatiGenerali oppure raw.S_CUST_GEN#ZDM-DatiGenerali.
  Un esito per cliente e per tabella.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
WITH clienti_flusso AS (
    SELECT DISTINCT
        'S_CUST_GEN#AddDatiGenerali'             AS source_table,
        x."KUNNR(k/*)"                           AS kunnr,
        x."_source"                              AS zip_source
    FROM raw."S_CUST_GEN#AddDatiGenerali" x
    WHERE x."_source" ILIKE '%ZBP-AddInterlocutoreCli%'
      AND x."KUNNR(k/*)" IS NOT NULL AND x."KUNNR(k/*)" <> ''
    UNION ALL
    SELECT DISTINCT
        'S_CUST_CONT#ZBP-AddInterlocutore'       AS source_table,
        x."KUNNR(k/*)"                           AS kunnr,
        x."_source"                              AS zip_source
    FROM raw."S_CUST_CONT#ZBP-AddInterlocutore" x
    WHERE x."_source" ILIKE '%ZBP-AddInterlocutoreCli%'
      AND x."KUNNR(k/*)" IS NOT NULL AND x."KUNNR(k/*)" <> ''
    UNION ALL
    SELECT DISTINCT
        'S_UNL_POINT#ZBP-AddPuntoScarico'        AS source_table,
        x."KUNNR(k/*)"                           AS kunnr,
        x."_source"                              AS zip_source
    FROM raw."S_UNL_POINT#ZBP-AddPuntoScarico" x
    WHERE x."_source" ILIKE '%ZBP-AddInterlocutoreCli%'
      AND x."KUNNR(k/*)" IS NOT NULL AND x."KUNNR(k/*)" <> ''
)
SELECT
    f.source_table                               AS source_table,
    'BP'                                         AS category,
    f.kunnr                                      AS object_key,
    'CK518'                                      AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" zbp
            WHERE zbp."KUNNR(k/*)" = f.kunnr
        )
        AND NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZDM-DatiGenerali" zdm
            WHERE zdm."KUNNR(k/*)" = f.kunnr
        )
            THEN 'Cliente [' || f.kunnr || '] presente in ' || f.source_table
                 || ' ma non in S_CUST_GEN#ZBP-DatiGenerali né in S_CUST_GEN#ZDM-DatiGenerali'
        ELSE 'Ok'
    END                                          AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZBP-DatiGenerali" zbp
            WHERE zbp."KUNNR(k/*)" = f.kunnr
        )
        AND NOT EXISTS (
            SELECT 1 FROM raw."S_CUST_GEN#ZDM-DatiGenerali" zdm
            WHERE zdm."KUNNR(k/*)" = f.kunnr
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK518')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    f.zip_source                                 AS zip_source,
    NOW()                                        AS created_at
FROM clienti_flusso f
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK518'
)
;
