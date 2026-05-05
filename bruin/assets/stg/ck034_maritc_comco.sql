/* @bruin
name: stg.ck034_maritc_comco
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK034 — SAP_REF: Materiali (S_MARITC): codice nomenclatura doganale
  COMCO(k/*) obbligatorio e presente nella tabella di riferimento
  SAP_CommodityCodesEU01 (codice).
  Il messaggio include il WERKS ricavato da S_MARC a parità di MATNR(k/*).
  In caso di più plant per lo stesso articolo, viene preso il primo in ordine alfabetico.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARITC'                                   AS source_table,
    'MAT'                                        AS category,
    raw."MATNR(k/*)" || '|' || raw."PLANT(k/*)" AS object_key,
    'CK034'                                      AS check_id,
    CASE
        WHEN raw."COMCO(k/*)" IS NULL OR raw."COMCO(k/*)" = ''
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] COMCO(k/*) obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_CommodityCodesEU01" ref
            WHERE ref."codice" = raw."COMCO(k/*)"
        )
            THEN '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Codice doganale [' || raw."COMCO(k/*)" || '] non presente in SAP (SAP_CommodityCodesEU01.codice)'
        ELSE '[' || COALESCE(marc."WERKS(k/*)", '?') || '] Codice doganale [' || raw."COMCO(k/*)" || '] valido'
    END                                          AS message,
    CASE
        WHEN raw."COMCO(k/*)" IS NULL OR raw."COMCO(k/*)" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_CommodityCodesEU01" ref
            WHERE ref."codice" = raw."COMCO(k/*)"
        )
            THEN 'Error'
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_MARITC" raw
LEFT JOIN LATERAL (
    SELECT "WERKS(k/*)"
    FROM raw."S_MARC"
    WHERE "PRODUCT(k/*)" = raw."MATNR(k/*)"
    ORDER BY "WERKS(k/*)"
    LIMIT 1
) marc ON TRUE
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK034'
)
;
