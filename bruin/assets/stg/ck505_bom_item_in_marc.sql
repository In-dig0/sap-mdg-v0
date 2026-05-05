/* @bruin
name: stg.ck505_bom_item_in_marc
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK505 — CROSS_SOURCE: Distinte base (S_BOM_ITEM): ogni componente
  (IDNRK + WERKS) deve essere presente nell'anagrafica materiali S_MARC
  come coppia (PRODUCT(k/*) + WERKS(k/*)).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_BOM_ITEM'                                  AS source_table,
    'MAT'                                         AS category,
    bom."MATNR(k/*)" || '|' ||
    bom."WERKS(k)"                                AS object_key,
    'CK505'                                       AS check_id,
    CASE
        WHEN bom."IDNRK" IS NULL OR bom."IDNRK" = ''
            THEN 'IDNRK (componente) obbligatorio mancante'
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARC" marc
            WHERE marc."PRODUCT(k/*)" = bom."IDNRK"
              AND marc."WERKS(k/*)"   = bom."WERKS(k)"
        )
            THEN 'Componente [' || bom."IDNRK" || '] / Divisione [' || bom."WERKS(k)" || '] non presente in S_MARC'
        ELSE 'Ok'
    END                                           AS message,
    CASE
        WHEN bom."IDNRK" IS NULL OR bom."IDNRK" = ''
            THEN 'Error'
        WHEN NOT EXISTS (
            SELECT 1 FROM raw."S_MARC" marc
            WHERE marc."PRODUCT(k/*)" = bom."IDNRK"
              AND marc."WERKS(k/*)"   = bom."WERKS(k)"
        )
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK505')
        ELSE 'Ok'
    END                                           AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)            AS run_id,
    bom."_source"                                 AS zip_source,
    NOW()                                         AS created_at
FROM raw."S_BOM_ITEM" bom
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK505'
)
;
