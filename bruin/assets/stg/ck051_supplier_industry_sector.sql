/* @bruin
name: stg.ck051_supplier_industry_sector
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK051 — SAP_REF: Fornitori: coppia ISTYPE+IND_SECTOR presente in TB038A.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_INDUSTRY#ZBP_SettoriIndust'         AS source_table,
    'BP'                                         AS category,
    raw."LIFNR(k/*)"                             AS object_key,
    'CK051'                                      AS check_id,
    CASE
        WHEN raw."ISTYPE(k/*)" IS NULL OR raw."ISTYPE(k/*)" = ''
            THEN 'ISTYPE obbligatorio mancante'
        WHEN raw."IND_SECTOR(k/*)" IS NULL OR raw."IND_SECTOR(k/*)" = ''
            THEN 'IND_SECTOR obbligatorio mancante (ISTYPE=' || raw."ISTYPE(k/*)" || ')'
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TB038A" ref
            WHERE ref."ISTYPE"     = raw."ISTYPE(k/*)"
              AND ref."IND_SECTOR" = raw."IND_SECTOR(k/*)"
        )
            THEN 'Coppia settore industriale [' || raw."ISTYPE(k/*)" || '/' || raw."IND_SECTOR(k/*)" || '] non presente in SAP (TB038A)'
        ELSE 'Coppia settore industriale [' || raw."ISTYPE(k/*)" || '/' || raw."IND_SECTOR(k/*)" || '] valida'
    END                                          AS message,
    CASE
        WHEN raw."ISTYPE(k/*)"     IS NULL OR raw."ISTYPE(k/*)"     = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK051')
        WHEN raw."IND_SECTOR(k/*)" IS NULL OR raw."IND_SECTOR(k/*)" = ''  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK051')
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_TB038A" ref
            WHERE ref."ISTYPE"     = raw."ISTYPE(k/*)"
              AND ref."IND_SECTOR" = raw."IND_SECTOR(k/*)"
        )                                                                  THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK051')
        ELSE 'Ok'
    END                                          AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    raw."_source"                                AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_INDUSTRY#ZBP_SettoriIndust" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK051'
)
;
