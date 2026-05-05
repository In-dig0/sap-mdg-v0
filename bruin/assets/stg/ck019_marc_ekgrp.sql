/* @bruin
name: stg.ck019_marc_ekgrp
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK019 — SAP_REF: Materiali (S_MARC): campo EKGRP (Gruppo acquisti)
  se valorizzato, deve essere presente in SAP_EXPORT_T024 (EKGRP).
  Se il campo è vuoto o NULL, il record viene ignorato.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARC'                                             AS source_table,
    'MAT'                                                AS category,
    raw."PRODUCT(k/*)"                                   AS object_key,
    'CK019'                                              AS check_id,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T024" ref
            WHERE ref."EKGRP" = raw."EKGRP"
        )
            THEN '[' || raw."WERKS(k/*)" || '] Gruppo acquisti [' || raw."EKGRP" || '] non presente in SAP (SAP_EXPORT_T024.EKGRP)'
        ELSE '[' || raw."WERKS(k/*)" || '] Gruppo acquisti [' || raw."EKGRP" || '] valido'
    END                                                  AS message,
    CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM ref."SAP_EXPORT_T024" ref
            WHERE ref."EKGRP" = raw."EKGRP"
        )                                                THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK019')
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_source"                                    AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MARC" raw
WHERE
    raw."EKGRP" IS NOT NULL AND raw."EKGRP" <> ''
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK019'
    )
;
