/* @bruin
name: stg.ck020_marc_ekgrp_beskz
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK020 — EXISTENCE: Materiali (S_MARC): se il tipo di approvvigionamento
  BESKZ è esterno (F) o misto (X), il campo EKGRP deve essere valorizzato.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_MARC'                                             AS source_table,
    'MAT'                                                AS category,
    raw."PRODUCT(k/*)" || '/' || raw."WERKS(k/*)"       AS object_key,
    'CK020'                                              AS check_id,
    CASE
        WHEN raw."EKGRP" IS NULL OR raw."EKGRP" = ''
            THEN 'EKGRP obbligatorio quando BESKZ=' || raw."BESKZ" || ' (approvvigionamento esterno/misto)'
        ELSE 'EKGRP [' || raw."EKGRP" || '] correttamente valorizzato per BESKZ=' || raw."BESKZ"
    END                                                  AS message,
    CASE
        WHEN raw."EKGRP" IS NULL OR raw."EKGRP" = ''    THEN 'Error'
        ELSE 'Ok'
    END                                                  AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)                   AS run_id,
    raw."_zip_source"                                    AS zip_source,
    NOW()                                                AS created_at
FROM raw."S_MARC" raw
WHERE
    raw."BESKZ" IN ('F', 'X')
    AND (
        SELECT COALESCE(is_active, FALSE)
        FROM stg.check_catalog WHERE check_id = 'CK020'
    )
;
