/* @bruin
name: stg.chk04_supplier_taxnum_duplicati
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CHK04 — Individua codici fiscali duplicati tra BP diversi.
  Segnala come Warning i record in S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc
  dove la coppia [TAXTYPE(k/*) + TAXNUM(*)] è condivisa da più LIFNR(k/*).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc'         AS source_table,
    'BP'                                         AS category,
    t."LIFNR(k/*)"                               AS object_key,
    'CHK04'                                      AS check_id,
    'Codice fiscale [' || t."TAXTYPE(k/*)" || '/' || t."TAXNUM(*)" ||
    '] condiviso con altri ' || (dup.cnt - 1) || ' BP: ' || dup.altri_lifnr
                                                 AS message,
    'Warning'                                    AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    t."_zip_source"                              AS zip_source,
    NOW()                                        AS created_at
FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc" t
JOIN (
    SELECT
        "TAXTYPE(k/*)",
        "TAXNUM(*)",
        COUNT(DISTINCT "LIFNR(k/*)") AS cnt,
        STRING_AGG(DISTINCT "LIFNR(k/*)", ', ' ORDER BY "LIFNR(k/*)") AS altri_lifnr
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
    WHERE "TAXNUM(*)" IS NOT NULL
      AND "TAXNUM(*)" <> ''
    GROUP BY "TAXTYPE(k/*)", "TAXNUM(*)"
    HAVING COUNT(DISTINCT "LIFNR(k/*)") > 1
) dup
    ON  dup."TAXTYPE(k/*)" = t."TAXTYPE(k/*)"
    AND dup."TAXNUM(*)"    = t."TAXNUM(*)"
WHERE t."TAXNUM(*)" IS NOT NULL
  AND t."TAXNUM(*)" <> ''
;
