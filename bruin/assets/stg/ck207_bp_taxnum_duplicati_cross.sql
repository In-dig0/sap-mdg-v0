/* @bruin
name: stg.ck207_bp_taxnum_duplicati_cross
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK207 — EXISTENCE: Business Partner (Fornitori + Clienti): codice fiscale
  duplicato a livello di BP cross-tabella.
  Segnala le coppie TAXTYPE+TAXNUM condivise da più BP (LIFNR o KUNNR)
  tra le tabelle:
    - S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc
    - S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc
    - S_CUST_TAXNUMBERS#ZBP-CodiciFisc
  Escluso il TAXTYPE 'IT4'.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
WITH all_taxnums AS (
    -- S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc
    SELECT
        'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc'      AS source_table,
        "LIFNR(k/*)"                             AS bp_key,
        "TAXTYPE(k/*)"                           AS taxtype,
        "TAXNUM(*)"                              AS taxnum,
        "_source"                                AS zip_source
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
    WHERE "TAXNUM(*)" IS NOT NULL
      AND "TAXNUM(*)" <> ''
      AND "TAXTYPE(k/*)" <> 'IT4'

    UNION ALL

    -- S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc
    SELECT
        'S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc'   AS source_table,
        "LIFNR(k/*)"                             AS bp_key,
        "TAXTYPE(k/*)"                           AS taxtype,
        "TAXNUM(*)"                              AS taxnum,
        "_source"                                AS zip_source
    FROM raw."S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc"
    WHERE "TAXNUM(*)" IS NOT NULL
      AND "TAXNUM(*)" <> ''
      AND "TAXTYPE(k/*)" <> 'IT4'

    UNION ALL

    -- S_CUST_TAXNUMBERS#ZBP-CodiciFisc
    SELECT
        'S_CUST_TAXNUMBERS#ZBP-CodiciFisc'       AS source_table,
        "KUNNR(k/*)"                             AS bp_key,
        "TAXTYPE(k/*)"                           AS taxtype,
        "TAXNUM(*)"                              AS taxnum,
        "_source"                                AS zip_source
    FROM raw."S_CUST_TAXNUMBERS#ZBP-CodiciFisc"
    WHERE "TAXNUM(*)" IS NOT NULL
      AND "TAXNUM(*)" <> ''
      AND "TAXTYPE(k/*)" <> 'IT4'
),
dup AS (
    SELECT
        taxtype,
        taxnum,
        COUNT(DISTINCT bp_key)                                              AS cnt,
        STRING_AGG(DISTINCT source_table || ':' || bp_key,
                   ', ' ORDER BY source_table || ':' || bp_key)            AS altri_bp
    FROM all_taxnums
    GROUP BY taxtype, taxnum
    HAVING COUNT(DISTINCT bp_key) > 1
)
SELECT
    a.source_table                               AS source_table,
    'BP'                                         AS category,
    a.bp_key                                     AS object_key,
    'CK207'                                      AS check_id,
    'Codice fiscale [' || a.taxtype || '/' || a.taxnum ||
    '] condiviso con altri ' || (d.cnt - 1) || ' BP: ' || d.altri_bp
                                                 AS message,
    (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK207') AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,
    a.zip_source                                 AS zip_source,
    NOW()                                        AS created_at
FROM all_taxnums a
JOIN dup d ON d.taxtype = a.taxtype
          AND d.taxnum  = a.taxnum
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK207'
)
;
