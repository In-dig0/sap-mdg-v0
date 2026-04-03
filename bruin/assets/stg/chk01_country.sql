/* @bruin
name: stg.chk01_country
type: pg.sql
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
description: >
  CHK01 — Verifica che il campo COUNTRY della tabella
  S_SUPPL_GEN#ZBP_DatiGenerali esista nella tabella di
  controllo SAP ref.EXPORT_T005S (colonna LAND1).
  Scrive i risultati in stg.check_results.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table,
    category,
    object_key,
    check_id,
    message,
    status,
    created_at
)
SELECT
    'S_SUPPL_GEN#ZBP_DatiGenerali'         AS source_table,
    'BP'                                    AS category,
    raw."LIFNR(k/*)"                        AS object_key,
    'CHK01'                                 AS check_id,
    'Codice paese [' || raw."COUNTRY" || '] non presente in SAP (T005S.LAND1)' AS message,
    'Error'                                 AS status,
    NOW()                                   AS created_at
FROM
    raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE
    raw."COUNTRY" IS NOT NULL
    AND raw."COUNTRY" <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM ref."EXPORT_T005S" ref
        WHERE ref."LAND1" = raw."COUNTRY"
    )
;
