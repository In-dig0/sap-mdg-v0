/* @bruin
name: stg.chk02_country_region
type: pg.sql
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
description: >
  CHK02 — Verifica che la coppia COUNTRY+REGION della tabella
  S_SUPPL_GEN#ZBP_DatiGenerali esista nella tabella di
  controllo SAP ref.EXPORT_T005S (colonne LAND1+BLAND).
  COUNTRY e REGION sono entrambi obbligatori per SAP.
  Segnala: REGION vuota/nulla E coppia non presente in T005S.
connection: mdg_postgres
@bruin */

-- Pulizia esecuzioni precedenti (idempotente)
DELETE FROM stg.check_results
WHERE check_id = 'CHK02'
  AND source_table = 'S_SUPPL_GEN#ZBP_DatiGenerali';

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
    'S_SUPPL_GEN#ZBP_DatiGenerali'  AS source_table,
    'BP'                             AS category,
    raw."LIFNR(k/*)"                 AS object_key,
    'CHK02'                          AS check_id,
    CASE
        WHEN raw."REGION" IS NULL OR raw."REGION" = ''
            THEN 'REGION obbligatoria mancante (COUNTRY=' || COALESCE(raw."COUNTRY", 'NULL') || ')'
        ELSE
            'Coppia paese/regione [' || raw."COUNTRY" || '/' || raw."REGION" || '] non presente in SAP (T005S)'
    END                              AS message,
    'Error'                          AS status,
    NOW()                            AS created_at
FROM raw."S_SUPPL_GEN#ZBP_DatiGenerali" raw
WHERE
    -- Caso 1: REGION vuota o nulla (campo obbligatorio)
    (raw."REGION" IS NULL OR raw."REGION" = '')
    OR
    -- Caso 2: coppia COUNTRY+REGION non presente in T005S
    (
        raw."REGION" IS NOT NULL
        AND raw."REGION" <> ''
        AND NOT EXISTS (
            SELECT 1
            FROM ref."EXPORT_T005S" ref
            WHERE ref."LAND1" = raw."COUNTRY"
              AND ref."BLAND" = raw."REGION"
        )
    )
;
