/* @bruin
name: stg.ck802_customer_vat_vies_summary
type: pg.sql
depends:
  - stg.ck801_customer_vat_vies
description: >
  CK802 — EXT_REF: Clienti: sintesi verifica P.IVA EU/UK.
  Per ogni P.IVA presente oggi in raw."S_CUST_TAXNUMBERS#ZBP-CodiciFisc" (TAXTYPE '%0')
  prende l'ultimo esito reale in stg.check_vat_vies (scritto da CK801, anche
  in un run precedente: i risultati riusati dalla cache non vengono riscritti)
  e scrive una riga in stg.check_results per ogni record anomalo:
    - INVALID           → status 'Error'
    - ERROR             → status 'Error' (servizio VIES/HMRC irraggiungibile)
    - GB_NO_CREDENTIALS → status 'Warning' (credenziali HMRC non configurate)
  I record VALID e NOT_EU non generano righe in check_results.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
WITH vat_attuali AS (
    -- Stessa normalizzazione di normalize_taxnum() in CK801: CC + numero senza spazi, punti, trattini
    SELECT DISTINCT
        raw."KUNNR(k/*)"                                    AS entity_id,
        CASE
            WHEN regexp_replace(UPPER(raw."TAXNUM(*)"), '[[:space:].-]', '', 'g')
                 LIKE UPPER(LEFT(raw."TAXTYPE(k/*)", 2)) || '%'
                THEN regexp_replace(UPPER(raw."TAXNUM(*)"), '[[:space:].-]', '', 'g')
            ELSE UPPER(LEFT(raw."TAXTYPE(k/*)", 2))
                 || regexp_replace(UPPER(raw."TAXNUM(*)"), '[[:space:].-]', '', 'g')
        END                                            AS vat_number,
        raw."_source"                                  AS zip_source
    FROM raw."S_CUST_TAXNUMBERS#ZBP-CodiciFisc" raw
    WHERE raw."TAXTYPE(k/*)" LIKE '%0'
      AND raw."TAXNUM(*)" IS NOT NULL
      AND TRIM(raw."TAXNUM(*)") <> ''
),
ultimo_esito AS (
    SELECT DISTINCT ON (entity_id, vat_number)
        entity_id, vat_number, check_status, is_gb, error_message
    FROM stg.check_vat_vies
    WHERE entity_type = 'customer'
      AND check_status NOT IN ('CACHED', 'PENDING')
    ORDER BY entity_id, vat_number, created_at DESC
)
SELECT
    'S_CUST_TAXNUMBERS#ZBP-CodiciFisc'           AS source_table,
    'BP'                                         AS category,
    a.entity_id                                  AS object_key,
    'CK802'                                      AS check_id,

    CASE v.check_status
        WHEN 'INVALID' THEN
            'P.IVA [' || v.vat_number || '] non valida secondo ' ||
            CASE WHEN v.is_gb THEN 'HMRC API v2' ELSE 'VIES EU' END
        WHEN 'ERROR' THEN
            'Errore verifica P.IVA [' || v.vat_number || '] — ' ||
            COALESCE(v.error_message, 'errore sconosciuto')
        WHEN 'GB_NO_CREDENTIALS' THEN
            'P.IVA UK [' || v.vat_number || '] non verificata: ' ||
            'credenziali HMRC non configurate (impostare HMRC_CLIENT_ID/SECRET nel .env)'
        ELSE v.check_status
    END                                          AS message,

    CASE v.check_status
        WHEN 'INVALID'           THEN 'Error'
        WHEN 'ERROR'             THEN 'Error'
        WHEN 'GB_NO_CREDENTIALS' THEN 'Warning'
        ELSE 'Ok'
    END                                          AS status,

    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)           AS run_id,

    a.zip_source                                 AS zip_source,
    NOW()                                        AS created_at

FROM vat_attuali a
JOIN ultimo_esito v
  ON v.entity_id  = a.entity_id
 AND v.vat_number = a.vat_number
WHERE v.check_status IN ('INVALID', 'ERROR', 'GB_NO_CREDENTIALS')
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK802'
  )
;
