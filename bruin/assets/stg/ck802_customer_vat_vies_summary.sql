/* @bruin
name: stg.ck802_customer_vat_vies_summary
type: pg.sql
depends:
  - stg.ck801_customer_vat_vies
description: >
  CK802 — EXT_REF: Clienti: sintesi verifica P.IVA EU/UK.
  Legge i risultati di dettaglio da stg.check_vat_vies (scritti da CK801)
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
SELECT
    'S_CUST_TAXNUMBERS#ZBP-CodiciFisc'          AS source_table,
    'BP'                                         AS category,
    v.entity_id                                  AS object_key,
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

    (SELECT MAX(run_id) FROM stg.check_vat_vies
     WHERE entity_type = 'customer')             AS run_id,

    v.zip_source                                 AS zip_source,
    NOW()                                        AS created_at

FROM stg.check_vat_vies v
WHERE v.run_id = (SELECT MAX(run_id) FROM stg.check_vat_vies WHERE entity_type = 'customer')
  AND v.entity_type = 'customer'
  AND v.check_status IN ('INVALID', 'ERROR', 'GB_NO_CREDENTIALS')
  AND (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK802'
  )
;
