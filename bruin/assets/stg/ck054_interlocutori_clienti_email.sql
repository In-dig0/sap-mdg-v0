/* @bruin
name: stg.ck054_interlocutori_clienti_email
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK054 — FORMAT: Interlocutori clienti (S_CUST_CONT#ZBP-AddInterlocutore):
  il campo E_MAIL_B deve essere valorizzato e contenere un indirizzo email
  formalmente valido (pattern: <local>@<domain>.<tld>).
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_CUST_CONT#ZBP-AddInterlocutore'              AS source_table,
    'BP'                                            AS category,
    raw."KUNNR(k/*)"                                AS object_key,
    'CK054'                                         AS check_id,
    CASE
        WHEN raw."E_MAIL_B" IS NULL OR raw."E_MAIL_B" = ''
            THEN 'E_MAIL_B obbligatorio mancante per KUNNR [' || raw."KUNNR(k/*)" || ']'
        WHEN raw."E_MAIL_B" !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'
            THEN 'E_MAIL_B [' || raw."E_MAIL_B" || '] non è un indirizzo email valido'
                 || ' per KUNNR [' || raw."KUNNR(k/*)" || ']'
        ELSE 'E_MAIL_B [' || raw."E_MAIL_B" || '] valido per KUNNR [' || raw."KUNNR(k/*)" || ']'
    END                                             AS message,
    CASE
        WHEN raw."E_MAIL_B" IS NULL OR raw."E_MAIL_B" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK054')
        WHEN raw."E_MAIL_B" !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK054')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_CUST_CONT#ZBP-AddInterlocutore" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK054'
)
;
