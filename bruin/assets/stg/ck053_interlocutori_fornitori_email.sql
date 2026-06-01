/* @bruin
name: stg.ck053_interlocutori_email
type: pg.sql
depends:
  - stg.clean_check_results
description: >
  CK053 — FORMAT: Interlocutori fornitori e clienti:
  il campo E_MAIL_B deve essere valorizzato e contenere un indirizzo email
  formalmente valido (pattern: <local>@<domain>.<tld>).
  Tabelle: S_SUPPL_CONT#ZBP-AddInterlocutore, S_CUST_CONT#ZBP-AddInterlocutore.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_results (
    source_table, category, object_key, check_id,
    message, status, run_id, zip_source, created_at
)
SELECT
    'S_SUPPL_CONT#ZBP-AddInterlocutore'             AS source_table,
    'BP'                                            AS category,
    raw."LIFNR(k/*)"                                AS object_key,
    'CK053'                                         AS check_id,
    CASE
        WHEN raw."E_MAIL_B" IS NULL OR raw."E_MAIL_B" = ''
            THEN 'E_MAIL_B obbligatorio mancante per LIFNR [' || raw."LIFNR(k/*)" || ']'
        WHEN raw."E_MAIL_B" !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'
            THEN 'E_MAIL_B [' || raw."E_MAIL_B" || '] non è un indirizzo email valido'
                 || ' per LIFNR [' || raw."LIFNR(k/*)" || ']'
        ELSE 'E_MAIL_B [' || raw."E_MAIL_B" || '] valido per LIFNR [' || raw."LIFNR(k/*)" || ']'
    END                                             AS message,
    CASE
        WHEN raw."E_MAIL_B" IS NULL OR raw."E_MAIL_B" = ''
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK053')
        WHEN raw."E_MAIL_B" !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$'
            THEN (SELECT severity FROM stg.check_catalog WHERE check_id = 'CK053')
        ELSE 'Ok'
    END                                             AS status,
    (SELECT run_id::integer FROM stg.pipeline_runs
     WHERE status = 'running'
     ORDER BY started_at DESC LIMIT 1)              AS run_id,
    raw."_source"                                   AS zip_source,
    NOW()                                           AS created_at
FROM raw."S_SUPPL_CONT#ZBP-AddInterlocutore" raw
WHERE (
    SELECT COALESCE(is_active, FALSE)
    FROM stg.check_catalog WHERE check_id = 'CK053'
)
;
