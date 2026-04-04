/* @bruin
name: setup.check_catalog
type: pg.sql
depends:
  - setup.init_db
description: >
  Mantiene aggiornata la tabella stg.check_catalog.
  check_type:
    SAP_REF      — coerenza con tabelle di riferimento SAP
    EXISTENCE    — esistenza o duplicazione del dato
    CROSS_TABLE  — coerenza tra tabelle dello stesso archivio ZIP
connection: mdg_postgres
@bruin */

-- Aggiunge check_type se la colonna non esiste (idempotente)
ALTER TABLE stg.check_catalog
    ADD COLUMN IF NOT EXISTS check_type VARCHAR(20) NOT NULL DEFAULT 'EXISTENCE';

INSERT INTO stg.check_catalog (
    check_id, check_desc, category, target_table,
    target_field, ref_table, severity, check_type, is_active, updated_at
)
VALUES
    -- Fornitori — SAP_REF
    ('CHK01_SUPPL',
     'Fornitori: codice paese (COUNTRY) valorizzato e presente in T005S',
     'BP', 'S_SUPPL_GEN#ZBP_DatiGenerali', 'COUNTRY',
     'ref.EXPORT_T005S (LAND1)', 'Error', 'SAP_REF', TRUE, NOW()),
    ('CHK02_SUPPL',
     'Fornitori: coppia paese/regione (COUNTRY+REGION) presente in T005S',
     'BP', 'S_SUPPL_GEN#ZBP_DatiGenerali', 'COUNTRY + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)', 'Error', 'SAP_REF', TRUE, NOW()),
    -- Fornitori — EXISTENCE
    ('CHK03_SUPPL',
     'Fornitori: partita IVA mancante per soggetti UE/ExtraUE',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'TAXNUM(*)',
     NULL, 'Error', 'EXISTENCE', TRUE, NOW()),
    ('CHK04_SUPPL',
     'Fornitori: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'TAXTYPE(k/*) + TAXNUM(*)',
     NULL, 'Warning', 'EXISTENCE', TRUE, NOW()),
    -- Fornitori — CROSS_TABLE
    ('CHK05_SUPPL',
     'Fornitori: LIFNR in ZBP_CodiciFisc senza corrispondenza in ZBP_DatiGenerali',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW()),
    ('CHK06_SUPPL',
     'Fornitori: LIFNR in ZBP_AddInterlocutore senza corrispondenza in ZBP_DatiGenerali',
     'BP', 'S_SUPPL_CONT#ZBP-AddInterlocutore', 'LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW()),
    -- Clienti — SAP_REF
    ('CHK01_CUST',
     'Clienti: codice paese (COUNTRY) valorizzato e presente in T005S',
     'BP', 'S_CUST_GEN#ZBP-DatiGenerali', 'COUNTRY(*)',
     'ref.EXPORT_T005S (LAND1)', 'Error', 'SAP_REF', TRUE, NOW()),
    ('CHK02_CUST',
     'Clienti: coppia paese/regione (COUNTRY+REGION) presente in T005S',
     'BP', 'S_CUST_GEN#ZBP-DatiGenerali', 'COUNTRY(*) + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)', 'Error', 'SAP_REF', TRUE, NOW()),
    -- Clienti — EXISTENCE
    ('CHK03_CUST',
     'Clienti: partita IVA mancante per soggetti UE/ExtraUE',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'TAXNUM(*)',
     NULL, 'Error', 'EXISTENCE', TRUE, NOW()),
    ('CHK04_CUST',
     'Clienti: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'TAXTYPE(k/*) + TAXNUM(*)',
     NULL, 'Warning', 'EXISTENCE', TRUE, NOW()),
    -- Clienti — CROSS_TABLE
    ('CHK05_CUST',
     'Clienti: KUNNR in ZBP_CodiciFisc senza corrispondenza in ZBP_DatiGenerali',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW()),
    ('CHK06_CUST',
     'Clienti: KUNNR in ZBP_AddInterlocutore senza corrispondenza in ZBP_DatiGenerali',
     'BP', 'S_CUST_CONT#ZBP-AddInterlocutore', 'KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW())
ON CONFLICT (check_id) DO UPDATE SET
    check_desc   = EXCLUDED.check_desc,
    category     = EXCLUDED.category,
    target_table = EXCLUDED.target_table,
    target_field = EXCLUDED.target_field,
    ref_table    = EXCLUDED.ref_table,
    severity     = EXCLUDED.severity,
    check_type   = EXCLUDED.check_type,
    is_active    = EXCLUDED.is_active,
    updated_at   = NOW()
;
