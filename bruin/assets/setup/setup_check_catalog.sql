/* @bruin
name: setup.check_catalog
type: pg.sql
depends:
  - setup.init_db
description: >
  Mantiene aggiornata la tabella stg.check_catalog.
  Legge lo stato attivo/inattivo da /project/bruin/config/check_states.json
  tramite la funzione pg_read_file (non disponibile) — lo stato viene
  applicato dall'asset Python setup.apply_check_states che gira dopo.
  check_type:
    SAP_REF      — coerenza con tabelle di riferimento SAP
    EXISTENCE    — esistenza o duplicazione del dato
    CROSS_TABLE  — coerenza tra tabelle dello stesso archivio ZIP
connection: mdg_postgres
@bruin */

ALTER TABLE stg.check_catalog
    ADD COLUMN IF NOT EXISTS check_type VARCHAR(20) NOT NULL DEFAULT 'EXISTENCE';

INSERT INTO stg.check_catalog (
    check_id, check_desc, category, target_table,
    target_field, ref_table, severity, check_type, is_active, updated_at
)
VALUES
    ('CHK01_SUPPL',
     'Fornitori: codice paese (COUNTRY) valorizzato e presente in T005S',
     'BP', 'S_SUPPL_GEN#ZBP_DatiGenerali', 'COUNTRY',
     'ref.EXPORT_T005S (LAND1)', 'Error', 'SAP_REF', TRUE, NOW()),
    ('CHK02_SUPPL',
     'Fornitori: coppia paese/regione (COUNTRY+REGION) presente in T005S',
     'BP', 'S_SUPPL_GEN#ZBP_DatiGenerali', 'COUNTRY + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)', 'Error', 'SAP_REF', TRUE, NOW()),
    ('CHK03_SUPPL',
     'Fornitori: partita IVA mancante per soggetti UE/ExtraUE',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'TAXNUM(*)',
     NULL, 'Error', 'EXISTENCE', TRUE, NOW()),
    ('CHK04_SUPPL',
     'Fornitori: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'TAXTYPE(k/*) + TAXNUM(*)',
     NULL, 'Warning', 'EXISTENCE', TRUE, NOW()),
    ('CHK05_SUPPL',
     'Fornitori: chiave FK orfana nelle tabelle secondarie del flusso ZBP',
     'BP', 'varie (tabelle secondarie flusso ZBP fornitori)', 'LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW()),
    ('CHK01_CUST',
     'Clienti: codice paese (COUNTRY) valorizzato e presente in T005S',
     'BP', 'S_CUST_GEN#ZBP-DatiGenerali', 'COUNTRY(*)',
     'ref.EXPORT_T005S (LAND1)', 'Error', 'SAP_REF', TRUE, NOW()),
    ('CHK02_CUST',
     'Clienti: coppia paese/regione (COUNTRY+REGION) presente in T005S',
     'BP', 'S_CUST_GEN#ZBP-DatiGenerali', 'COUNTRY(*) + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)', 'Error', 'SAP_REF', TRUE, NOW()),
    ('CHK03_CUST',
     'Clienti: partita IVA mancante per soggetti UE/ExtraUE',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'TAXNUM(*)',
     NULL, 'Error', 'EXISTENCE', TRUE, NOW()),
    ('CHK04_CUST',
     'Clienti: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'TAXTYPE(k/*) + TAXNUM(*)',
     NULL, 'Warning', 'EXISTENCE', TRUE, NOW()),
    ('CHK05_CUST',
     'Clienti ZBP: chiave FK orfana nelle tabelle secondarie del flusso ZBP',
     'BP', 'varie (tabelle secondarie flusso ZBP clienti)', 'KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW()),
    ('CHK05_CUST_ZDM',
     'Clienti ZDM: chiave FK orfana nelle tabelle secondarie del flusso ZDM',
     'BP', 'varie (tabelle secondarie flusso ZDM clienti)', 'KUNNR(k/*)',
     'S_CUST_GEN#ZDM-DatiGenerali', 'Error', 'CROSS_TABLE', TRUE, NOW())
ON CONFLICT (check_id) DO UPDATE SET
    check_desc   = EXCLUDED.check_desc,
    category     = EXCLUDED.category,
    target_table = EXCLUDED.target_table,
    target_field = EXCLUDED.target_field,
    ref_table    = EXCLUDED.ref_table,
    severity     = EXCLUDED.severity,
    check_type   = EXCLUDED.check_type,
    -- NON aggiorniamo is_active: viene gestito da apply_check_states
    updated_at   = NOW()
;
