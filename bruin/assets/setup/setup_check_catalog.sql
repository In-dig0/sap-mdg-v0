/* @bruin
name: setup.check_catalog
type: pg.sql
depends:
  - setup.init_db
description: >
  Catalogo check MDG con nuova naming convention:
    CK001-CK100  → SAP_REF
    CK201-CK299  → EXISTENCE
    CK401-CK499  → CROSS_TABLE
connection: mdg_postgres
@bruin */

ALTER TABLE stg.check_catalog
    ADD COLUMN IF NOT EXISTS check_type VARCHAR(20) NOT NULL DEFAULT 'EXISTENCE';

INSERT INTO stg.check_catalog (
    check_id, check_desc, category, target_table,
    target_field, ref_table, severity, check_type, is_active, updated_at
)
VALUES
    -- SAP_REF
    ('CK001','Fornitori: codice paese (COUNTRY) presente in T005S',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali','COUNTRY',
     'ref.EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),
    ('CK002','Fornitori: coppia COUNTRY+REGION presente in T005S',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali','COUNTRY + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),
    ('CK003','Clienti: codice paese COUNTRY(*) presente in T005S',
     'BP','S_CUST_GEN#ZBP-DatiGenerali','COUNTRY(*)',
     'ref.EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),
    ('CK004','Clienti: coppia COUNTRY(*)+REGION presente in T005S',
     'BP','S_CUST_GEN#ZBP-DatiGenerali','COUNTRY(*) + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),
    -- EXISTENCE
    ('CK201','Fornitori: partita IVA mancante per soggetti UE/ExtraUE',
     'BP','S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc','TAXNUM(*)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),
    ('CK202','Fornitori: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP','S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc','TAXTYPE(k/*) + TAXNUM(*)',
     NULL,'Warning','EXISTENCE',TRUE,NOW()),
    ('CK203','Clienti: partita IVA mancante per soggetti UE/ExtraUE',
     'BP','S_CUST_TAXNUMBERS#ZBP-CodiciFisc','TAXNUM(*)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),
    ('CK204','Clienti: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP','S_CUST_TAXNUMBERS#ZBP-CodiciFisc','TAXTYPE(k/*) + TAXNUM(*)',
     NULL,'Warning','EXISTENCE',TRUE,NOW()),
    -- CROSS_TABLE
    ('CK401','Orfani flusso 01-ZBP-Vettori: LIFNR assente nella master',
     'BP','varie (tabelle secondarie 01-ZBP-Vettori)','LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali','Error','CROSS_TABLE',TRUE,NOW()),
    ('CK402','Orfani flusso 04-ZBP-Fornitori: LIFNR assente nella master',
     'BP','varie (tabelle secondarie 04-ZBP-Fornitori)','LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali','Error','CROSS_TABLE',TRUE,NOW()),
    ('CK403','Orfani flusso 02-ZDM-Clienti: KUNNR assente nella master',
     'BP','varie (tabelle secondarie 02-ZDM-Clienti)','KUNNR(k/*)',
     'S_CUST_GEN#ZDM-DatiGenerali','Error','CROSS_TABLE',TRUE,NOW()),
    ('CK404','Orfani flusso 03-ZBP-Clienti: KUNNR assente nella master',
     'BP','varie (tabelle secondarie 03-ZBP-Clienti)','KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali','Error','CROSS_TABLE',TRUE,NOW())
ON CONFLICT (check_id) DO UPDATE SET
    check_desc   = EXCLUDED.check_desc,
    category     = EXCLUDED.category,
    target_table = EXCLUDED.target_table,
    target_field = EXCLUDED.target_field,
    ref_table    = EXCLUDED.ref_table,
    severity     = EXCLUDED.severity,
    check_type   = EXCLUDED.check_type,
    updated_at   = NOW()
;
