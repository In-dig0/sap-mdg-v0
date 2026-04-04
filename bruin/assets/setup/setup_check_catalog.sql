/* @bruin
name: setup.check_catalog
type: pg.sql
depends:
  - setup.init_db
description: >
  Mantiene aggiornata la tabella stg.check_catalog con i check
  implementati nella pipeline MDG.
  Strategia: UPSERT su check_id.
connection: mdg_postgres
@bruin */

INSERT INTO stg.check_catalog (
    check_id, check_desc, category, target_table,
    target_field, ref_table, severity, is_active, updated_at
)
VALUES
    -- Vettori/Fornitori
    ('CHK01_SUPPL', 'Fornitori: codice paese (COUNTRY) valorizzato e presente in T005S',
     'BP', 'S_SUPPL_GEN#ZBP_DatiGenerali', 'COUNTRY',
     'ref.EXPORT_T005S (LAND1)', 'Error', TRUE, NOW()),
    ('CHK02_SUPPL', 'Fornitori: coppia paese/regione (COUNTRY+REGION) presente in T005S',
     'BP', 'S_SUPPL_GEN#ZBP_DatiGenerali', 'COUNTRY + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)', 'Error', TRUE, NOW()),
    ('CHK03_SUPPL', 'Fornitori: partita IVA mancante per soggetti UE/ExtraUE',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'TAXNUM(*)',
     NULL, 'Error', TRUE, NOW()),
    ('CHK04_SUPPL', 'Fornitori: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP', 'S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc', 'TAXTYPE(k/*) + TAXNUM(*)',
     NULL, 'Warning', TRUE, NOW()),
    -- Clienti
    ('CHK01_CUST', 'Clienti: codice paese (COUNTRY) valorizzato e presente in T005S',
     'BP', 'S_CUST_GEN#ZBP-DatiGenerali', 'COUNTRY',
     'ref.EXPORT_T005S (LAND1)', 'Error', TRUE, NOW()),
    ('CHK02_CUST', 'Clienti: coppia paese/regione (COUNTRY+REGION) presente in T005S',
     'BP', 'S_CUST_GEN#ZBP-DatiGenerali', 'COUNTRY + REGION',
     'ref.EXPORT_T005S (LAND1+BLAND)', 'Error', TRUE, NOW()),
    ('CHK03_CUST', 'Clienti: partita IVA mancante per soggetti UE/ExtraUE',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'TAXNUM(*)',
     NULL, 'Error', TRUE, NOW()),
    ('CHK04_CUST', 'Clienti: codice fiscale duplicato tra BP diversi (TAXTYPE+TAXNUM)',
     'BP', 'S_CUST_TAXNUMBERS#ZBP-CodiciFisc', 'TAXTYPE(k/*) + TAXNUM(*)',
     NULL, 'Warning', TRUE, NOW())
ON CONFLICT (check_id) DO UPDATE SET
    check_desc   = EXCLUDED.check_desc,
    category     = EXCLUDED.category,
    target_table = EXCLUDED.target_table,
    target_field = EXCLUDED.target_field,
    ref_table    = EXCLUDED.ref_table,
    severity     = EXCLUDED.severity,
    is_active    = EXCLUDED.is_active,
    updated_at   = NOW()
;
