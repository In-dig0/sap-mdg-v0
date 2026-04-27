/* @bruin
name: setup.check_catalog
type: pg.sql
depends:
  - setup.init_db
description: >
  Catalogo check MDG con naming convention:
    CK001-CK099  → SAP_REF      (coerenza tabelle di riferimento SAP)
    CK201-CK299  → EXISTENCE    (esistenza dato obbligatorio / duplicati)
    CK401-CK499  → CROSS_TABLE  (coerenza referenziale tra tabelle dello stesso ZIP)
    CK501-CK599  → CROSS_SOURCE (coerenza referenziale tra tabelle di ZIP/sorgenti diversi)
    CK801-CK899  → EXT_REF      (verifiche tramite servizi esterni)
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
    ('CK016','Materiali (S_MARA): tipo materiale MTART(*) obbligatorio e presente in SAP_EXPORT_T134',
     'MAT','S_MARA','MTART(*)',
     'ref.SAP_EXPORT_T134 (MTART)','Error','SAP_REF',TRUE,NOW()),
    ('CK017','Materiali (S_MARA): peso netto NTGEW obbligatorio e diverso da zero',
     'MAT','S_MARA','NTGEW',
     NULL,'Error','EXISTENCE',TRUE,NOW()),
    ('CK018','Materiali (S_MARA): gerarchia prodotto PRDHA se valorizzata presente in SAP_EXPORT_PRDHA',
     'MAT','S_MARA','PRDHA',
     'ref.SAP_EXPORT_PRDHA (PRDHA)','Error','SAP_REF',TRUE,NOW()),
    ('CK019','Materiali (S_MARC): gruppo acquisti EKGRP se valorizzato presente in SAP_EXPORT_T024',
     'MAT','S_MARC','EKGRP',
     'ref.SAP_EXPORT_T024 (EKGRP)','Error','SAP_REF',TRUE,NOW()),
    ('CK020','Materiali (S_MARC): se BESKZ in (F,X) allora EKGRP deve essere valorizzato',
     'MAT','S_MARC','EKGRP',
     NULL,'Error','EXISTENCE',TRUE,NOW()),
    ('CK021','Materiali (S_MARC): coppia WERKS+DISPO presente in SAP_EXPORT_T024D',
     'MAT','S_MARC','WERKS(k/*) + DISPO',
     'ref.SAP_EXPORT_T024D (WERKS+DISPO)','Error','SAP_REF',TRUE,NOW()),
    ('CK022','Materiali (S_MBEW): classe di valorizzazione BKLAS(*) obbligatoria e presente in SAP_EXPORT_T025',
     'MAT','S_MBEW','BKLAS(*)',
     'ref.SAP_EXPORT_T025 (BKLAS)','Error','SAP_REF',TRUE,NOW()),
    ('CK023','Materiali (S_MARA): stato materiale MSTAE obbligatorio e presente in SAP_EXPORT_T141',
     'MAT','S_MARA','MSTAE',
     'ref.SAP_EXPORT_T141 (MMSTA)','Error','SAP_REF',TRUE,NOW()),
    ('CK024','Materiali (S_MARC): profit center PRCTR obbligatorio e presente in SAP_EXPORT_CEPC',
     'MAT','S_MARC','PRCTR',
     'ref.SAP_EXPORT_CEPC (PRCTR)','Error','SAP_REF',TRUE,NOW()),
    ('CK025','Materiali (S_MVKE): gruppo prezzi KONDM se valorizzato presente in SAP_EXPORT_T178',
     'MAT','S_MVKE','KONDM',
     'ref.SAP_EXPORT_T178 (KONDM)','Error','SAP_REF',TRUE,NOW()),
    ('CK026','Materiali (S_MARC): tipo approvvigionamento BESKZ obbligatorio e presente in SAP_EXPORT_T460A',
     'MAT','S_MARC','BESKZ',
     'ref.SAP_EXPORT_T460A (BESKZ)','Error','SAP_REF',TRUE,NOW()),
    ('CK027','Materiali (S_MARC): tipo approvvigionamento speciale SOBSL se valorizzato presente in SAP_EXPORT_T460A',
     'MAT','S_MARC','SOBSL',
     'ref.SAP_EXPORT_T460A (SOBSL)','Error','SAP_REF',TRUE,NOW()),
    ('CK028','Materiali (S_MARC): tipo fabbisogno MTVFP obbligatorio e presente in SAP_EXPORT_TMVF',
     'MAT','S_MARC','MTVFP',
     'ref.SAP_EXPORT_TMVF (MTVFP)','Error','SAP_REF',TRUE,NOW()),
    ('CK029','Fornitori (S_SUPPL_COMPANY): condizione pagamento ZTERM1 obbligatoria e presente in SAP_EXPORT_T052',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','ZTERM1',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),
    ('CK030','Clienti (S_CUST_COMPANY#ZBP-DatiSocieta): condizione pagamento ZTERM obbligatoria e presente in SAP_EXPORT_T052',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','ZTERM',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),
    ('CK031','Fornitori (S_SUPPL_COMPANY#ZBP_DatiSocieta): modalità pagamento ZWELS_01 obbligatoria e presente in SAP_EXPORT_T042Z',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',TRUE,NOW()),
    ('CK032','Clienti (S_CUST_COMPANY#ZBP-DatiSocieta): modalità pagamento ZWELS_01 obbligatoria e presente in SAP_EXPORT_T042Z',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',TRUE,NOW()),
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
     'S_CUST_GEN#ZBP-DatiGenerali','Error','CROSS_TABLE',TRUE,NOW()),
    -- CROSS_SOURCE
    ('CK501','Materiali: distinta base (BOM) obbligatoria per produzione interna o mista (BESKZ in E, X)',
     'MAT','S_MARC','BESKZ',
     'S_BOM_HEADER (MATNR)','Error','CROSS_SOURCE',TRUE,NOW()),
    ('CK502','Materiali: ciclo di lavoro standard (PLNAL=01) obbligatorio per produzione interna o mista (BESKZ in E, X)',
     'MAT','S_MARC','BESKZ',
     'S_MAPL (MATNR + WERKS_MAT + PLNAL)','Error','CROSS_SOURCE',TRUE,NOW()),
    ('CK503','Materiali: inforecord acquisti obbligatorio per acquisto esterno puro (BESKZ=F e SOBSL vuoto)',
     'MAT','S_MARC','BESKZ + SOBSL',
     'S_EINA#INFORMATFOR (MATNR)','Error','CROSS_SOURCE',TRUE,NOW()),
    ('CK504','Materiali S_MARC devono essere presenti in A2F a parità di articolo e divisione (IT11→A2F_BO, IT12→A2F_FA)',
     'MAT','S_MARC','PRODUCT(k/*) + WERKS(k/*)',
     'A2F_BO / A2F_FA (CODART)','Error','CROSS_SOURCE',TRUE,NOW())
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
