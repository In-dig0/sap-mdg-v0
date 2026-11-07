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
  Categorie (category): BP, MAT, BOM, ROUTING, PIR, CMIR, PRICE, PO
  (etichette in streamlit/app/pages/2_check_catalog.py).
connection: mdg_postgres
@bruin */

ALTER TABLE stg.check_catalog
    ADD COLUMN IF NOT EXISTS check_type VARCHAR(20) NOT NULL DEFAULT 'EXISTENCE';

-- Esclusione dall'output dei record che non superano il controllo:
--   exclude_supported: l'export/merge del flusso sa escludere le righe del check (impostato qui);
--   exclude_output:    esclusione attiva (default qui sotto, scelta dell'utente in
--                      check_states.json applicata da setup.apply_check_states).
-- Un record è escluso solo se il check è attivo e exclude_output è attivo.
ALTER TABLE stg.check_catalog
    ADD COLUMN IF NOT EXISTS exclude_supported BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS exclude_output    BOOLEAN NOT NULL DEFAULT FALSE;

INSERT INTO stg.check_catalog (
    check_id, check_desc, category, target_table,
    target_field, ref_table, severity, check_type, is_active, updated_at
)
VALUES
    -- SAP_REF — CK001-CK004
    ('CK001','Fornitori: il paese dell''indirizzo deve esistere tra i paesi configurati in SAP',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali','COUNTRY',
     'ref.SAP_EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),

    ('CK002','Fornitori: la provincia/regione dell''indirizzo deve esistere in SAP per il paese indicato',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali','COUNTRY + REGION',
     'ref.SAP_EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),

    ('CK003','Clienti: il paese dell''indirizzo deve esistere tra i paesi configurati in SAP',
     'BP','S_CUST_GEN#ZBP-DatiGenerali','COUNTRY(*)',
     'ref.SAP_EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),

    ('CK004','Clienti: la provincia/regione dell''indirizzo deve esistere in SAP per il paese indicato',
     'BP','S_CUST_GEN#ZBP-DatiGenerali','COUNTRY(*) + REGION',
     'ref.SAP_EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),

    ('CK005','Fornitori estesi da clienti (05a), dati di società: la condizione di pagamento deve esistere in SAP',
     'BP','S_SUPPL_COMPANY#ZBP_AddDatiSocieta','ZTERM1',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),

    ('CK006','Fornitori, dati di società: la condizione di pagamento deve esistere in SAP',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','ZTERM1',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),
     
    ('CK007','Clienti, dati di società: la condizione di pagamento deve esistere in SAP',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','ZTERM1',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),

    ('CK008','Fornitori, dati di società: la modalità di pagamento deve esistere in SAP',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',TRUE,NOW()),

    ('CK009','Fornitori estesi da clienti (05a), dati di società: la modalità di pagamento deve esistere in SAP',
     'BP','S_SUPPL_COMPANY#ZBP_AddDatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',TRUE,NOW()),

    ('CK010','Clienti, dati di società: la modalità di pagamento deve esistere in SAP',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',TRUE,NOW()),

    ('CK011','Fornitori, dati di società: il conto di riconciliazione deve essere tra quelli previsti per i fornitori',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','AKONT(*)',
     'ref.SAP_Conto_Riconciliazione_Fornitori (Cod_Conto)','Error','SAP_REF',TRUE,NOW()),

    ('CK012','Clienti, dati di società: il conto di riconciliazione deve essere tra quelli previsti per i clienti',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','AKONT(*)',
     'ref.SAP_Conto_Riconciliazione_Clienti (Cod_Conto)','Error','SAP_REF',TRUE,NOW()),

    ('CK013','Fornitori estesi da clienti (05a), dati di società: il conto di riconciliazione deve essere tra quelli previsti per i fornitori',
     'BP','S_SUPPL_COMPANY#ZBP_AddDatiSocieta','AKONT(*)',
     'ref.SAP_Conto_Riconciliazione_Fornitori (Cod_Conto)','Error','SAP_REF',TRUE,NOW()),

    ('CK014','Materiali: il gruppo merci esterno, se indicato, deve esistere in SAP',
     'MAT','S_MARA','EXTWG',
     'ref.SAP_EXPORT_TWEW (EXTWG)','Error','SAP_REF',TRUE,NOW()),

    ('CK015','Materiali: il gruppo merci è obbligatorio e deve esistere in SAP',
     'MAT','S_MARA','MATKL',
     'ref.SAP_EXPORT_T023 (MATKL)','Error','SAP_REF',TRUE,NOW()),
   
    ('CK016','Materiali: il tipo materiale è obbligatorio e deve esistere in SAP',
     'MAT','S_MARA','MTART(*)',
     'ref.SAP_EXPORT_T134 (MTART)','Error','SAP_REF',TRUE,NOW()),

    ('CK017','Materiali: pesi netto e lordo obbligatori e maggiori di zero (lordo non inferiore al netto), con unità di peso; per gli imballi solo il peso lordo',
     'MAT','S_MARA','NTGEW + BRGEW + GEWEI',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK018','Materiali: la gerarchia prodotto, se indicata, deve esistere in SAP',
     'MAT','S_MARA','PRDHA',
     'ref.SAP_EXPORT_PRDHA (PRDHA)','Error','SAP_REF',TRUE,NOW()),

    ('CK019','Materiali, dati di divisione: il gruppo acquisti, se indicato, deve esistere in SAP',
     'MAT','S_MARC','EKGRP',
     'ref.SAP_EXPORT_T024 (EKGRP)','Error','SAP_REF',TRUE,NOW()),

    ('CK020','Materiali, dati di divisione: per i materiali acquistati (approvvigionamento esterno o misto) il gruppo acquisti è obbligatorio',
     'MAT','S_MARC','EKGRP',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK021','Materiali, dati di divisione: il responsabile della pianificazione MRP deve esistere in SAP per la divisione',
     'MAT','S_MARC','WERKS(k/*) + DISPO',
     'ref.SAP_EXPORT_T024D (WERKS+DISPO)','Error','SAP_REF',TRUE,NOW()),

    ('CK022','Materiali, dati di valorizzazione: la classe di valorizzazione è obbligatoria e deve esistere in SAP',
     'MAT','S_MBEW','BKLAS(*)',
     'ref.SAP_EXPORT_T025 (BKLAS)','Error','SAP_REF',TRUE,NOW()),

    ('CK023','Materiali: lo stato del materiale è obbligatorio e deve esistere in SAP',
     'MAT','S_MARA','MSTAE',
     'ref.SAP_EXPORT_T141 (MMSTA)','Error','SAP_REF',TRUE,NOW()),

    ('CK024','Materiali, dati di divisione: il profit center è obbligatorio e deve esistere in SAP',
     'MAT','S_MARC','PRCTR',
     'ref.SAP_EXPORT_CEPC (PRCTR)','Error','SAP_REF',TRUE,NOW()),
     
    ('CK025','Materiali, dati di vendita: il gruppo prezzi materiale, se indicato, deve esistere in SAP',
     'MAT','S_MVKE','KONDM',
     'ref.SAP_EXPORT_T178 (KONDM)','Error','SAP_REF',TRUE,NOW()),

    ('CK026','Materiali, dati di divisione: il tipo di approvvigionamento è obbligatorio e deve esistere in SAP',
     'MAT','S_MARC','BESKZ',
     'ref.SAP_EXPORT_T460A (BESKZ)','Error','SAP_REF',TRUE,NOW()),
     
    ('CK027','Materiali, dati di divisione: il tipo di approvvigionamento speciale, se indicato, deve esistere in SAP',
     'MAT','S_MARC','SOBSL',
     'ref.SAP_EXPORT_T460A (SOBSL)','Error','SAP_REF',TRUE,NOW()),

    ('CK028','Materiali, dati di divisione: il gruppo di verifica disponibilità è obbligatorio e deve esistere in SAP',
     'MAT','S_MARC','MTVFP',
     'ref.SAP_EXPORT_TMVF (MTVFP)','Error','SAP_REF',TRUE,NOW()),

    ('CK030','Clienti, dati di società: la condizione di pagamento è obbligatoria e deve esistere in SAP',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','ZTERM',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),

    ('CK031','Fornitori, dati di società: modalità di pagamento obbligatoria ed esistente in SAP (doppione di CK008, disattivato)',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',FALSE,NOW()),

    ('CK032','Clienti, dati di società: modalità di pagamento obbligatoria ed esistente in SAP (doppione di CK010, disattivato)',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','ZWELS_01',
     'ref.SAP_EXPORT_T042Z (ZLSCH)','Error','SAP_REF',FALSE,NOW()),

    ('CK033','Materiali, dati di vendita: la gerarchia prodotto è obbligatoria e deve esistere in SAP',
     'MAT','S_MVKE','PRDHA',
     'ref.SAP_EXPORT_PRDHA (PRDHA)','Error','SAP_REF',TRUE,NOW()),

    ('CK034','Codici doganali: il codice di nomenclatura doganale è obbligatorio e deve esistere nell''elenco dei codici UE',
     'MAT','S_MARITC','COMCO(k/*)',
     'ref.SAP_CommodityCodesEU01 (codice)','Error','SAP_REF',TRUE,NOW()),

    ('CK041','Fornitori, correzioni manuali (STG): il paese corretto deve esistere tra i paesi configurati in SAP',
     'BP','S_SUPPL_GEN#ZBP/ZDM_DatiGenerali_STG','COUNTRY',
     'ref.SAP_EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),

    ('CK042','Fornitori, correzioni manuali (STG): la provincia/regione corretta deve esistere in SAP per il paese indicato',
     'BP','S_SUPPL_GEN#ZBP/ZDM_DatiGenerali_STG','COUNTRY + REGION',
     'ref.SAP_EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),
     
    ('CK043','Clienti, correzioni manuali (STG): il paese corretto deve esistere tra i paesi configurati in SAP',
     'BP','S_CUST_GEN#ZBP_DatiGenerali_STG','COUNTRY(*)',
     'ref.SAP_EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),

    ('CK044','Clienti, correzioni manuali (STG): la provincia/regione corretta deve esistere in SAP per il paese indicato',
     'BP','S_CUST_GEN#ZBP_DatiGenerali_STG','COUNTRY(*) + REGION',
     'ref.SAP_EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),
     
    ('CK045','Destinatari merci, correzioni manuali (STG): il paese corretto deve esistere tra i paesi configurati in SAP',
     'BP','S_CUST_GEN#ZDM_DatiGenerali_STG','COUNTRY(*)',
     'ref.SAP_EXPORT_T005S (LAND1)','Error','SAP_REF',TRUE,NOW()),

    ('CK048','Coordinate bancarie dei clienti: per le banche italiane, ABI e CAB devono corrispondere a una banca censita in SAP',
     'BP','S_CUST_BANK_DATA#ZBP-AppoggioBanca','BANKL(k)',
     'ref.SAP_EXPORT_BNKA_IT (BANKS+BANKL)','Error','SAP_REF',TRUE,NOW()),

    ('CK047','Coordinate bancarie dei fornitori (anche 05a): per le banche italiane, ABI e CAB devono corrispondere a una banca censita in SAP',
     'BP','S_SUPP_BANK#ZBP_AppoggioBanca / AddAppoggioBanca','BANKL(k)',
     'ref.SAP_EXPORT_BNKA_IT (BANKS+BANKL)','Error','SAP_REF',TRUE,NOW()),

    ('CK049','Fornitori: il tipo e il codice di ritenuta d''acconto devono esistere in SAP',
     'BP','S_SUPPL_WITH_TAX#ZBP_RitenAcco','WITHT(k/*) + WT_WITHCD',
     'ref.SAP_EXPORT_T059Z (WITHT+WT_WITHCD)','Error','SAP_REF',TRUE,NOW()),

    ('CK050','Fornitori estesi da clienti (05a): il tipo e il codice di ritenuta d''acconto devono esistere in SAP',
     'BP','S_SUPPL_WITH_TAX#ZBP_AddRitenAcco','WITHT(k/*) + WT_WITHCD',
     'ref.SAP_EXPORT_T059Z (WITHT+WT_WITHCD)','Error','SAP_REF',TRUE,NOW()),

    ('CK051','Fornitori: il settore industriale deve esistere in SAP per il sistema di classificazione indicato',
     'BP','S_SUPPL_INDUSTRY#ZBP_SettoriIndust','ISTYPE(k/*) + IND_SECTOR(k/*)',
     'ref.SAP_EXPORT_TB038A (ISTYPE+IND_SECTOR)','Error','SAP_REF',TRUE,NOW()),

    ('CK052','Fornitori estesi da clienti (05a): il settore industriale deve esistere in SAP per il sistema di classificazione indicato',
     'BP','S_SUPPL_INDUSTRY#ZBP_AddSettoriIndust','ISTYPE(k/*) + IND_SECTOR(k/*)',
     'ref.SAP_EXPORT_TB038A (ISTYPE+IND_SECTOR)','Error','SAP_REF',TRUE,NOW()),

    ('CK046','Destinatari merci, correzioni manuali (STG): la provincia/regione corretta deve esistere in SAP per il paese indicato',
     'BP','S_CUST_GEN#ZDM_DatiGenerali_STG','COUNTRY(*) + REGION',
     'ref.SAP_EXPORT_T005S (LAND1+BLAND)','Error','SAP_REF',TRUE,NOW()),

    ('CK053','Interlocutori dei fornitori: l''indirizzo e-mail deve essere scritto correttamente',
     'BP','S_SUPPL_CONT#ZBP-AddInterlocutore','E_MAIL_B',
     NULL,'Error','FORMAT',TRUE,NOW()),

    ('CK054','Interlocutori dei clienti: l''indirizzo e-mail deve essere scritto correttamente',
     'BP','S_CUST_CONT#ZBP-AddInterlocutore','E_MAIL_B',
     NULL,'Error','FORMAT',TRUE,NOW()),

    ('CK057','Clienti, dati di vendita: il gruppo clienti 5 è obbligatorio e deve esistere in SAP (il valore ''???'' viene esportato vuoto)',
     'BP','S_CUST_SALES_DATA#ZBP-DatiVendite','KVGR5',
     'ref.SAP_EXPORT_TVV5 (KVGR5)','Warning','SAP_REF',TRUE,NOW()),

    ('CK058','Destinatari merci, dati di vendita: il gruppo clienti 5 è obbligatorio e deve esistere in SAP (il valore ''???'' viene esportato vuoto; escluso l''ufficio vendite SUP)',
     'BP','S_CUST_SALES_DATA#ZDM-DatiVendite','KVGR5',
     'ref.SAP_EXPORT_TVV5 (KVGR5)','Warning','SAP_REF',TRUE,NOW()),

    ('CK059','Clienti, dati di vendita: il gruppo clienti è obbligatorio e deve esistere in SAP (se manca viene esportato 04)',
     'BP','S_CUST_SALES_DATA#ZBP-DatiVendite','KDGRP',
     'ref.SAP_EXPORT_T151 (KDGRP)','Error','SAP_REF',TRUE,NOW()),

    ('CK060','Ordini di acquisto: il gruppo acquisti è obbligatorio e deve esistere in SAP',
     'PO','S_EKKO','EKGRP',
     'ref.SAP_EXPORT_T024 (EKGRP)','Error','SAP_REF',TRUE,NOW()),

    ('CK061','Ordini di acquisto: la condizione di pagamento è obbligatoria e deve esistere in SAP',
     'PO','S_EKKO','ZTERM',
     'ref.SAP_EXPORT_T052 (ZTERM)','Error','SAP_REF',TRUE,NOW()),

    ('CK062','Ordini di acquisto: il fornitore deve esistere in SAP e corrispondere al codice fornitore del vecchio ERP',
     'PO','S_EKKO','LIFNR(*) + VERKF',
     'ref.SAP_EXPORT_BUT000 (PARTNER, BU_SORT1)','Error','SAP_REF',TRUE,NOW()),

    ('CK063','Business Partner (clienti, destinatari merci, fornitori): la società partner intercompany, se indicata, deve esistere in SAP',
     'BP','S_CUST_GEN#ZBP/ZDM-DatiGenerali + S_SUPPL_GEN#ZBP/ZDM_DatiGenerali','VBUND',
     'ref.SAP_EXPORT_T880 (RCOMP)','Error','SAP_REF',TRUE,NOW()),

    ('CK055','Coordinate bancarie dei fornitori (anche 05a): per le banche estere, la banca deve essere riconoscibile in SAP dal codice banca, dall''IBAN o dallo SWIFT',
     'BP','S_SUPP_BANK#ZBP_AppoggioBanca / AddAppoggioBanca','BANKL(k) / IBAN(k) / BKREF (SWIFT)',
     'ref.SAP_EXPORT_BNKA_UE (BANKS+BANKL)','Error','SAP_REF',TRUE,NOW()),

    ('CK056','Coordinate bancarie dei clienti: per le banche estere, la banca deve essere riconoscibile in SAP dal codice banca, dall''IBAN o dallo SWIFT',
     'BP','S_CUST_BANK_DATA#ZBP-AppoggioBanca','BANKL(k) / IBAN(k) / BKREF (SWIFT)',
     'ref.SAP_EXPORT_BNKA_UE (BANKS+BANKL)','Error','SAP_REF',TRUE,NOW()),

    -- EXISTENCE — CK201, CK203 (is_active=FALSE come da DB locale; CK202/CK204 eliminati, vedi fondo file)
    ('CK201','Fornitori: la partita IVA è obbligatoria per i soggetti UE ed extra UE',
     'BP','S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc','TAXNUM(*)',
     NULL,'Error','EXISTENCE',FALSE,NOW()),

    ('CK203','Clienti: la partita IVA è obbligatoria per i soggetti UE ed extra UE',
     'BP','S_CUST_TAXNUMBERS#ZBP-CodiciFisc','TAXNUM(*)',
     NULL,'Error','EXISTENCE',FALSE,NOW()),

    ('CK205','Info record acquisti, condizioni: l''importo della condizione (prezzo) è obbligatorio',
     'PIR','S_COND#INFORCOND','KBETR_EXT',
     NULL,'Error','EXISTENCE',FALSE,NOW()),

    ('CK206','Info record acquisti, scaglioni: il prezzo di ogni scaglione è obbligatorio e diverso da zero',
     'PIR','S_SCALES#INFORSCALES','KBETR',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK207','Business Partner: la stessa identità fiscale (codice fiscale, partita IVA o identificativo estero) non deve comparire su fornitori e clienti diversi; ammesso il fornitore 05a con il proprio cliente',
     'BP','ZBP_CodiciFisc + ZBP_AddCodiciFisc (SUPPL) + ZBP-CodiciFisc (CUST)','IT0 / IT1 / IT2',
     NULL,'Warning','EXISTENCE',TRUE,NOW()),

    ('CK208','Clienti intercompany: la società partner è obbligatoria',
     'BP','S_CUST_GEN#ZBP-DatiGenerali','VBUND',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK209','Fornitori intercompany: la società partner è obbligatoria',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali','VBUND',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK210','Clienti intercompany: il conto di riconciliazione indicato non è ammesso per i clienti intercompany',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta','AKONT(*)',
     'S_CUST_GEN#ZBP-DatiGenerali (BU_GROUP(*))','Error','EXISTENCE',TRUE,NOW()),

    ('CK211','Fornitori intercompany: il conto di riconciliazione indicato non è ammesso per i fornitori intercompany',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta','AKONT(*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali (BU_GROUP)','Error','EXISTENCE',TRUE,NOW()),

    ('CK213','Coordinate bancarie dei clienti: se è indicata la banca, sono obbligatori anche numero di conto e IBAN',
     'BP','S_CUST_BANK_DATA#ZBP-AppoggioBanca','BANKN(k) + IBAN(k)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK214','Coordinate bancarie dei fornitori (anche 05a): se è indicata la banca, sono obbligatori anche numero di conto e IBAN',
     'BP','S_SUPP_BANK#ZBP_AppoggioBanca / AddAppoggioBanca','BANKN(k) + IBAN(k)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK215','Clienti: la ragione sociale (o nome e cognome) è obbligatoria',
     'BP','S_CUST_GEN#ZBP-DatiGenerali','NAMORG1 + NAMORG2 + NAME_FIRST + NAME_LAST',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK216','Fornitori: la ragione sociale (o nome e cognome) è obbligatoria',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali','NAME_FIRST + NAME_LAST + NAME_FIRST_P + NAME_LAST_P',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK217','Destinatari merci: la ragione sociale (o nome e cognome) è obbligatoria',
     'BP','S_CUST_GEN#ZDM-DatiGenerali','NAMORG1 + NAMORG2 + NAME_FIRST + NAME_LAST',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK218','Clienti, dati di vendita: Incoterm e luogo Incoterm sono obbligatori (se mancano vengono esportati EXW e ''.'')',
     'BP','S_CUST_SALES_DATA#ZBP-DatiVendite','INCO1 + INCO2',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK219','Destinatari merci, dati di vendita: Incoterm e luogo Incoterm sono obbligatori (se mancano vengono esportati EXW e ''.'')',
     'BP','S_CUST_SALES_DATA#ZDM-DatiVendite','INCO1 + INCO2',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK220','Clienti, dati fiscali: ogni riga deve avere l''identificativo fiscale',
     'BP','S_CUST_TAXNUMBERS#ZBP-CodiciFisc','TAXNUM(*)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK221','Business Partner, correzioni manuali (STG): la via non deve superare 60 caratteri e il complemento d''indirizzo 40',
     'BP','S_CUST_GEN#ZBP/ZDM_DatiGenerali_STG + S_SUPPL_GEN#ZBP/ZDM_DatiGenerali_STG','STREET + STR_SUPPL3',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK222','Ordini di acquisto: Incoterm e luogo Incoterm sono obbligatori',
     'PO','S_EKKO','INCO1 + INCO2_L',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK223','Ordini di acquisto, posizioni: quantità, prezzo e unità di prezzo sono obbligatori e maggiori di zero',
     'PO','S_EKPO','MENGE + NETPR + PEINH',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK224','Ordini di acquisto, schedulazioni: la data di consegna deve essere una data valida e non già passata',
     'PO','S_EKET','EINDT(*)',
     NULL,'Warning','EXISTENCE',TRUE,NOW()),

    ('CK225','Ordini di acquisto: la quantità di ogni posizione deve essere uguale alla somma delle sue schedulazioni',
     'PO','S_EKPO + S_EKET','MENGE / MENGE(*)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    ('CK212','Distinte base: la quantità di ogni componente deve essere diversa da zero',
     'BOM','S_BOM_ITEM','MENGE(*)',
     NULL,'Error','EXISTENCE',TRUE,NOW()),

    -- CROSS_TABLE — CK401-CK405 (CK401-CK404 is_active=FALSE come da DB locale)
    ('CK401','Vettori: ogni dato collegato deve riferirsi a un vettore presente nell''anagrafica del flusso',
     'BP','varie (tabelle secondarie 01-ZBP-Vettori)','LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali','Error','CROSS_TABLE',FALSE,NOW()),

    ('CK402','Fornitori (06a e 05a): ogni dato collegato (banche, società, partner, …) deve riferirsi a un fornitore presente nell''anagrafica del flusso',
     'BP','varie (tabelle secondarie 06a-ZBP-Fornitori e 05a-ZBP-FornitoriCli)','LIFNR(k/*)',
     'S_SUPPL_GEN#ZBP_DatiGenerali / S_SUPPL_GEN#AddDatiGenerali','Error','CROSS_TABLE',FALSE,NOW()),

    ('CK403','Destinatari merci: ogni dato collegato deve riferirsi a un destinatario presente nell''anagrafica del flusso',
     'BP','varie (tabelle secondarie 02-ZDM-Clienti)','KUNNR(k/*)',
     'S_CUST_GEN#ZDM-DatiGenerali','Error','CROSS_TABLE',FALSE,NOW()),

    ('CK404','Clienti: ogni dato collegato deve riferirsi a un cliente presente nell''anagrafica del flusso',
     'BP','varie (tabelle secondarie 03-ZBP-Clienti)','KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali','Error','CROSS_TABLE',FALSE,NOW()),

    ('CK405','Materiali: ogni dato collegato (divisioni, descrizioni, valorizzazione, …) deve riferirsi a un materiale presente nell''anagrafica materiali',
     'MAT','varie (tabelle secondarie archivio materiali)','PRODUCT(k/*)',
     'raw.S_MARA (PRODUCT(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK406','Distinte base: ogni posizione deve avere la testata della distinta (stesso materiale e divisione)',
     'BOM','S_BOM_ITEM','MATNR(k/*) + WERKS(k)',
     'raw.S_BOM_HEADER (MATNR(k/*)+WERKS(k))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK407','Info record acquisti: dati di acquisto, condizioni e scaglioni devono riferirsi a un info record presente nella testata',
     'PIR','S_EINE#INFORDATIACQ + S_SCALES#INFORSCALES + S_COND#INFORCOND','INFNR(k/*)',
     'raw.S_EINA#INFORMATFOR (INFNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK408','Cicli di lavoro: ogni dato collegato deve riferirsi a un ciclo presente tra le operazioni',
     'ROUTING','S_MAPL + S_GROUP + S_PLKO','PLNNR(k/*)',
     'raw.S_OPERATION (PLNNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK409','Fornitori: ogni fornitore deve avere i dati di società e quelli di organizzazione acquisti',
     'BP','S_SUPPL_COMPANY#ZBP_DatiSocieta + S_SUPPL_PURCHASING#ZBP_OrganAcq','LIFNR(k/*)',
     'raw.S_SUPPL_GEN#ZBP_DatiGenerali (LIFNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK410','Clienti: ogni cliente deve avere i dati di società e quelli di area vendite',
     'BP','S_CUST_COMPANY#ZBP-DatiSocieta + S_CUST_SALES_DATA#ZBP-DatiVendite','KUNNR(k/*)',
     'raw.S_CUST_GEN#ZBP-DatiGenerali (KUNNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK411','Destinatari merci: ogni destinatario deve avere il partner destinatario merci (WE)',
     'BP','S_CUST_SALES_PARTNER#ZDM-Partner','KUNNR(k/*) + PARVW(k/*)=WE',
     'raw.S_CUST_GEN#ZDM-DatiGenerali (KUNNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK412','Destinatari merci: ogni destinatario deve avere un ruolo Business Partner',
     'BP','S_ROLES#ZBP-RuoliDM','KUNNR(k/*) + BP_ROLE(k/*)',
     'raw.S_CUST_GEN#ZDM-DatiGenerali (KUNNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK413','Clienti: ogni cliente deve avere un ruolo Business Partner',
     'BP','S_ROLES#ZBP-RuoliClienti','KUNNR(k/*) + BP_ROLE(k/*)',
     'raw.S_CUST_GEN#ZBP-DatiGenerali (KUNNR(k/*))','Error','CROSS_TABLE',TRUE,NOW()),

    ('CK414','Clienti, partner: il destinatario merci (WE) deve essere indicato e già presente in SAP',
     'BP','S_CUST_SALES_PARTNER#ZBP-Partner','KUNN2(k) (PARVW=WE)',
     'ref.SAP_EXPORT_BUT000 (BU_SORT2)','Warning','CROSS_TABLE',TRUE,NOW()),

    ('CK415','Ordini di acquisto: testate, posizioni e schedulazioni devono essere complete e collegate tra loro',
     'PO','S_EKKO + S_EKPO + S_EKET','EBELN(k/*) + EBELP(k/*)',
     NULL,'Error','CROSS_TABLE',TRUE,NOW()),

    ('CK416','Clienti: società partner intercompany e conto di riconciliazione devono essere coerenti tra loro',
     'BP','S_CUST_GEN#ZBP-DatiGenerali + S_CUST_COMPANY#ZBP-DatiSocieta','VBUND + AKONT(*)',
     NULL,'Error','CROSS_TABLE',TRUE,NOW()),

    ('CK417','Fornitori: società partner intercompany e conto di riconciliazione devono essere coerenti tra loro',
     'BP','S_SUPPL_GEN#ZBP_DatiGenerali + S_SUPPL_COMPANY#ZBP_DatiSocieta/AddDatiSocieta','VBUND + AKONT(*)',
     NULL,'Error','CROSS_TABLE',TRUE,NOW()),
    ('CK418','Fornitori estesi da clienti (05a): l''IBAN del fornitore deve essere diverso da quelli già caricati con il cliente',
     'BP','S_SUPP_BANK#ZBP_AddAppoggioBanca','IBAN(k)',
     'S_CUST_BANK_DATA#ZBP-AppoggioBanca (prd, via REF_KUNNR)','Warning','CROSS_TABLE',TRUE,NOW()),
    ('CK419','Fornitori estesi da clienti (05a): l''identificativo fiscale del fornitore deve coincidere con quello del cliente (il file non viene esportato: vale quello del cliente)',
     'BP','S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc','TAXTYPE / TAXNUM',
     'S_CUST_TAXNUMBERS#ZBP-CodiciFisc (prd, via REF_KUNNR)','Warning','CROSS_TABLE',TRUE,NOW()),
    ('CK420','Fornitori (06a e 05a), partner: il fornitore partner, se diverso dal fornitore stesso, deve essere già presente in SAP',
     'BP','S_SUPPL_PARTNER#ZBP_Partner / AddPartner','LIFN2(k)',
     'SAP_EXPORT_BUT000 (BU_SORT1)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK421','Info record acquisti: ogni scaglione deve avere la condizione di prezzo a cui si riferisce, con la stessa chiave (altrimenti SAP lo rifiuta)',
     'PIR','S_SCALES#INFORSCALES','INFNR, EKORG, ESOKZ, WERKS, KOPOS, COND_TYPE, VALID_FROM, VALID_TO',
     'raw.S_COND#INFORCOND (stessa chiave)','Error','CROSS_TABLE',TRUE,NOW()),
    ('CK422','Partner dei clienti: ogni partner deve indicare a chi si riferisce (cliente, fornitore, persona o interlocutore)',
     'BP','S_CUST_SALES_PARTNER#ZBP-Partner','KUNN2(k) / LIFNR(k) / PERNR(k) / PARNR(k)',
     NULL,'Warning','EXISTENCE',TRUE,NOW()),
    ('CK423','Partner dei clienti con funzione RA o ZA: il fornitore indicato deve essere già presente in SAP',
     'BP','S_CUST_SALES_PARTNER#ZBP-Partner','KUNN2(k) (PARVW = RA / ZA)',
     'SAP_EXPORT_BUT000 (BU_SORT1)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK424','Fornitori estesi da clienti (05a): il cliente di riferimento deve essere già presente in SAP',
     'BP','S_SUPPL_GEN#AddDatiGenerali','REF_KUNNR',
     'SAP_EXPORT_BUT000 (BU_SORT2)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK425','Interlocutori, dati aggiuntivi e gestione credito dei clienti (03b, 07a): il cliente deve essere già presente in SAP',
     'BP','S_CUST_GEN#AddDatiGenerali / S_CUST_CONT#ZBP-AddInterlocutore / S_UKM_PROFILE / S_UKM_SEGMENT','KUNNR(k/*)',
     'SAP_EXPORT_BUT000 (BU_SORT2)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK426','Interlocutori e dati aggiuntivi dei clienti (03b): il codice deve essere un cliente e non solo un destinatario merci',
     'BP','S_CUST_GEN#AddDatiGenerali / S_CUST_CONT#ZBP-AddInterlocutore','KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali / S_CUST_GEN#ZDM-DatiGenerali','Warning','CROSS_TABLE',TRUE,NOW()),

    -- CROSS_SOURCE — CK501-CK506
    ('CK501','Materiali prodotti internamente o misti: deve esistere la distinta base (esclusi i materiali in stato 01 e 08)',
     'MAT','S_MARC','BESKZ',
     'S_BOM_HEADER (MATNR)','Warning','CROSS_SOURCE',TRUE,NOW()),

    ('CK502','Materiali prodotti internamente o misti: deve esistere il ciclo di lavoro standard (esclusi i materiali in stato 01 e 08)',
     'MAT','S_MARC','BESKZ',
     'S_MAPL (MATNR + WERKS_MAT + PLNAL)','Warning','CROSS_SOURCE',TRUE,NOW()),

    ('CK503','Materiali acquistati: deve esistere l''info record di acquisto',
     'MAT','S_MARC','BESKZ + SOBSL',
     'S_EINA#INFORMATFOR (MATNR)','Warning','CROSS_SOURCE',TRUE,NOW()),

    ('CK504','Materiali, dati di divisione: il materiale deve essere presente in A2F per la stessa divisione (IT11 in A2F_BO, IT12 in A2F_FA)',
     'MAT','S_MARC','PRODUCT(k/*) + WERKS(k/*)',
     'A2F_BO / A2F_FA (CODART)','Warning','CROSS_SOURCE',TRUE,NOW()),

    ('CK505','Distinte base: ogni componente deve essere esteso alla divisione della distinta nell''anagrafica materiali',
     'BOM','S_BOM_ITEM','IDNRK + WERKS(k)',
     'raw.S_MARC (PRODUCT(k/*) + WERKS(k/*))','Warning','CROSS_SOURCE',TRUE,NOW()),

    ('CK506','Codici doganali: il materiale deve essere esteso alla divisione indicata nell''anagrafica materiali dello stesso caricamento',
     'MAT','S_MARITC','MATNR(k/*) + PLANT(k/*)',
     'raw.S_MARC (PRODUCT(k/*) + WERKS(k/*))','Warning','CROSS_SOURCE',TRUE,NOW()),

    ('CK507','Sconti di testata cliente (ZSPT, file BO): il cliente deve essere presente in anagrafica clienti o destinatari merci',
     'PRICE','BO-ZSPT-ScontoTestataCliente','CUSTOMER',
     'S_CUST_GEN#ZBP-DatiGenerali','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK508','Sconti di testata cliente (ZSPT, file FA): il cliente deve essere presente in anagrafica clienti o destinatari merci',
     'PRICE','FA-ZSPT-ScontoTestataCliente','CUSTOMER',
     'S_CUST_GEN#ZBP-DatiGenerali','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK509','Prezzi piani di consegna (ZPPC, file BO): il cliente deve essere in anagrafica e il materiale deve avere i dati di vendita',
     'PRICE','BO-ZPPC-PrezziPianiConsegna','CUSTOMER-MATERIAL',
     'S_CUST_GEN#ZBP-DatiGenerali + S_MVKE','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK510','Prezzi piani di consegna (ZPPC, file FA): il cliente deve essere in anagrafica e il materiale deve avere i dati di vendita',
     'PRICE','FA-ZPPC-PrezziPianiConsegna','CUSTOMER-MATERIAL',
     'S_CUST_GEN#ZBP-DatiGenerali + S_MVKE','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK511','Codici doganali: il materiale deve essere presente nell''anagrafica materiali dello stesso caricamento',
     'MAT','S_MARITC','MATNR(k/*)',
     'raw.S_MARA (PRODUCT(k/*))','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK512','Info record acquisti: il materiale deve essere nell''anagrafica materiali e il fornitore nell''anagrafica fornitori',
     'PIR','S_EINA#INFORMATFOR','MATNR + LIFNR(*)',
     'raw.S_MARA (PRODUCT(k/*)) + raw.S_SUPPL_GEN#ZBP_DatiGenerali / AddDatiGenerali','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK513','Cicli di lavoro: il materiale deve essere esteso alla divisione del ciclo nell''anagrafica materiali',
     'ROUTING','S_MAPL','MATNR(k/*) + WERKS_MAT(k/*)',
     'raw.S_MARC (PRODUCT(k/*)+WERKS(k/*))','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK514','Cicli di lavoro: il materiale del ciclo deve avere una distinta base nella stessa divisione',
     'ROUTING','S_MAPL','MATNR(k/*) + WERKS_MAT(k/*)',
     'raw.S_BOM_HEADER (MATNR(k/*)+WERKS(k))','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK515', 'Info record acquisti: il materiale deve avere il gruppo acquisti nei dati di divisione',
     'MAT', 'S_EINA#INFORMATFOR', 'MATNR',
     'raw.S_MARC (PRODUCT(k/*))', 'Error', 'CROSS_SOURCE',TRUE,NOW()),

    ('CK516','Info record acquisti, dati di acquisto: il tempo di consegna può mancare solo se è indicato nell''anagrafica materiali',
     'PIR','S_EINE#INFORDATIACQ','APLFZ',
     'raw.S_MARC (PLIFZ) via S_EINA#INFORMATFOR (MATNR)','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK517','Info record acquisti: lo stesso materiale non deve comparire in più info record',
     'PIR','S_EINA#INFORMATFOR','MATNR',
     NULL,'Error','DUPLICATE',TRUE,NOW()),

    ('CK518','Interlocutori e punti di scarico dei clienti: il cliente deve essere presente in anagrafica clienti o destinatari merci',
     'BP','S_CUST_GEN#AddDatiGenerali + S_CUST_CONT#ZBP-AddInterlocutore + S_UNL_POINT#ZBP-AddPuntoScarico','KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali / ZDM-DatiGenerali','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK519','Gestione credito dei clienti (rischio e fido): il cliente deve essere presente in anagrafica clienti o destinatari merci',
     'BP','S_UKM_PROFILE#ZBP-Rischio + S_UKM_SEGMENT#ZBP-Fido','KUNNR(k/*)',
     'S_CUST_GEN#ZBP-DatiGenerali / ZDM-DatiGenerali','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK520','Ordini di acquisto, posizioni: il materiale deve essere nell''anagrafica materiali ed esteso alla divisione dell''ordine',
     'PO','S_EKPO','MATNR + WERKS',
     'S_MARA (PRODUCT) / S_MARC (PRODUCT+WERKS)','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK521','Ordini di acquisto, posizioni: l''unità di misura deve essere l''unità base del materiale o una sua unità alternativa',
     'PO','S_EKPO','MEINS',
     'S_MARA (MEINS) / S_MARM (MEINH)','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK522','Ordini di acquisto, posizioni: il materiale non deve essere obsoleto (stato 07) o incompleto (stato 08)',
     'PO','S_EKPO','MATNR',
     'S_MARA (MSTAE)','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK523','Ordini di acquisto, posizioni: il materiale deve avere la descrizione nella lingua del fornitore, se diversa dall''italiano',
     'PO','S_EKPO + S_EKKO','MATNR + VERKF',
     'S_SUPPL_GEN#ZBP_DatiGenerali (LANGU_CORR) / S_MAKT (SPRAS, MAKTX)','Error','CROSS_SOURCE',TRUE,NOW()),

    ('CK524','Ordini di acquisto: il fornitore deve essere presente nell''anagrafica fornitori (anche fornitori estesi da clienti 05a)',
     'PO','S_EKKO','VERKF',
     'S_SUPPL_GEN#ZBP_DatiGenerali / S_SUPPL_GEN#AddDatiGenerali (LIFNR)','Error','CROSS_SOURCE',TRUE,NOW()),
    ('CK525','Codici doganali: il materiale deve essere già migrato in SAP nella divisione indicata',
     'MAT','S_MARITC','MATNR(k/*) + PLANT(k/*)',
     'SAP_EXPORT_MARC (MATNR + WERKS)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK526','Info record acquisti: il fornitore deve essere già presente in SAP',
     'PIR','S_EINA#INFORMATFOR','LIFNR(*)',
     'SAP_EXPORT_BUT000 (BU_SORT1 / REF_KUNNR e BU_SORT2)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK527','Distinte base: il materiale padre e ogni componente devono essere già migrati in SAP nella divisione della distinta',
     'BOM','S_BOM_HEADER / S_BOM_ITEM','MATNR(k/*) / IDNRK + WERKS(k)',
     'SAP_EXPORT_MARC (MATNR + WERKS)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK528','Info record acquisti: il materiale deve essere già migrato in SAP',
     'PIR','S_EINA#INFORMATFOR','MATNR',
     'SAP_EXPORT_MARC (MATNR)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK529','Ordini di acquisto, posizioni: il materiale deve essere già migrato in SAP nella divisione dell''ordine',
     'PO','S_EKPO','MATNR + WERKS',
     'SAP_EXPORT_MARC (MATNR + WERKS)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK530','Cicli di lavoro: il materiale deve essere già migrato in SAP nella divisione del ciclo',
     'ROUTING','S_MAPL','MATNR(k/*) + WERKS_MAT(k/*)',
     'SAP_EXPORT_MARC (MATNR + WERKS)','Warning','CROSS_SOURCE',TRUE,NOW()),
    ('CK531','Info record cliente-materiale: il materiale deve essere già migrato in SAP',
     'CMIR','S_CUSTMAT#INFORMATFOR','MATERIAL(k/*) (+ PLANT)',
     'SAP_EXPORT_MARC (MATNR + WERKS)','Warning','CROSS_SOURCE',TRUE,NOW()),

    -- EXT_REF — CK801-CK804 (mancanti nella versione precedente)
    ('CK801','Clienti: la partita IVA UE o britannica deve risultare valida (verifica online VIES / HMRC)',
     'BP','S_CUST_TAXNUMBERS#ZBP-CodiciFisc','TAXNUM(*)',
     'VIES (ec.europa.eu) | HMRC API v2 (api.service.hmrc.gov.uk)','Error','EXT_REF',FALSE,NOW()),

    ('CK802','Clienti: riepilogo della verifica online delle partite IVA (CK801)',
     'BP','stg.check_vat_vies','check_status',
     'stg.check_vat_vies (output CK801)','Error','EXT_REF',FALSE,NOW()),

    ('CK803','Fornitori: la partita IVA UE o britannica deve risultare valida (verifica online VIES / HMRC)',
     'BP','S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc','TAXNUM(*)',
     'VIES (ec.europa.eu) | HMRC API v2 (api.service.hmrc.gov.uk)','Error','EXT_REF',TRUE,NOW()),

    ('CK804','Fornitori: riepilogo della verifica online delle partite IVA (CK803)',
     'BP','stg.check_vat_vies','check_status',
     'stg.check_vat_vies (output CK803)','Error','EXT_REF',TRUE,NOW())

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

-- Check eliminati: rimossi anche dai cataloghi già esistenti (es. VPS),
-- dove l'INSERT ... ON CONFLICT da solo non li toglierebbe.
--   CK202, CK204: duplicati codice fiscale solo fornitori / solo clienti,
--                 sostituiti da CK207 (identità fiscale fornitori + clienti).
--   CK029: condizione di pagamento fornitori (ZTERM1 in T052), doppione di CK006.
DELETE FROM stg.check_catalog WHERE check_id IN ('CK202', 'CK204', 'CK029');

-- Check con esclusione dall'output implementata e valore di default del flag (riscritti a
-- ogni run, poi setup.apply_check_states applica le scelte dell'utente). Per i nuovi check
-- il default è disattivato; quelli qui sotto escludevano già i record prima del flag.
--   CK034/CK506/CK511/CK525  codici doganali S_MARITC        (prd.export_maritc_zip)
--   CK051/CK052/CK402/CK418/CK420  fornitori               (prd.export_suppliers_zip)
--   CK047/CK055/CK214/CK804  banche e P.IVA fornitori       (prd.merge_banche_fornitori, merge P.IVA)
--   CK048/CK056/CK213/CK802  banche e P.IVA clienti / DM    (prd.merge_banche_clienti, merge P.IVA)
--   CK220/CK404              clienti                        (prd.export_clienti_zip)
--   CK403                    destinatari merci              (prd.export_dest_merci_zip)
--   CK526                    info record acquisti           (prd.export_inforecord_zip)
--   CK518/CK519/CK425/CK426      interlocutori e credito clienti (prd.export_clienti_add_zip)
--   CK422/CK423              partner clienti                (prd.export_clienti_zip)
--   CK421                    scaglioni info record          (prd.export_inforecord_zip)
--   CK424                    fornitori 05a                  (prd.export_suppliers_zip)
--   CK527 distinte base (prd.export_bom_zip) e CK528 info record (prd.export_inforecord_zip):
--   supportati, default disattivato.
UPDATE stg.check_catalog
SET exclude_supported = check_id IN (
        'CK034', 'CK506', 'CK511', 'CK525',
        'CK051', 'CK052', 'CK402', 'CK418', 'CK420',
        'CK047', 'CK055', 'CK214', 'CK804',
        'CK048', 'CK056', 'CK213', 'CK802',
        'CK220', 'CK404', 'CK403', 'CK526',
        'CK518', 'CK519', 'CK422', 'CK423', 'CK424', 'CK425', 'CK426', 'CK421',
        'CK527', 'CK528'),
    exclude_output = check_id IN (
        'CK034', 'CK506', 'CK511', 'CK525',
        'CK051', 'CK052', 'CK402', 'CK418', 'CK420',
        'CK047', 'CK055', 'CK214', 'CK804',
        'CK048', 'CK056', 'CK213', 'CK802',
        'CK220', 'CK404', 'CK403', 'CK526',
        'CK518', 'CK519', 'CK422', 'CK423', 'CK424', 'CK425', 'CK426', 'CK421');
