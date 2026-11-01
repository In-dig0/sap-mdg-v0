""" @bruin
name: prd.export_suppliers_zip
type: python
description: >
  Genera gli archivi ZIP di output per i fornitori combinando:
    - prd."S_SUPPL_GEN#ZBP_DatiGenerali"  (dati anagrafici con indirizzi normalizzati)
    - tutte le altre tabelle fornitori da schema raw
  e, per i fornitori destinatari merci (flusso DM06-02b):
    - prd."S_SUPPL_GEN#ZDM_DatiGenerali" + raw."S_ROLES#ZDM_RuoliFornitori"
  e, per i clienti già in SAP estesi al ruolo fornitore (flusso DM06-05a-ZBP-FornitoriCli):
    - le tabelle raw ...Add... del flusso, con master raw."S_SUPPL_GEN#AddDatiGenerali";
      i codici fiscali (S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc) non sono esportati: sono già
      sul BP, caricati con il flusso clienti (CK419 segnala quelli diversi dal cliente);
      le coordinate bancarie da prd."S_SUPP_BANK#ZBP_AddAppoggioBanca" (stesse regole
      CK047/CK055/CK214 delle banche dei fornitori ZBP).
      REF_KUNNR e KUNNR sono trascodificati con il codice SAP del cliente (PARTNER di
      ref.SAP_EXPORT_BUT000, tramite BU_SORT2); LIFNR resta il codice del vecchio ERP.
      I fornitori il cui cliente non è in BUT000 sono esclusi da tutte le tabelle e
      scritti in <tabella>_discarded.csv. Negli interlocutori (S_SUPPL_CONT#ZBP-AddInterlocutore)
      email in minuscolo e nome, paese, regione (e CAP per BR) dall'anagrafica del cliente
      di riferimento (prd."S_CUST_GEN#ZBP_DatiGenerali", come negli interlocutori clienti).
  Un ZIP separato viene creato per ogni valore distinto di _source
  (es. "DM06-01a-ZBP-Vettori.zip", "DM06-02b-ZDM-Fornitori.zip").
  I CSV dentro il ZIP usano separatore ";" e non includono colonne audit.
  Le banche fornitori (tabelle S_SUPP_BANK#..., nome dato dall'ERP) sono scritte con il nome
  della struttura SAP S_SUPPL_BANK#... (CSV_NAME), mappato in automatico nel caricamento.
  Le righe escluse dalle regole in ROW_RULES sono scritte in <tabella>_discarded.csv; se non
  ne resta nessuna il CSV non è scritto:
    - CK051/CK052: settore industriale non in TB038A;
    - CK420: partner con fornitore LIFN2 (diverso da LIFNR) non presente in SAP (BUT000, BU_SORT1);
      LIFN2 è già il codice del fornitore in SAP ed è esportato senza trascodifica;
    - CK418 (flusso 05a): coordinata bancaria (IBAN) uguale a quella del cliente di
      riferimento, già sul BP in SAP.
  Negli interlocutori (S_SUPPL_CONT#ZBP-Interlocutore) le email E_MAIL_B/E_MAIL sono in
  minuscolo; VNAME/LNAME, COUNTRY e REGION vengono dall'anagrafica
  prd."S_SUPPL_GEN#ZBP_DatiGenerali" (corretta in STG): NAME_FIRST_P/NAME_LAST_P per
  le persone fisiche (NATPERS = X), LNAME = NAME_FIRST (VNAME vuoto) per le società.
  Estrazione ristretta (test) del flusso 05a: se stg.export_filter contiene codici, viene
  generato anche lo ZIP dei soli fornitori il cui cliente di riferimento (REF_KUNNR) o il cui
  LIFNR è nell'elenco, in out_source_mdg/test.
  Fornitori 05a già caricati in SAP: il BP è in BUT000 come cliente, quindi l'esclusione
  automatica non li riconosce; sono esclusi da tutte le tabelle se il cliente di riferimento
  (REF_KUNNR) o il LIFNR è in stg.export_exclude_fornitori_cli (elenco manuale), con
  riepilogo in <nome ZIP>_gia_in_SAP.csv.
  I fornitori/vettori già presenti in SAP (ref.SAP_EXPORT_BUT000, BU_SORT1) sono esclusi
  da tutte le tabelle: riepilogo in <nome ZIP>_gia_in_SAP.csv; se non resta nulla lo ZIP
  non è generato.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - ingestion.ingest_xlsx_to_ref
  - setup.fn_fornitori_cli
  - prd.merge_banche_clienti
  - prd.merge_banche_fornitori
  - prd.merge_fornitori
  - prd.merge_fornitori_dm
  - prd.merge_taxnumbers_fornitori
  - prd.merge_add_taxnumbers_fornitori
  - prd.merge_clienti
  - prd.merge_dest_merci
@bruin """

import os
import io
import csv
import zipfile
import logging
import math
import sys
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from prd_export_clienti_zip import (
    get_but000_map, already_in_sap_rows, write_already_in_sap, write_output_zip,
    write_discarded_rows, get_export_filter, write_test_zip,
)
from prd_export_clienti_add_zip import get_customer_data, contact_row_fn as customer_contact_row_fn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

OUTPUT_DIR = "/project/datalake/out_source_mdg"

# Nome del CSV nello ZIP per le tabelle il cui nome differisce dalla struttura SAP: l'ERP
# chiama le banche fornitori S_SUPP_BANK#..., la struttura SAP è S_SUPPL_BANK (con il nome
# corretto il file è mappato in automatico nel caricamento in SAP)
CSV_NAME = {
    "S_SUPP_BANK#ZBP_AppoggioBanca":    "S_SUPPL_BANK#ZBP_AppoggioBanca",
    "S_SUPP_BANK#ZBP_AddAppoggioBanca": "S_SUPPL_BANK#ZBP_AddAppoggioBanca",
}

# Colonne audit da escludere dai CSV di output
AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

# Tabelle da schema prd che sostituiscono le rispettive versioni raw.
# L'ordine determina la sequenza di scrittura nel ZIP.
PRD_TABLES = [
    ("prd", "S_SUPPL_GEN#ZBP_DatiGenerali"),
    ("prd", "S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"),
    ("prd", "S_SUPP_BANK#ZBP_AppoggioBanca"),
]

# Tabelle da raw da includere nel ZIP (filtrate per _source)
RAW_TABLES = [
    "S_ROLES#ZBP_RuoliFornitori",
    "S_SUPPL_COMPANY#ZBP_DatiSocieta",
    "S_SUPPL_CONT#ZBP-Interlocutore",
    "S_SUPPL_GEN#ZBP_DatiGenerali",           # sostituita da prd
    "S_SUPPL_INDUSTRY#ZBP_SettoriIndust",
    "S_SUPPL_PARTNER#ZBP_Partner",
    "S_SUPPL_PURCHASING#ZBP_OrganAcq",
    "S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc",       # sostituita da prd
    "S_SUPPL_IDENT#ZBP-NumIdent",
    "S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc",
    "S_SUPPL_WITH_TAX#ZBP_RitenAcco",
    "S_SUPP_BANK#ZBP_AppoggioBanca",           # sostituita da prd (filtro CK047)
]

# Fornitori destinatari merci (DM06-02b): anagrafica da prd, ruoli da raw
DM_PRD_TABLES = [
    ("prd", "S_SUPPL_GEN#ZDM_DatiGenerali"),
]
DM_RAW_TABLES = [
    "S_ROLES#ZDM_RuoliFornitori",
    "S_SUPPL_GEN#ZDM_DatiGenerali",            # sostituita da prd
]

# Clienti già in SAP estesi al ruolo fornitore (DM06-05a): solo tabelle raw, master Add
CLI_MASTER     = "S_SUPPL_GEN#AddDatiGenerali"
CLI_CONTACT    = "S_SUPPL_CONT#ZBP-AddInterlocutore"
CLI_REF_COLS   = ["REF_KUNNR", "KUNNR"]          # codice cliente, trascodificato in SAP
CLI_RAW_TABLES = [
    CLI_MASTER,
    "S_ROLES#ZBP_AddRuoliFornitori",
    "S_SUPPL_COMPANY#ZBP_AddDatiSocieta",
    "S_SUPPL_PURCHASING#ZBP_AddOrganAcq",
    "S_SUPPL_PARTNER#ZBP_AddPartner",
    "S_SUPP_BANK#ZBP_AddAppoggioBanca",
    "S_SUPPL_INDUSTRY#ZBP_AddSettoriIndust",
    CLI_CONTACT,
]   # S_SUPPL_TAXNUMBERS#ZBP_AddCodiciFisc non esportata: codici fiscali già caricati con il cliente

LIFN2_IN_SAP = ('COALESCE(TRIM("LIFN2(k)"), \'\') IN (\'\', TRIM("LIFNR(k/*)"))'
                ' OR EXISTS (SELECT 1 FROM ref."SAP_EXPORT_BUT000" b WHERE TRIM(b."BU_SORT1") = TRIM("LIFN2(k)"))')

# Righe escluse dallo ZIP (e scritte in <tabella>_discarded.csv) quando il check è attivo:
# tabella -> (check_id, tabella necessaria come "schema.tabella" o None, condizione SQL delle righe valide)
ROW_RULES = {
    # CK051: coppia ISTYPE + IND_SECTOR presente in TB038A
    "S_SUPPL_INDUSTRY#ZBP_SettoriIndust": (
        "CK051", "ref.SAP_EXPORT_TB038A",
        'EXISTS (SELECT 1 FROM ref."SAP_EXPORT_TB038A" t'
        ' WHERE t."ISTYPE" = "ISTYPE(k/*)" AND t."IND_SECTOR" = "IND_SECTOR(k/*)")',
    ),
    # CK052: stessa regola per i settori del flusso 05a
    "S_SUPPL_INDUSTRY#ZBP_AddSettoriIndust": (
        "CK052", "ref.SAP_EXPORT_TB038A",
        'EXISTS (SELECT 1 FROM ref."SAP_EXPORT_TB038A" t'
        ' WHERE t."ISTYPE" = "ISTYPE(k/*)" AND t."IND_SECTOR" = "IND_SECTOR(k/*)")',
    ),
    # CK420: fornitore partner LIFN2 (diverso da LIFNR) già in SAP; il codice è quello del
    # fornitore in SAP (es. indirizzi di ordinazione ZDM del flusso 02b), esportato invariato
    "S_SUPPL_PARTNER#ZBP_Partner":    ("CK420", "ref.SAP_EXPORT_BUT000", LIFN2_IN_SAP),
    "S_SUPPL_PARTNER#ZBP_AddPartner": ("CK420", "ref.SAP_EXPORT_BUT000", LIFN2_IN_SAP),
    # CK418 (flusso 05a): IBAN uguale a quello del cliente di riferimento, già in SAP
    "S_SUPP_BANK#ZBP_AddAppoggioBanca": (
        "CK418", None,
        'NOT stg.fn_iban_uguale_cliente("LIFNR(k/*)", "IBAN(k)")',
    ),
}

# Interlocutori fornitori: nome, paese e regione dall'anagrafica fornitori in prd
CONTACT_TABLE  = "S_SUPPL_CONT#ZBP-Interlocutore"
PRD_SUPPLIER   = "S_SUPPL_GEN#ZBP_DatiGenerali"
EMAIL_COLS     = ["E_MAIL_B", "E_MAIL"]

# Nomi tabella raw sostituiti da prd (vengono skippati nella sezione raw)
PRD_TABLE_NAMES = {table for _, table in PRD_TABLES + DM_PRD_TABLES}

# Flussi esportati. sap_sort_cols: colonne di BUT000 con il vecchio codice dei BP già in
# SAP (esclusi dallo ZIP); orphan_check: check degli orfani nelle tabelle secondarie;
# customer_ref: flusso dei clienti estesi a fornitori (trascodifica di REF_KUNNR/KUNNR);
# from_prd: tabelle del flusso lette dal merge in prd invece che da raw.
FLOWS = [
    {"prd_tables": PRD_TABLES, "raw_tables": RAW_TABLES,
     "master": "S_SUPPL_GEN#ZBP_DatiGenerali", "orphan_check": "CK402",
     "sap_sort_cols": ("BU_SORT1",)},
    {"prd_tables": DM_PRD_TABLES, "raw_tables": DM_RAW_TABLES,
     "master": "S_SUPPL_GEN#ZDM_DatiGenerali", "orphan_check": None,
     "sap_sort_cols": ("BU_SORT1", "BU_SORT2")},
    {"prd_tables": [], "raw_tables": CLI_RAW_TABLES,
     "master": CLI_MASTER, "orphan_check": "CK402",
     "sap_sort_cols": ("BU_SORT1",), "customer_ref": True,
     "from_prd": {"S_SUPP_BANK#ZBP_AddAppoggioBanca"}},   # filtri CK047/CK055/CK214
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def write_discarded_csv(conn, table: str, source_name: str,
                        cols: list, orphan_keys: set, fk_col: str):
    """
    Scrive i record scartati (orfani) in un file CSV nella cartella OUTPUT_DIR.
    Nome file: <table>_discarded.csv  (es. S_ROLES#ZBP_RuoliFornitori_discarded.csv)
    Se non ci sono orfani o la colonna chiave non è presente, non scrive nulla.
    """
    if not orphan_keys or fk_col not in cols:
        return
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    placeholders = ",".join(["%s"] * len(orphan_keys))
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt}'
            f' WHERE "_source" = %s'
            f' AND {q(fk_col)} IN ({placeholders})'
            f' ORDER BY 1',
            (source_name, *orphan_keys)
        )
        rows = cur.fetchall()
    if not rows:
        return
    def sanitize(val):
        if val is None: return ""
        if isinstance(val, float) and math.isnan(val): return ""
        if isinstance(val, str) and val.strip() == "NaN": return ""
        return val
    # In append: più ZIP (es. 01a e 06a) e più regole possono scartare righe della stessa tabella
    write_discarded_rows(table, cols, [tuple(sanitize(v) for v in row) for row in rows])


def remove_discarded_csv(tables: list[str]):
    """
    Cancella i file <table>_discarded.csv delle tabelle gestite da questo export:
    vanno eliminati a ogni run, altrimenti resterebbero gli scarti di run precedenti
    (write_discarded_csv scrive il file solo se ci sono scarti).
    """
    for table in tables:
        safe_table = table.replace("#", "_").replace("/", "_")
        out_path   = os.path.join(OUTPUT_DIR, f"{safe_table}_discarded.csv")
        if os.path.exists(out_path):
            os.remove(out_path)
            log.info("  [DISCARDED] rimosso file di scarto precedente: %s", out_path)


def get_distinct_sources(conn, flow: dict) -> list[str]:
    """Valori distinti di _source della tabella principale del flusso (prd, o la master raw)."""
    schema, table = flow["prd_tables"][0] if flow["prd_tables"] else ("raw", flow["master"])
    fqt = f'{schema}.{q(table)}'
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (fqt,))
        if cur.fetchone()[0] is None:
            return []
        cur.execute(f'SELECT DISTINCT "_source" FROM {fqt} WHERE "_source" IS NOT NULL ORDER BY 1')
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> list[str]:
    """Recupera le colonne della tabella escludendo le audit."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def get_orphan_keys(conn, check_id: str) -> set:
    """
    Recupera le chiavi (LIFNR o KUNNR) orfane dall'ultimo run del check_id
    specificato in stg.check_results (status != 'Ok').
    Ritorna un set vuoto se il check non è attivo o non ha risultati.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(is_active, FALSE)
            FROM stg.check_catalog WHERE check_id = %s
        """, (check_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return set()
        cur.execute("""
            SELECT DISTINCT object_key
            FROM stg.check_results
            WHERE check_id = %s
              AND status  != 'Ok'
              AND run_id  = (
                  SELECT MAX(run_id) FROM stg.check_results
                  WHERE check_id = %s
              )
        """, (check_id, check_id))
        return {r[0] for r in cur.fetchall()}


def get_supplier_data(conn) -> dict:
    """Codice fornitore -> (VNAME, LNAME, COUNTRY, REGION) degli interlocutori, dall'anagrafica in prd."""
    if not get_columns(conn, "prd", PRD_SUPPLIER):
        log.warning(f"  prd.{PRD_SUPPLIER} non trovata — interlocutori senza dati anagrafici")
        return {}
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT TRIM("LIFNR(k/*)"), COALESCE(TRIM("NATPERS"), \'\'), '
            f'COALESCE(TRIM("NAME_FIRST"), \'\'), '
            f'COALESCE(TRIM("NAME_FIRST_P"), \'\'), COALESCE(TRIM("NAME_LAST_P"), \'\'), '
            f'COALESCE(TRIM("COUNTRY"), \'\'), COALESCE(TRIM("REGION"), \'\') '
            f'FROM prd.{q(PRD_SUPPLIER)}'
        )
        data = {}
        for key, natpers, org1, first_p, last_p, country, region in cur.fetchall():
            if natpers == "X" or first_p or last_p:    # persona fisica
                data[key] = (first_p, last_p, country, region)
            else:                                      # società
                data[key] = ("", org1, country, region)
        return data


def contact_row_fn(suppliers: dict):
    """Interlocutori: email in minuscolo; VNAME/LNAME, COUNTRY e REGION dall'anagrafica fornitore."""
    def fn(cols: list, row: list):
        for col in EMAIL_COLS:
            if col in cols and row[cols.index(col)]:
                row[cols.index(col)] = row[cols.index(col)].strip().lower()
        data = suppliers.get((row[cols.index("LIFNR(k/*)")] or "").strip())
        if not data:
            return
        for col, val in zip(("VNAME", "LNAME", "COUNTRY", "REGION"), data):
            if col in cols:
                row[cols.index(col)] = val
    return fn


def get_customer_ref_map(conn, source_name: str) -> dict:
    """Flusso 05a: LIFNR -> codice del cliente di riferimento (REF_KUNNR) dalla master raw."""
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT "LIFNR(k/*)", TRIM("REF_KUNNR") FROM raw.{q(CLI_MASTER)} WHERE "_source" = %s',
            (source_name,),
        )
        return {lifnr: ref for lifnr, ref in cur.fetchall() if lifnr}


def get_exclude_fornitori_cli(conn) -> set:
    """Flusso 05a: codici (REF_KUNNR o LIFNR) già caricati in SAP (stg.export_exclude_fornitori_cli)."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('stg.export_exclude_fornitori_cli') IS NOT NULL")
        if not cur.fetchone()[0]:
            return set()
        cur.execute("SELECT TRIM(kunnr) FROM stg.export_exclude_fornitori_cli WHERE COALESCE(TRIM(kunnr), '') <> ''")
        return {r[0] for r in cur.fetchall()}


def customer_ref_row_fn(customer_codes: dict):
    """Flusso 05a: REF_KUNNR e KUNNR (vecchio codice cliente) -> codice SAP del cliente."""
    def fn(cols: list, row: list):
        for col in CLI_REF_COLS:
            if col in cols and row[cols.index(col)]:
                old = row[cols.index(col)].strip()
                row[cols.index(col)] = customer_codes.get(old, old)
    return fn


def get_row_rule(conn, table: str) -> str | None:
    """
    Condizione SQL delle righe valide di una tabella con regola in ROW_RULES, se il check
    è attivo nel catalogo e la tabella di riferimento è caricata; altrimenti None.
    """
    rule = ROW_RULES.get(table)
    if not rule:
        return None
    check_id, required, valid_if = rule
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = %s", (check_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        if required:
            schema, req_table = required.split(".", 1)
            cur.execute("SELECT to_regclass(%s)", (f"{schema}.{q(req_table)}",))
            if cur.fetchone()[0] is None:
                log.warning(f"  [{check_id}] {required} non caricata — righe di {table} non filtrate")
                return None
    return valid_if


def select_rows(conn, schema: str, table: str,
                source_filter: str, cols: list[str],
                excluded_keys: set | None = None,
                fk_col: str | None = None,
                where: str | None = None,
                only_keys: set | None = None) -> list[tuple]:
    """
    Legge i record di una tabella filtrati per _source, senza le chiavi escluse
    (orfani o BP già in SAP) e, se indicata, solo quelli che soddisfano la condizione where.
    only_keys: solo queste chiavi fk_col (estrazione ristretta).
    """
    fqt      = f'{schema}.{q(table)}'
    col_list = ", ".join(q(c) for c in cols)
    sql      = f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s'
    params   = [source_filter]

    # Esclude in SQL i record orfani (solo tabelle secondarie)
    if excluded_keys and fk_col and fk_col in cols:
        sql += f' AND {q(fk_col)} NOT IN ({",".join(["%s"] * len(excluded_keys))})'
        params += list(excluded_keys)
    if only_keys is not None and fk_col:
        if fk_col not in cols or not only_keys:
            return []
        sql += f' AND {q(fk_col)} IN ({",".join(["%s"] * len(only_keys))})'
        params += list(only_keys)
    if where:
        sql += f" AND ({where})"

    with conn.cursor() as cur:
        cur.execute(sql + " ORDER BY 1", params)
        return cur.fetchall()


def fetch_table_csv(conn, schema: str, table: str,
                    source_filter: str, cols: list[str],
                    excluded_keys: set | None = None,
                    fk_col: str | None = None,
                    where: str | None = None,
                    row_fn=None,
                    only_keys: set | None = None) -> bytes:
    """
    Legge i record di una tabella (vedi select_rows) e
    li serializza come CSV con separatore ";".
    row_fn: funzione (cols, riga come lista) che modifica la riga in uscita.
    Ritorna i bytes del CSV.
    """
    rows = select_rows(conn, schema, table, source_filter, cols, excluded_keys, fk_col, where, only_keys)
    if row_fn:
        rows = [list(r) for r in rows]
        for r in rows:
            row_fn(cols, r)
    if excluded_keys and fk_col and fk_col in cols:
        log.warning(
            "  [%s] %s.%s: %d chiavi escluse (orfani o BP già in SAP, _source=%s)",
            fk_col, schema, table, len(excluded_keys), source_filter
        )

    def sanitize(val):
        """Converte None, float NaN e stringa 'NaN' in stringa vuota."""
        if val is None:
            return ""
        if isinstance(val, float) and math.isnan(val):
            return ""
        if isinstance(val, str) and val.strip() == "NaN":
            return ""
        return val

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    return buf.getvalue().encode("utf-8")


def build_zip(conn, source_name: str, flow: dict, excluded_out: list | None = None,
              key_filter: set | None = None) -> bytes:
    """
    Costruisce il contenuto di un archivio ZIP per un dato _source del flusso (vedi FLOWS).
    Usa prd per le tabelle prd_tables, raw per tutte le altre.
    excluded_out: se passata, riceve le righe escluse perché il fornitore è già in SAP.
    key_filter:   codici fornitore dell'estrazione ristretta (None = output completo); non
                  scrive i file di scarto, già prodotti dall'output completo.
    """
    write_discards = key_filter is None
    buf = io.BytesIO()
    # Fornitori/vettori già creati in SAP (BUT000): esclusi da tutte le tabelle
    sap_map  = get_but000_map(conn, sort_cols=flow["sap_sort_cols"])
    sap_keys = set(sap_map)
    excluded = []

    # Carica le chiavi orfane (es. CK402) — solo tabelle secondarie raw
    check_id = flow["orphan_check"]
    orphan_keys = get_orphan_keys(conn, check_id) if check_id else set()
    if orphan_keys:
        log.warning(
            "  *** [%s] %d chiavi orfane per '%s' "
            "— verranno escluse dalle tabelle secondarie: %s",
            check_id, len(orphan_keys), source_name, sorted(orphan_keys)
        )
    elif check_id:
        log.info("  [%s] Nessuna chiave orfana per '%s'.", check_id, source_name)

    # Flusso 05a: codice SAP del cliente di riferimento; senza corrispondenza il fornitore è escluso
    row_fns = {}
    unmapped = set()
    if flow.get("customer_ref"):
        customer_codes = get_but000_map(conn, sort_cols=("BU_SORT2",))
        ref_map  = get_customer_ref_map(conn, source_name)
        unmapped = {lifnr for lifnr, ref in ref_map.items() if ref not in customer_codes}
        if unmapped:
            log.warning("  *** %d fornitori con cliente di riferimento non presente in BUT000 (BU_SORT2) "
                        "esclusi da '%s': %s", len(unmapped), source_name, sorted(unmapped))
        # Fornitori già caricati in SAP (elenco manuale): esclusi come i BP già in BUT000
        done_codes = get_exclude_fornitori_cli(conn)
        done = {lifnr: customer_codes.get(ref, "") for lifnr, ref in ref_map.items()
                if ref in done_codes or lifnr.strip() in done_codes}
        if done:
            log.info("  %d fornitori già caricati in SAP (stg.export_exclude_fornitori_cli) esclusi da '%s': %s",
                     len(done), source_name, sorted(done))
            sap_map  = {**sap_map, **done}
            sap_keys = set(sap_map)
        row_fns[CLI_MASTER] = customer_ref_row_fn(customer_codes)
        customers = get_customer_data(conn)
        row_fns[CLI_CONTACT] = customer_contact_row_fn(
            {lifnr: customers[ref] for lifnr, ref in ref_map.items() if ref in customers},
            key_col="LIFNR(k/*)")

    if CONTACT_TABLE in flow["raw_tables"]:
        row_fns[CONTACT_TABLE] = contact_row_fn(get_supplier_data(conn))

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

        # 1. Tabelle da prd (ordine definito in prd_tables)
        for prd_schema, prd_table in flow["prd_tables"]:
            prd_cols = get_columns(conn, prd_schema, prd_table)
            if prd_cols:
                excluded += already_in_sap_rows(conn, prd_schema, prd_table, source_name, "LIFNR(k/*)", sap_map)
                csv_bytes = fetch_table_csv(conn, prd_schema, prd_table, source_name, prd_cols,
                                            excluded_keys=sap_keys or None, fk_col="LIFNR(k/*)",
                                            only_keys=key_filter)
                n_rows = csv_bytes.count(b"\r\n") - 1
                if n_rows <= 0:
                    log.info(f"  {prd_schema}.{prd_table}: 0 righe per '{source_name}' — skip")
                    continue
                zf.writestr(f"{CSV_NAME.get(prd_table, prd_table)}.csv", csv_bytes)
                log.info(f"  + {prd_schema}.{prd_table}: {n_rows} righe")
            else:
                log.warning(f"  Nessuna colonna trovata per {prd_schema}.{prd_table}")

        # 2. Tabelle da raw (skip quelle già prese da prd)
        for raw_table in flow["raw_tables"]:
            if raw_table in PRD_TABLE_NAMES:
                log.info(f"  SKIP raw.{raw_table} — sostituito da prd")
                continue

            # Verifica che la tabella esista
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'raw' AND table_name = %s
                """, (raw_table,))
                if not cur.fetchone():
                    log.warning(f"  SKIP raw.{raw_table} — tabella non trovata")
                    continue

            raw_cols = get_columns(conn, "raw", raw_table)
            if not raw_cols:
                log.warning(f"  SKIP raw.{raw_table} — nessuna colonna")
                continue
            # Tabelle con merge in prd (es. filtri delle coordinate bancarie): righe lette da prd
            schema = "prd" if raw_table in flow.get("from_prd", ()) and get_columns(conn, "prd", raw_table) else "raw"

            _is_master = raw_table == flow["master"]
            discard_keys = unmapped if _is_master else orphan_keys | unmapped
            if discard_keys and write_discards:
                write_discarded_csv(conn, raw_table, source_name,
                                   raw_cols, discard_keys, "LIFNR(k/*)")
            excluded += already_in_sap_rows(conn, schema, raw_table, source_name, "LIFNR(k/*)", sap_map)
            excluded_keys = (sap_keys | unmapped if _is_master else orphan_keys | sap_keys | unmapped) or None
            valid_if = get_row_rule(conn, raw_table)
            if valid_if:
                invalid = select_rows(conn, schema, raw_table, source_name, raw_cols,
                                      excluded_keys, "LIFNR(k/*)", where=f"NOT ({valid_if})")
                if invalid and write_discards:
                    write_discarded_rows(raw_table, raw_cols, invalid)
                    log.warning(f"  [{ROW_RULES[raw_table][0]}] {schema}.{raw_table}: {len(invalid)} righe escluse "
                                f"dallo ZIP '{source_name}'")
            csv_bytes = fetch_table_csv(
                conn, schema, raw_table, source_name, raw_cols,
                excluded_keys=excluded_keys,
                fk_col="LIFNR(k/*)",
                where=valid_if,
                row_fn=row_fns.get(raw_table),
                only_keys=key_filter,
            )
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  {schema}.{raw_table}: 0 righe per source '{source_name}' — skip")
                continue

            zf.writestr(f"{CSV_NAME.get(raw_table, raw_table)}.csv", csv_bytes)
            log.info(f"  + {schema}.{raw_table}: {n_rows} righe")

    if excluded_out is not None:
        excluded_out.extend(excluded)
    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    remove_discarded_csv(RAW_TABLES + DM_RAW_TABLES + CLI_RAW_TABLES)
    conn = get_connection()

    try:
        for flow in FLOWS:
            sources = get_distinct_sources(conn, flow)
            if not sources:
                log.warning(f"Nessun valore di _source per {flow['master']} — nulla da esportare.")
                continue

            log.info(f"Sorgenti trovate: {sources}")

            for source_name in sources:
                log.info(f"=== Generazione ZIP: {source_name} ===")
                excluded  = []
                zip_bytes = build_zip(conn, source_name, flow, excluded_out=excluded)
                write_output_zip(source_name, zip_bytes)
                write_already_in_sap(source_name, excluded, CSV_NAME)
                if flow.get("customer_ref"):
                    # Estrazione ristretta (test): fornitori con cliente di riferimento o LIFNR nell'elenco
                    codes = get_export_filter(conn)
                    keys  = {lifnr for lifnr, ref in get_customer_ref_map(conn, source_name).items()
                             if ref in codes or lifnr.strip() in codes}
                    write_test_zip(source_name, lambda: build_zip(conn, source_name, flow, key_filter=keys),
                                   keys, "fornitori")

    finally:
        conn.close()

    log.info("=== export_suppliers_zip completato ===")


if __name__ == "__main__":
    main()
