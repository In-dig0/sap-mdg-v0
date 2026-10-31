""" @bruin
name: prd.export_suppliers_zip
type: python
description: >
  Genera gli archivi ZIP di output per i fornitori combinando:
    - prd."S_SUPPL_GEN#ZBP_DatiGenerali"  (dati anagrafici con indirizzi normalizzati)
    - tutte le altre tabelle fornitori da schema raw
  e, per i fornitori destinatari merci (flusso DM06-02b):
    - prd."S_SUPPL_GEN#ZDM_DatiGenerali" + raw."S_ROLES#ZDM_RuoliFornitori"
  Un ZIP separato viene creato per ogni valore distinto di _source
  (es. "DM06-01a-ZBP-Vettori.zip", "DM06-02b-ZDM-Fornitori.zip").
  I CSV dentro il ZIP usano separatore ";" e non includono colonne audit.
  Le righe segnalate dai check in ROW_RULES (CK051: settore industriale non in TB038A)
  sono escluse e scritte in <tabella>_discarded.csv; se non ne resta nessuna il CSV non
  è scritto.
  Negli interlocutori (S_SUPPL_CONT#ZBP-Interlocutore) le email E_MAIL_B/E_MAIL sono in
  minuscolo; VNAME/LNAME, COUNTRY e REGION vengono dall'anagrafica
  prd."S_SUPPL_GEN#ZBP_DatiGenerali" (corretta in STG): NAME_FIRST_P/NAME_LAST_P per
  le persone fisiche (NATPERS = X), LNAME = NAME_FIRST (VNAME vuoto) per le società.
  I fornitori/vettori già presenti in SAP (ref.SAP_EXPORT_BUT000, BU_SORT1) sono esclusi
  da tutte le tabelle: riepilogo in <nome ZIP>_gia_in_SAP.csv; se non resta nulla lo ZIP
  non è generato.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - ingestion.ingest_xlsx_to_ref
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
    write_discarded_rows,
)

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

# Righe escluse dallo ZIP (e scritte in <tabella>_discarded.csv) quando il check è attivo:
# tabella -> (check_id, tabella ref necessaria, condizione SQL delle righe valide)
ROW_RULES = {
    # CK051: coppia ISTYPE + IND_SECTOR presente in TB038A
    "S_SUPPL_INDUSTRY#ZBP_SettoriIndust": (
        "CK051", "SAP_EXPORT_TB038A",
        'EXISTS (SELECT 1 FROM ref."SAP_EXPORT_TB038A" t'
        ' WHERE t."ISTYPE" = "ISTYPE(k/*)" AND t."IND_SECTOR" = "IND_SECTOR(k/*)")',
    ),
}

# Interlocutori fornitori: nome, paese e regione dall'anagrafica fornitori in prd
CONTACT_TABLE  = "S_SUPPL_CONT#ZBP-Interlocutore"
PRD_SUPPLIER   = "S_SUPPL_GEN#ZBP_DatiGenerali"
EMAIL_COLS     = ["E_MAIL_B", "E_MAIL"]

# Nomi tabella raw sostituiti da prd (vengono skippati nella sezione raw)
PRD_TABLE_NAMES = {table for _, table in PRD_TABLES + DM_PRD_TABLES}

# Flussi esportati. sap_sort_cols: colonne di BUT000 con il vecchio codice dei BP già in
# SAP (esclusi dallo ZIP); orphan_check: check degli orfani nelle tabelle secondarie.
FLOWS = [
    {"prd_tables": PRD_TABLES, "raw_tables": RAW_TABLES,
     "master": "S_SUPPL_GEN#ZBP_DatiGenerali", "orphan_check": "CK402",
     "sap_sort_cols": ("BU_SORT1",)},
    {"prd_tables": DM_PRD_TABLES, "raw_tables": DM_RAW_TABLES,
     "master": "S_SUPPL_GEN#ZDM_DatiGenerali", "orphan_check": None,
     "sap_sort_cols": ("BU_SORT1", "BU_SORT2")},
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


def get_distinct_sources(conn, prd_tables: list) -> list[str]:
    """Recupera i valori distinti di _source dalla tabella prd principale del flusso."""
    schema, table = prd_tables[0]
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


def get_row_rule(conn, table: str) -> str | None:
    """
    Condizione SQL delle righe valide di una tabella con regola in ROW_RULES, se il check
    è attivo nel catalogo e la tabella di riferimento è caricata; altrimenti None.
    """
    rule = ROW_RULES.get(table)
    if not rule:
        return None
    check_id, ref_table, valid_if = rule
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = %s", (check_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        cur.execute("SELECT to_regclass(%s)", (f"ref.{q(ref_table)}",))
        if cur.fetchone()[0] is None:
            log.warning(f"  [{check_id}] ref.{ref_table} non caricata — righe di {table} non filtrate")
            return None
    return valid_if


def select_rows(conn, schema: str, table: str,
                source_filter: str, cols: list[str],
                excluded_keys: set | None = None,
                fk_col: str | None = None,
                where: str | None = None) -> list[tuple]:
    """
    Legge i record di una tabella filtrati per _source, senza le chiavi escluse
    (orfani o BP già in SAP) e, se indicata, solo quelli che soddisfano la condizione where.
    """
    fqt      = f'{schema}.{q(table)}'
    col_list = ", ".join(q(c) for c in cols)
    sql      = f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s'
    params   = [source_filter]

    # Esclude in SQL i record orfani (solo tabelle secondarie)
    if excluded_keys and fk_col and fk_col in cols:
        sql += f' AND {q(fk_col)} NOT IN ({",".join(["%s"] * len(excluded_keys))})'
        params += list(excluded_keys)
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
                    row_fn=None) -> bytes:
    """
    Legge i record di una tabella (vedi select_rows) e
    li serializza come CSV con separatore ";".
    row_fn: funzione (cols, riga come lista) che modifica la riga in uscita.
    Ritorna i bytes del CSV.
    """
    rows = select_rows(conn, schema, table, source_filter, cols, excluded_keys, fk_col, where)
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


def build_zip(conn, source_name: str, flow: dict, excluded_out: list | None = None) -> bytes:
    """
    Costruisce il contenuto di un archivio ZIP per un dato _source del flusso (vedi FLOWS).
    Usa prd per le tabelle prd_tables, raw per tutte le altre.
    excluded_out: se passata, riceve le righe escluse perché il fornitore è già in SAP.
    """
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

    contact_fn = (contact_row_fn(get_supplier_data(conn))
                  if CONTACT_TABLE in flow["raw_tables"] else None)

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

        # 1. Tabelle da prd (ordine definito in prd_tables)
        for prd_schema, prd_table in flow["prd_tables"]:
            prd_cols = get_columns(conn, prd_schema, prd_table)
            if prd_cols:
                excluded += already_in_sap_rows(conn, prd_schema, prd_table, source_name, "LIFNR(k/*)", sap_map)
                csv_bytes = fetch_table_csv(conn, prd_schema, prd_table, source_name, prd_cols,
                                            excluded_keys=sap_keys or None, fk_col="LIFNR(k/*)")
                n_rows = csv_bytes.count(b"\r\n") - 1
                if n_rows <= 0:
                    log.info(f"  {prd_schema}.{prd_table}: 0 righe per '{source_name}' — skip")
                    continue
                zf.writestr(f"{prd_table}.csv", csv_bytes)
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

            _is_master = raw_table == flow["master"]
            if not _is_master and orphan_keys:
                write_discarded_csv(conn, raw_table, source_name,
                                   raw_cols, orphan_keys, "LIFNR(k/*)")
            excluded += already_in_sap_rows(conn, "raw", raw_table, source_name, "LIFNR(k/*)", sap_map)
            excluded_keys = (sap_keys if _is_master else orphan_keys | sap_keys) or None
            valid_if = get_row_rule(conn, raw_table)
            if valid_if:
                invalid = select_rows(conn, "raw", raw_table, source_name, raw_cols,
                                      excluded_keys, "LIFNR(k/*)", where=f"NOT ({valid_if})")
                if invalid:
                    write_discarded_rows(raw_table, raw_cols, invalid)
                    log.warning(f"  [{ROW_RULES[raw_table][0]}] raw.{raw_table}: {len(invalid)} righe escluse "
                                f"dallo ZIP '{source_name}'")
            csv_bytes = fetch_table_csv(
                conn, "raw", raw_table, source_name, raw_cols,
                excluded_keys=excluded_keys,
                fk_col="LIFNR(k/*)",
                where=valid_if,
                row_fn=contact_fn if raw_table == CONTACT_TABLE else None,
            )
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  raw.{raw_table}: 0 righe per source '{source_name}' — skip")
                continue

            zf.writestr(f"{raw_table}.csv", csv_bytes)
            log.info(f"  + raw.{raw_table}: {n_rows} righe")

    if excluded_out is not None:
        excluded_out.extend(excluded)
    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    remove_discarded_csv(RAW_TABLES + DM_RAW_TABLES)
    conn = get_connection()

    try:
        for flow in FLOWS:
            sources = get_distinct_sources(conn, flow["prd_tables"])
            if not sources:
                log.warning(f"Nessun valore di _source in prd.{flow['master']} — nulla da esportare.")
                continue

            log.info(f"Sorgenti trovate: {sources}")

            for source_name in sources:
                log.info(f"=== Generazione ZIP: {source_name} ===")
                excluded  = []
                zip_bytes = build_zip(conn, source_name, flow, excluded_out=excluded)
                write_output_zip(source_name, zip_bytes)
                write_already_in_sap(source_name, excluded)

    finally:
        conn.close()

    log.info("=== export_suppliers_zip completato ===")


if __name__ == "__main__":
    main()
