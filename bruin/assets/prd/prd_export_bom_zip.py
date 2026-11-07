""" @bruin
name: prd.export_bom_zip
type: python
description: >
  Genera l'archivio ZIP di output per le distinte base (BOM).
  Tabelle coinvolte:
    - S_BOM_HEADER  (intestazioni distinta base)
    - S_BOM_ITEM    (componenti distinta base)
  Nessuna esclusione di record orfani (CK406 segnala le anomalie nel report
  qualita, ma non blocca la scrittura dell'output).
  Con l'esclusione dall'output di CK527 attiva nel catalogo (default: disattivata), le
  distinte con materiale padre o un componente non ancora migrati in SAP non sono esportate:
  testata e tutte le posizioni (chiave MATNR, WERKS, STLAN, STLAL) finiscono in
  <tabella>_discarded.csv.
  Il CSV usa separatore ";" e non include colonne audit.
  Un ZIP separato per ogni valore distinto di _source.
  Output: /project/datalake/out_source_mdg/<_source>
depends:
  - stg.ck406_bom_item_header_orphans
  - stg.ck527_bom_materiale_in_sap_marc
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
from prd_export_clienti_zip import write_discarded_rows, remove_discarded_csv
from prd_merge_lib import exclusion_enabled, failing_keys

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
AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}
RAW_TABLES = ["S_BOM_HEADER", "S_BOM_ITEM"]
BOM_KEY_COLS = ["MATNR(k/*)", "WERKS(k)", "STLAN(k/*)", "STLAL(k/*)"]   # chiave della distinta


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT DISTINCT "_source" FROM raw."S_BOM_HEADER" WHERE "_source" IS NOT NULL ORDER BY 1')
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'raw' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def table_exists(conn, table):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='raw' AND table_name=%s", (table,))
        return cur.fetchone() is not None


def get_excluded_boms(conn):
    """
    Chiavi MATNR|WERKS|STLAN|STLAL delle distinte con un esito non Ok di CK527 (testata o
    posizione), se l'esclusione dall'output di CK527 è attiva nel catalogo.
    """
    with conn.cursor() as cur:
        if not exclusion_enabled(cur, "CK527"):
            return set()
        boms = {"|".join(key.split("|")[:len(BOM_KEY_COLS)]) for key in failing_keys(cur, "CK527")}
    if boms:
        log.warning("  *** [CK527] %d distinte base ESCLUSE (testata e posizioni): materiale padre o "
                    "componente non ancora migrato in SAP", len(boms))
    return boms


def fetch_table_csv(conn, table, source_name, cols, excluded):
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    with conn.cursor() as cur:
        cur.execute(f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s ORDER BY 1', (source_name,))
        rows = cur.fetchall()
    if excluded:
        idx = [cols.index(c) for c in BOM_KEY_COLS]
        bom_key = lambda row: "|".join(row[i] or "" for i in idx)
        discarded = [row for row in rows if bom_key(row) in excluded]
        if discarded:
            write_discarded_rows(table, cols, discarded)
            rows = [row for row in rows if bom_key(row) not in excluded]

    def sanitize(val):
        if val is None: return ""
        if isinstance(val, float) and math.isnan(val): return ""
        if isinstance(val, str) and val.strip() == "NaN": return ""
        return val

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    return buf.getvalue().encode("utf-8")


def build_zip(conn, source_name, cols_map, excluded):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in RAW_TABLES:
            cols = cols_map.get(table)
            if not cols:
                log.warning("  SKIP raw.%s — tabella non trovata o senza colonne", table)
                continue
            csv_bytes = fetch_table_csv(conn, table, source_name, cols, excluded)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe per '%s' — skip", table, source_name)
                continue
            zf.writestr(f"{table}.csv", csv_bytes)
            log.info("  + raw.%s: %d righe", table, n_rows)
    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    remove_discarded_csv(RAW_TABLES)
    conn = get_connection()
    try:
        if not table_exists(conn, "S_BOM_HEADER"):
            log.warning("raw.S_BOM_HEADER non trovata — export annullato.")
            return
        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun _source trovato in raw.S_BOM_HEADER — nulla da esportare.")
            return
        cols_map = {}
        for table in RAW_TABLES:
            if table_exists(conn, table):
                cols = get_columns(conn, table)
                if cols:
                    cols_map[table] = cols
                else:
                    log.warning("  raw.%s — nessuna colonna, verra omessa.", table)
            else:
                log.warning("  raw.%s — tabella non trovata, verra omessa.", table)
        excluded = get_excluded_boms(conn)
        log.info("Sorgenti trovate: %s", sources)
        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)
            zip_bytes   = build_zip(conn, source_name, cols_map, excluded)
            output_path = os.path.join(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(zip_bytes)
            log.info("  checkmark  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)
    finally:
        conn.close()
    log.info("=== export_bom_zip completato ===")


if __name__ == "__main__":
    main()
