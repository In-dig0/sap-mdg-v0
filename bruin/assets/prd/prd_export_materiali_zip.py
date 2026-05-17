""" @bruin
name: prd.export_materiali_zip
type: python
description: >
  Genera l'archivio ZIP di output per i materiali.
  Tutte le tabelle vengono lette dallo schema raw e scritte integralmente —
  nessuna esclusione di record orfani (i check CK segnalano le anomalie nel
  report qualita, ma non bloccano la scrittura dell'output).
  Il CSV usa separatore ";" e non include colonne audit.
  Output: /project/datalake/out_source_mdg/DM08_MATERIALI.zip
depends: []
@bruin """

import os
import io
import csv
import zipfile
import logging
import math
import psycopg2

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

OUTPUT_DIR  = "/project/datalake/out_source_mdg"
SOURCE_NAME = "DM08_MATERIALI.zip"
AUDIT_COLS  = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

RAW_TABLES = [
    "S_MARA",
    "S_MAKT",
    "S_MARC",
    "S_MARD",
    "S_MARM",
    "S_MBEW",
    "S_MLAN",
    "S_MVKE",
    "S_QMAT",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name):
    return '"' + name.replace('"', '""') + '"'


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
        cur.execute(
            "SELECT 1 FROM information_schema.tables"
            " WHERE table_schema='raw' AND table_name=%s", (table,)
        )
        return cur.fetchone() is not None


def fetch_table_csv(conn, table, cols):
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s ORDER BY 1',
            (SOURCE_NAME,)
        )
        rows = cur.fetchall()

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


def build_zip(conn, cols_map):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in RAW_TABLES:
            cols = cols_map.get(table)
            if not cols:
                log.warning("  SKIP raw.%s — tabella non trovata o senza colonne", table)
                continue
            csv_bytes = fetch_table_csv(conn, table, cols)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe — skip", table)
                continue
            zf.writestr(f"{table}.csv", csv_bytes)
            log.info("  + raw.%s: %d righe", table, n_rows)
    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_connection()
    try:
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

        if not cols_map:
            log.warning("Nessuna tabella trovata — export annullato.")
            return

        log.info("=== Generazione ZIP: %s ===", SOURCE_NAME)
        zip_bytes   = build_zip(conn, cols_map)
        output_path = os.path.join(OUTPUT_DIR, SOURCE_NAME)
        with open(output_path, "wb") as f:
            f.write(zip_bytes)
        log.info("  ✓  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)
    finally:
        conn.close()
    log.info("=== export_materiali_zip completato ===")


if __name__ == "__main__":
    main()
