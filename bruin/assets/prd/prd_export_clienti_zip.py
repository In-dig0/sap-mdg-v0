""" @bruin
name: prd.export_clienti_zip
type: python
description: >
  Genera gli archivi ZIP di output per i clienti combinando:
    - prd."S_CUST_GEN#ZBP_DatiGenerali"  (dati anagrafici con indirizzi normalizzati)
    - tutte le altre tabelle clienti da schema raw
  Un ZIP separato viene creato per ogni valore distinto di _source
  trovato in prd."S_CUST_GEN#ZBP_DatiGenerali".
  I CSV dentro il ZIP usano separatore ";" e non includono colonne audit.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - prd.merge_clienti
@bruin """

import os
import io
import csv
import zipfile
import logging
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

OUTPUT_DIR = "/project/datalake/out_source_mdg"

AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

# Tabella anagrafici da prd
PRD_TABLE = ("prd", "S_CUST_GEN#ZBP_DatiGenerali")

# Tabelle da raw (filtrate per _source)
RAW_TABLES = [
    "S_CUST_BANK_DATA#ZBP-AppoggioBanca",
    "S_CUST_COMPANY#ZBP-DatiSocieta",
    "S_CUST_GEN#ZBP-DatiGenerali",        # versione raw — sostituita da prd
    "S_CUST_SALES_DATA#ZBP-DatiVendite",
    "S_CUST_SALES_PARTNER#ZBP-Partner",
    "S_CUST_TAXCLASS#ZBP-IvaVendite",
    "S_CUST_TAXNUMBERS#ZBP-CodiciFisc",
    "S_ROLES#ZBP-RuoliClienti",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn) -> list[str]:
    schema, table = PRD_TABLE
    fqt = f'{schema}.{q(table)}'
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT DISTINCT "_source" FROM {fqt} WHERE "_source" IS NOT NULL ORDER BY 1'
        )
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def fetch_table_csv(conn, schema: str, table: str,
                    source_filter: str, cols: list[str]) -> bytes:
    fqt      = f'{schema}.{q(table)}'
    col_list = ", ".join(q(c) for c in cols)
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s ORDER BY 1',
            (source_filter,)
        )
        rows = cur.fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return cur.fetchone() is not None


def build_zip(conn, source_name: str) -> bytes:
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

        # 1. Tabella anagrafici da prd
        prd_schema, prd_table = PRD_TABLE
        prd_cols = get_columns(conn, prd_schema, prd_table)
        if prd_cols:
            csv_bytes = fetch_table_csv(conn, prd_schema, prd_table, source_name, prd_cols)
            n_rows = csv_bytes.count(b"\r\n") - 1
            zf.writestr(f"{prd_table}.csv", csv_bytes)
            log.info(f"  + {prd_schema}.{prd_table}: {n_rows} righe")
        else:
            log.warning(f"  Nessuna colonna trovata per {prd_schema}.{prd_table}")

        # 2. Tabelle da raw (skip S_CUST_GEN#ZBP-DatiGenerali — già presa da prd)
        raw_prd_equiv = "S_CUST_GEN#ZBP-DatiGenerali"
        for raw_table in RAW_TABLES:
            if raw_table == raw_prd_equiv:
                log.info(f"  SKIP raw.{raw_table} — sostituito da prd")
                continue

            if not table_exists(conn, "raw", raw_table):
                log.warning(f"  SKIP raw.{raw_table} — tabella non trovata")
                continue

            raw_cols = get_columns(conn, "raw", raw_table)
            if not raw_cols:
                log.warning(f"  SKIP raw.{raw_table} — nessuna colonna")
                continue

            csv_bytes = fetch_table_csv(conn, "raw", raw_table, source_name, raw_cols)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  raw.{raw_table}: 0 righe per '{source_name}' — skip")
                continue

            zf.writestr(f"{raw_table}.csv", csv_bytes)
            log.info(f"  + raw.{raw_table}: {n_rows} righe")

    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_connection()

    try:
        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun valore di _source trovato in prd — nulla da esportare.")
            return

        log.info(f"Sorgenti trovate: {sources}")

        for source_name in sources:
            log.info(f"=== Generazione ZIP: {source_name} ===")
            zip_bytes   = build_zip(conn, source_name)
            output_path = os.path.join(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(zip_bytes)
            size_kb = len(zip_bytes) / 1024
            log.info(f"  ✓  {output_path}  ({size_kb:.1f} KB)")

    finally:
        conn.close()

    log.info("=== export_clienti_zip completato ===")


if __name__ == "__main__":
    main()
