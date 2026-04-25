""" @bruin
name: ingestion.ingest_xlsx_to_ref
type: python
description: >
  Legge tutti i file XLSX da /project/datalake/in_source_sap/,
  carica ogni foglio nello schema ref di PostgreSQL.
  Nome tabella = nome file XLSX senza estensione.
  Nomi colonne = intestazioni originali invariate (virgolette doppie).
  Colonne audit: _source TEXT, _loaded_at TIMESTAMPTZ.
  Strategia: TRUNCATE + INSERT (full-refresh, i dati SAP sono stabili).
@bruin """

import os
import re
import io
import logging
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

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

INBOUND_PATH = "/project/datalake/in_source_sap"
REF_SCHEMA   = "ref"


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_table(cur, schema: str, table: str, col_names: list):
    """Crea la tabella se non esiste, aggiunge colonne mancanti, TRUNCATE."""
    fqt = f'{schema}.{q(table)}'

    col_defs = ",\n    ".join(f"{q(c)} TEXT" for c in col_names)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {fqt} (
            {col_defs},
            "_source" TEXT,
            "_loaded_at"   TIMESTAMPTZ
        )
    """)

    # Schema evolution
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table))
    existing = {r[0] for r in cur.fetchall()}

    for col in col_names:
        if col not in existing:
            cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS {q(col)} TEXT")
            log.info(f"    + Colonna aggiunta: {col}")
    for col, ctype in [("_source", "TEXT"), ("_loaded_at", "TIMESTAMPTZ")]:
        if col not in existing:
            cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS {q(col)} {ctype}")

    # TRUNCATE totale (dati SAP sono stabili, full-refresh)
    cur.execute(f'TRUNCATE TABLE {fqt}')
    log.info(f"    Tabella {fqt} svuotata (TRUNCATE)")


def ingest_df(cur, schema: str, table: str,
              df: pd.DataFrame, xlsx_name: str) -> int:
    if df.empty:
        log.warning("    Foglio vuoto, skip.")
        return 0

    fqt     = f'{schema}.{q(table)}'
    now_utc = datetime.now(timezone.utc)
    df_ins  = df.copy()
    df_ins["_source"] = xlsx_name
    df_ins["_loaded_at"]   = now_utc

    col_list = ", ".join(q(c) for c in df_ins.columns)
    sql      = f"INSERT INTO {fqt} ({col_list}) VALUES %s"
    rows     = [tuple(r) for r in df_ins.itertuples(index=False, name=None)]

    execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def process_xlsx(xlsx_path: str, conn):
    xlsx_name  = os.path.basename(xlsx_path)
    table_name = os.path.splitext(xlsx_name)[0]
    log.info(f"=== XLSX: {xlsx_name} ===")
    log.info(f"  Tabella: {REF_SCHEMA}.{q(table_name)}")

    # Leggi tutti i fogli — per file SAP di solito è uno solo
    xls = pd.ExcelFile(xlsx_path)
    log.info(f"  Fogli trovati: {xls.sheet_names}")

    # Usa il primo foglio
    df = pd.read_excel(
        xlsx_path,
        sheet_name=0,
        dtype=str,
        keep_default_na=False,
    )

    # Nomi colonna originali (strip spazi)
    col_names = [c.strip() for c in df.columns]
    df.columns = col_names

    log.info(f"  Righe: {len(df)} | Colonne: {len(df.columns)}")
    log.info(f"  Colonne: {col_names}")

    with conn.cursor() as cur:
        ensure_table(cur, REF_SCHEMA, table_name, col_names)
        n = ingest_df(cur, REF_SCHEMA, table_name, df, xlsx_name)
        conn.commit()

    log.info(f"  Inseriti: {n} record in {REF_SCHEMA}.{q(table_name)}")
    return n


def main():
    if not os.path.isdir(INBOUND_PATH):
        raise FileNotFoundError(f"Cartella inbound non trovata: {INBOUND_PATH}")

    xlsx_files = sorted([
        os.path.join(INBOUND_PATH, f)
        for f in os.listdir(INBOUND_PATH)
        if f.lower().endswith((".xlsx", ".xls"))
    ])

    if not xlsx_files:
        log.warning(f"Nessun file XLSX in {INBOUND_PATH}")
        return

    log.info(f"XLSX da processare: {len(xlsx_files)}")
    conn = get_connection()
    try:
        for xlsx_path in xlsx_files:
            process_xlsx(xlsx_path, conn)
    finally:
        conn.close()

    log.info("=== Ingestion XLSX completata ===")


if __name__ == "__main__":
    main()
