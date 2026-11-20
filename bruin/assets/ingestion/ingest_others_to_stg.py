""" @bruin
name: ingestion.ingest_others_to_stg
type: python
description: >
  Legge tutti i file XLSX da /project/datalake/in_source_others/ e li carica
  nello schema stg di PostgreSQL.
  - Nome tabella stg = nome file XLSX senza estensione.
  - Nomi colonne = intestazioni originali invariate (virgolette doppie).
  - _status forzato a 'EXISTS' indipendentemente dal valore nel file.
  - UNIQUE index sulla colonna chiave (k) per prevenire duplicati.
  - Se la tabella esiste ed è popolata → skip (gestita da detect_new_records).
  - Se la tabella esiste ma è vuota (es. dopo TRUNCATE) → ricarica i dati.
  - Colonne audit: _status TEXT, _source TEXT, _loaded_at TIMESTAMPTZ.
  Tabelle "specchio" del file (FILE_MIRROR_TABLES, es. S_OPERATION_STG dei cicli): ricaricate
  dal file a ogni run, senza indice UNIQUE (la chiave si completa nel merge); se il file non è
  più in in_source_others la tabella viene eliminata.
depends:
  - ingestion.ingest_zip_to_raw
connections:
    mdg_postgres: pg
@bruin """

import os
import sys
import re
import glob
import psycopg2
import pandas as pd
from datetime import datetime, timezone
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from mdg_env import DB_CONFIG, IN_OTHERS  # noqa: E402

SOURCE_DIR = IN_OTHERS
SCHEMA     = "stg"
BATCH_SIZE = 500
# Tabelle ricaricate dal file a ogni run (il file è la fonte, non si modificano da Streamlit):
# niente indice UNIQUE perché la chiave è incompleta nel file (S_OPERATION_STG: PLNNR vuoto,
# ricavato dal materiale nel merge prd.merge_cicli, che segnala anche le chiavi duplicate).
FILE_MIRROR_TABLES = {"S_OPERATION_STG"}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table))
    return cur.fetchone() is not None


def table_has_rows(cur, schema: str, table: str) -> bool:
    """Restituisce True se la tabella contiene almeno una riga."""
    cur.execute(f'SELECT EXISTS (SELECT 1 FROM {schema}."{table}" LIMIT 1)')
    return cur.fetchone()[0]


def get_key_columns(col_names: list[str]) -> list[str]:
    return [c for c in col_names if re.search(r"\(.*?k.*?\)", c)]


def load_xlsx(cur, xlsx_path: str, loaded_at: str) -> tuple[int, bool]:
    """
    Carica un singolo file XLSX in stg.
    - Skip solo se la tabella esiste ED è popolata (gestita da detect_new_records).
    - Se la tabella esiste ma è vuota (dopo TRUNCATE) → reinserisce i dati.
    - _status forzato a 'EXISTS' (ignora il valore nel file).
    - UNIQUE index sulla chiave per bloccare duplicati da detect_new_records.
    """
    table      = os.path.splitext(os.path.basename(xlsx_path))[0]
    fqt        = f'{SCHEMA}."{table}"'
    exists     = table_exists(cur, SCHEMA, table)
    has_rows   = table_has_rows(cur, SCHEMA, table) if exists else False

    mirror     = table in FILE_MIRROR_TABLES
    if mirror and exists:
        cur.execute(f"DROP TABLE {fqt}")
        exists = has_rows = False
        print(f'  RELOAD stg."{table}" — ricaricata dal file a ogni run.')

    if exists and has_rows:
        print(f'  SKIP  stg."{table}" — tabella popolata, gestita da detect_new_records.')
        return 0, True

    df = pd.read_excel(xlsx_path, dtype=str, keep_default_na=False, na_values=[])
    df = df.replace("nan", "").fillna("")

    if df.empty:
        print(f"  WARN: {os.path.basename(xlsx_path)} è vuoto — skip.")
        return 0, True

    # Rimuove _status dal file: lo gestiamo noi con valore fisso 'EXISTS'
    data_cols = [c for c in df.columns if c != "_status"]
    df        = df[data_cols]
    key_cols  = get_key_columns(data_cols)

    if not exists:
        # Crea tabella e indice (primo caricamento)
        col_defs = ", ".join([f'"{c}" TEXT' for c in data_cols])
        cur.execute(f"""
            CREATE TABLE {fqt} (
                {col_defs},
                "_status"    TEXT DEFAULT 'EXISTS',
                "_source"    TEXT,
                "_loaded_at" TIMESTAMPTZ
            )
        """)
        if key_cols and not mirror:
            key_def  = ", ".join(f'"{k}"' for k in key_cols)
            idx_name = re.sub(r"[^a-z0-9]", "_", table.lower())[:40]
            cur.execute(f'CREATE UNIQUE INDEX "uidx_{idx_name}" ON {fqt} ({key_def})')
            print(f'  UNIQUE index su: {key_cols}')
    else:
        # Tabella vuota dopo TRUNCATE — reinserisce senza ricreare struttura
        print(f'  RELOAD stg."{table}" — tabella vuota, ricarico i dati.')

    placeholders = ", ".join(["%s"] * (len(data_cols) + 3))
    insert_sql   = f'INSERT INTO {fqt} VALUES ({placeholders}) ON CONFLICT DO NOTHING'

    inserted = 0
    batch    = []
    for row in df.itertuples(index=False, name=None):
        batch.append(list(row) + ['EXISTS', os.path.basename(xlsx_path), loaded_at])
        if len(batch) >= BATCH_SIZE:
            cur.executemany(insert_sql, batch)
            inserted += len(batch)
            batch = []
    if batch:
        cur.executemany(insert_sql, batch)
        inserted += len(batch)

    return inserted, False


def drop_missing_mirrors(xlsx_files: list[str]):
    """Elimina le tabelle specchio il cui file non è più in in_source_others."""
    present = {os.path.splitext(os.path.basename(f))[0] for f in xlsx_files}
    missing = FILE_MIRROR_TABLES - present
    if not missing:
        return
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            for table in sorted(missing):
                if table_exists(cur, SCHEMA, table):
                    cur.execute(f'DROP TABLE {SCHEMA}."{table}"')
                    print(f'  DROP  stg."{table}" — file {table}.xlsx non più presente in {SOURCE_DIR}')
    finally:
        conn.close()


def main():
    xlsx_files = sorted(glob.glob(os.path.join(SOURCE_DIR, "*.xlsx")))
    drop_missing_mirrors(xlsx_files)
    if not xlsx_files:
        print(f"Nessun file XLSX trovato in {SOURCE_DIR} — nulla da fare.")
        return

    loaded_at = datetime.now(timezone.utc).isoformat()
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_schema(cur)
                for xlsx_path in xlsx_files:
                    n, skipped = load_xlsx(cur, xlsx_path, loaded_at)
                    if not skipped:
                        table = os.path.splitext(os.path.basename(xlsx_path))[0]
                        print(f'  ✓  stg."{table}"  ({n} righe)')
    finally:
        conn.close()
    print(f"ingest_others_to_stg completato: {len(xlsx_files)} file elaborati.")


if __name__ == "__main__":
    main()
