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
  - Se la tabella esiste già → skip (gestita da detect_new_records).
  - Colonne audit: _status TEXT, _source TEXT, _loaded_at TIMESTAMPTZ.
depends:
  - ingestion.ingest_zip_to_raw
connections:
    mdg_postgres: pg
@bruin """

import os
import re
import glob
import psycopg2
import pandas as pd
from datetime import datetime, timezone

SOURCE_DIR = "/project/datalake/in_source_others"
SCHEMA     = "stg"
BATCH_SIZE = 500


def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def ensure_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table))
    return cur.fetchone() is not None


def get_key_columns(col_names: list[str]) -> list[str]:
    return [c for c in col_names if re.search(r"\(.*?k.*?\)", c)]


def load_xlsx(cur, xlsx_path: str, loaded_at: str) -> tuple[int, bool]:
    """
    Carica un singolo file XLSX in stg solo se la tabella non esiste ancora.
    - _status forzato a 'EXISTS' (ignora il valore nel file)
    - UNIQUE index sulla chiave per bloccare duplicati da detect_new_records
    """
    table = os.path.splitext(os.path.basename(xlsx_path))[0]
    fqt   = f'{SCHEMA}."{table}"'

    if table_exists(cur, SCHEMA, table):
        print(f'  SKIP  stg."{table}" — tabella già esistente, gestita da detect_new_records.')
        return 0, True

    df = pd.read_excel(xlsx_path, dtype=str, keep_default_na=False, na_values=[])
    df = df.replace("nan", "").fillna("")

    if df.empty:
        print(f"  WARN: {os.path.basename(xlsx_path)} è vuoto — skip.")
        return 0, True

    # Rimuove _status dal file: lo gestiamo noi con valore fisso 'EXISTS'
    data_cols = [c for c in df.columns if c != "_status"]
    df = df[data_cols]
    key_cols = get_key_columns(data_cols)

    # Crea tabella
    col_defs = ", ".join([f'"{c}" TEXT' for c in data_cols])
    cur.execute(f"""
        CREATE TABLE {fqt} (
            {col_defs},
            "_status"    TEXT DEFAULT 'EXISTS',
            "_source"    TEXT,
            "_loaded_at" TIMESTAMPTZ
        )
    """)

    # UNIQUE index sulla chiave
    if key_cols:
        key_def  = ", ".join(f'"{k}"' for k in key_cols)
        idx_name = re.sub(r"[^a-z0-9]", "_", table.lower())[:40]
        cur.execute(f'CREATE UNIQUE INDEX "uidx_{idx_name}" ON {fqt} ({key_def})')
        print(f'  UNIQUE index su: {key_cols}')

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


def main():
    xlsx_files = sorted(glob.glob(os.path.join(SOURCE_DIR, "*.xlsx")))
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
