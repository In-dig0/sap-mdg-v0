"""
MDG — Asset Bruin: ingestion ZIP → schema raw
==============================================
Regole:
  - Nome tabella  = nome file CSV senza .csv, tra virgolette doppie
                    es. S_SUPPL_GEN#ZBP_DatiGenerali.csv
                    →   raw."S_SUPPL_GEN#ZBP_DatiGenerali"
  - Nomi colonne  = intestazioni ORIGINALI invariate, tra virgolette doppie
                    es. "LIFNR(k/*)"  "AKONT(*)"  "BP_ROLE(k/*)"
  - Colonne chiave: quelle con 'k' nel suffisso → UNIQUE INDEX
  - Colonne audit: "_zip_source" TEXT, "_loaded_at" TIMESTAMPTZ
  - Prima del reload: DELETE selettivo per _zip_source = nome ZIP
  - Duplicati su chiave: scartati PRIMA dell'insert e loggati
  - Tutte le colonne dati sono TEXT
"""

import os
import re
import zipfile
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
    "host":     os.environ["BRUIN_POSTGRES_HOST"],
    "port":     int(os.environ.get("BRUIN_POSTGRES_PORT", 5432)),
    "dbname":   os.environ["BRUIN_POSTGRES_DB"],
    "user":     os.environ["BRUIN_POSTGRES_USER"],
    "password": os.environ["BRUIN_POSTGRES_PASSWORD"],
}

INBOUND_PATH = "/data/inbound/from_olderp"
RAW_SCHEMA   = "raw"


def identify_keys(raw_cols: list) -> list:
    """Restituisce i nomi ORIGINALI delle colonne chiave (suffisso con 'k')."""
    return [
        c.strip() for c in raw_cols
        if re.search(r"\(.*?k.*?\)", c.strip())
    ]


def key_display_name(col: str) -> str:
    """Nome pulito dal suffisso, usato solo per i messaggi di log."""
    return re.sub(r"\(.*?\)", "", col).strip()


def q(name: str) -> str:
    """Identificatore SQL tra virgolette doppie (escape delle virgolette interne)."""
    return '"' + name.replace('"', '""') + '"'


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_table(cur, schema: str, table: str,
                 raw_cols: list, key_cols: list, zip_name: str):
    """
    Crea la tabella se non esiste, aggiunge colonne mancanti,
    crea UNIQUE INDEX sulle chiavi, poi cancella i record del ZIP.
    """
    fqt = f'{schema}.{q(table)}'

    # CREATE TABLE IF NOT EXISTS
    col_defs = ",\n    ".join(f"{q(c)} TEXT" for c in raw_cols)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {fqt} (
            {col_defs},
            "_zip_source" TEXT,
            "_loaded_at"  TIMESTAMPTZ
        )
    """)

    # Schema evolution: aggiunge colonne mancanti
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table))
    existing = {r[0] for r in cur.fetchall()}

    for col in raw_cols:
        if col not in existing:
            cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS {q(col)} TEXT")
            log.info(f"    + Colonna aggiunta: {col}")
    for col, ctype in [("_zip_source", "TEXT"), ("_loaded_at", "TIMESTAMPTZ")]:
        if col not in existing:
            cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS {q(col)} {ctype}")

    # UNIQUE INDEX su (chiavi..., _zip_source)
    if key_cols:
        safe_name = re.sub(r"[^a-z0-9]", "_", table.lower())[:40]
        idx_name  = f"uidx_{safe_name}_key"
        key_defs  = ", ".join(q(c) for c in key_cols)
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {q(idx_name)}
            ON {fqt} ({key_defs}, "_zip_source")
        """)

    # DELETE selettivo per questo ZIP
    cur.execute(f'DELETE FROM {fqt} WHERE "_zip_source" = %s', (zip_name,))
    deleted = cur.rowcount
    if deleted > 0:
        log.info(f"    Rimossi {deleted} record precedenti di '{zip_name}'")


def deduplicate(df: pd.DataFrame, key_cols: list) -> tuple:
    """
    Rimuove i duplicati su chiave PRIMA dell'insert.
    Restituisce (df_clean, df_dupes).
    Strategia: mantiene il primo occorrenza, scarta i successivi.
    """
    if not key_cols:
        return df, pd.DataFrame()

    # Usa i nomi originali delle colonne chiave per il groupby
    # (sono già presenti nel df con il nome originale es. "LIFNR(k/*)")
    dupes_mask = df.duplicated(subset=key_cols, keep="first")
    df_dupes   = df[dupes_mask].copy()
    df_clean   = df[~dupes_mask].copy()
    return df_clean, df_dupes


def ingest_csv(cur, schema: str, table: str,
               df: pd.DataFrame, key_cols: list, zip_name: str) -> tuple:
    """
    1. Deduplica nel DataFrame (prima dell'insert)
    2. Logga i record scartati con i valori delle chiavi
    3. Inserisce i record puliti in batch
    Restituisce (inseriti, scartati).
    """
    if df.empty:
        log.warning("    CSV vuoto, skip.")
        return 0, 0

    # Deduplicazione preventiva
    df_clean, df_dupes = deduplicate(df, key_cols)
    discarded = len(df_dupes)

    if discarded > 0:
        key_display = [key_display_name(k) for k in key_cols]
        for _, row in df_dupes.iterrows():
            key_values = {kd: row.get(ko, "?")
                          for kd, ko in zip(key_display, key_cols)}
            log.warning(
                f"    [SCARTATO] Duplicato su chiave {key_values} "
                f"— tabella {schema}.{q(table)}"
            )

    if df_clean.empty:
        log.warning("    Nessun record da inserire dopo deduplicazione.")
        return 0, discarded

    # Aggiunge colonne audit
    fqt     = f'{schema}.{q(table)}'
    now_utc = datetime.now(timezone.utc)
    df_ins  = df_clean.copy()
    df_ins["_zip_source"] = zip_name
    df_ins["_loaded_at"]  = now_utc

    col_list = ", ".join(q(c) for c in df_ins.columns)
    sql      = f"INSERT INTO {fqt} ({col_list}) VALUES %s"
    rows     = [tuple(r) for r in df_ins.itertuples(index=False, name=None)]

    execute_values(cur, sql, rows, page_size=500)
    return len(rows), discarded


def process_zip(zip_path: str, conn):
    zip_name = os.path.basename(zip_path)
    log.info(f"=== ZIP: {zip_name} ===")

    total_ins  = 0
    total_disc = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        log.info(f"  CSV trovati: {len(csv_files)}")

        for csv_filename in csv_files:
            table_name = os.path.splitext(csv_filename)[0]
            log.info(f"  -> {csv_filename}")
            log.info(f"     Tabella: {RAW_SCHEMA}.{q(table_name)}")

            with zf.open(csv_filename) as f:
                raw_bytes = f.read()

            df = pd.read_csv(
                io.BytesIO(raw_bytes),
                sep=";",
                encoding="utf-8-sig",
                dtype=str,
                keep_default_na=False,
            )

            # Nomi colonna ORIGINALI (solo strip spazi iniziali/finali di riga)
            raw_cols = [c.strip() for c in df.columns]
            df.columns = raw_cols

            key_cols = identify_keys(raw_cols)

            log.info(f"     Righe: {len(df)} | Colonne: {len(df.columns)}")
            log.info(f"     Chiavi: {key_cols}")

            with conn.cursor() as cur:
                ensure_table(cur, RAW_SCHEMA, table_name,
                             raw_cols, key_cols, zip_name)
                ins, disc = ingest_csv(cur, RAW_SCHEMA, table_name,
                                       df, key_cols, zip_name)
                conn.commit()

            total_ins  += ins
            total_disc += disc
            status = "OK" if disc == 0 else f"ATTENZIONE: {disc} duplicati"
            log.info(f"     Inseriti: {ins} | Scartati: {disc} | {status}")

    log.info(
        f"  Riepilogo '{zip_name}': "
        f"inseriti={total_ins}, scartati={total_disc}"
    )


def main():
    if not os.path.isdir(INBOUND_PATH):
        raise FileNotFoundError(
            f"Cartella inbound non trovata: {INBOUND_PATH}"
        )

    zip_files = sorted([
        os.path.join(INBOUND_PATH, f)
        for f in os.listdir(INBOUND_PATH)
        if f.lower().endswith(".zip")
    ])

    if not zip_files:
        log.warning(f"Nessun file ZIP in {INBOUND_PATH}")
        return

    log.info(f"ZIP da processare: {len(zip_files)}")
    conn = get_connection()
    try:
        for zip_path in zip_files:
            process_zip(zip_path, conn)
    finally:
        conn.close()

    log.info("=== Ingestion completata ===")


if __name__ == "__main__":
    main()
