""" @bruin
name: ingestion.ingest_zip_to_raw
type: python
description: >
  Legge tutti i file ZIP da /project/datalake/from_olderp/,
  estrae i CSV e li carica nello schema raw di PostgreSQL.
  Nome tabella = nome file CSV senza estensione (virgolette doppie).
  Nomi colonne = intestazioni originali invariate (virgolette doppie).
  Colonne con (k) nel suffisso → UNIQUE INDEX sulla tabella.
  Duplicati su chiave: scartati e loggati prima dell'insert.
  Righe malformate (campi in eccesso): skippate e loggatecon numero riga.
  Caratteri non stampabili o non UTF-8: rimossi e loggati.
  Colonne audit: _zip_source TEXT, _loaded_at TIMESTAMPTZ.
  Strategia: DELETE selettivo per _zip_source + INSERT.
@bruin """

import os
import re
import sys
import zipfile
import io
import logging
import warnings
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

INBOUND_PATH = "/project/datalake/from_olderp"
RAW_SCHEMA   = "raw"


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def identify_keys(raw_cols: list) -> list:
    return [c.strip() for c in raw_cols if re.search(r"\(.*?k.*?\)", c.strip())]


def key_display_name(col: str) -> str:
    return re.sub(r"\(.*?\)", "", col).strip()


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# Pulizia bytes: rimuove caratteri non stampabili e non UTF-8
# ---------------------------------------------------------------------------

def clean_bytes(raw_bytes: bytes, csv_filename: str) -> bytes:
    """
    1. Decodifica con UTF-8, sostituendo byte non validi con il
       carattere di replacement U+FFFD (invece di crashare).
    2. Rimuove caratteri di controllo non stampabili (eccetto
       tab, newline, carriage return che sono legittimi nei CSV).
    3. Logga quante sostituzioni sono avvenute.
    """
    # Decodifica con gestione errori — i byte non UTF-8 diventano '?'
    text_raw  = raw_bytes.decode("utf-8-sig", errors="replace")
    text_clean = text_raw

    # Rimuovi caratteri di controllo non stampabili (U+0000-U+001F esclusi
    # \t=0x09, \n=0x0A, \r=0x0D che sono legittimi nei CSV)
    non_printable = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
    matches = non_printable.findall(text_clean)
    if matches:
        unique_chars = set(repr(c) for c in matches)
        text_clean = non_printable.sub("", text_clean)
        log.warning(
            f"    [PULIZIA] {csv_filename}: rimossi {len(matches)} caratteri "
            f"non stampabili/non UTF-8: {unique_chars}"
        )

    # Controlla se ci sono stati replacement U+FFFD (byte non UTF-8)
    replacements = text_clean.count("\ufffd")
    if replacements > 0:
        log.warning(
            f"    [PULIZIA] {csv_filename}: {replacements} byte non UTF-8 "
            f"sostituiti con carattere di replacement (U+FFFD)"
        )

    return text_clean.encode("utf-8")


# ---------------------------------------------------------------------------
# Lettura CSV con intercettazione righe malformate
# ---------------------------------------------------------------------------

class BadLineCollector:
    """Collector che intercetta i warning di pandas sulle righe malformate."""
    def __init__(self, csv_filename: str):
        self.csv_filename = csv_filename
        self.bad_lines    = []

    def warn(self, msg):
        self.bad_lines.append(msg)
        log.warning(
            f"    [RIGA SKIPPATA] {self.csv_filename}: {msg}"
        )


def scan_bad_lines(clean_bytes: bytes, csv_filename: str) -> list:
    """
    Pre-scansiona il CSV riga per riga per trovare le righe con
    un numero di campi diverso dall'header.
    Restituisce lista di dict con numero riga, campi attesi/trovati e contenuto.
    """
    bad = []
    lines = clean_bytes.decode("utf-8").splitlines()
    if not lines:
        return bad

    # Numero campi attesi dall'header
    header_fields = len(lines[0].split(";"))

    for i, line in enumerate(lines[1:], start=2):  # riga 1 = header, partiamo da 2
        if not line.strip():
            continue
        n_fields = len(line.split(";"))
        if n_fields != header_fields:
            preview = line[:120] + ("..." if len(line) > 120 else "")
            bad.append({
                "line_num":  i,
                "expected":  header_fields,
                "found":     n_fields,
                "preview":   preview,
            })
    return bad


def read_csv_safe(raw_bytes: bytes, csv_filename: str) -> tuple:
    """
    Legge il CSV intercettando e loggando:
    - Righe malformate (numero campi diverso dall'header) con numero riga e preview
    - Byte non UTF-8 e caratteri non stampabili (clean_bytes)
    Restituisce (df, n_bad_lines).
    """
    # Step 1: pulizia byte
    clean = clean_bytes(raw_bytes, csv_filename)

    # Step 2: pre-scansione per identificare righe malformate con dettaglio
    bad_lines = scan_bad_lines(clean, csv_filename)
    if bad_lines:
        for b in bad_lines:
            log.warning(
                f"    [RIGA SKIPPATA] {csv_filename} "
                f"riga {b['line_num']}: "
                f"attesi {b['expected']} campi, trovati {b['found']} | "
                f"contenuto: {b['preview']}"
            )
        log.warning(
            f"    [RIEPILOGO] {csv_filename}: {len(bad_lines)} righe skippate "
            f"per formato non valido"
        )

    # Step 3: lettura CSV — sopprimo il ParserWarning di pandas
    # perché le righe malformate sono già state loggiate da scan_bad_lines
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pd.errors.ParserWarning)
        df = pd.read_csv(
            io.BytesIO(clean),
            sep=";",
            encoding="utf-8",
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip",  # skip silenzioso: il log lo gestiamo noi
        )

    return df, len(bad_lines)


# ---------------------------------------------------------------------------
# DDL e gestione tabella
# ---------------------------------------------------------------------------

def ensure_table(cur, schema: str, table: str,
                 raw_cols: list, key_cols: list, zip_name: str):
    fqt = f'{schema}.{q(table)}'

    col_defs = ",\n    ".join(f"{q(c)} TEXT" for c in raw_cols)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {fqt} (
            {col_defs},
            "_zip_source" TEXT,
            "_loaded_at"  TIMESTAMPTZ
        )
    """)

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

    if key_cols:
        safe_name = re.sub(r"[^a-z0-9]", "_", table.lower())[:40]
        idx_name  = f"uidx_{safe_name}_key"
        key_defs  = ", ".join(q(c) for c in key_cols)
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {q(idx_name)}
            ON {fqt} ({key_defs}, "_zip_source")
        """)

    cur.execute(f'DELETE FROM {fqt} WHERE "_zip_source" = %s', (zip_name,))
    deleted = cur.rowcount
    if deleted > 0:
        log.info(f"    Rimossi {deleted} record precedenti di '{zip_name}'")


# ---------------------------------------------------------------------------
# Deduplicazione e insert
# ---------------------------------------------------------------------------

def deduplicate(df: pd.DataFrame, key_cols: list) -> tuple:
    if not key_cols:
        return df, pd.DataFrame()
    dupes_mask = df.duplicated(subset=key_cols, keep="first")
    return df[~dupes_mask].copy(), df[dupes_mask].copy()


def ingest_csv(cur, schema: str, table: str,
               df: pd.DataFrame, key_cols: list, zip_name: str) -> tuple:
    if df.empty:
        log.warning("    CSV vuoto, skip.")
        return 0, 0

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


# ---------------------------------------------------------------------------
# Elaborazione ZIP
# ---------------------------------------------------------------------------

def process_zip(zip_path: str, conn):
    zip_name   = os.path.basename(zip_path)
    total_ins  = 0
    total_disc = 0
    total_bad  = 0

    log.info(f"=== ZIP: {zip_name} ===")

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        log.info(f"  CSV trovati: {len(csv_files)}")

        for csv_filename in csv_files:
            table_name = os.path.splitext(csv_filename)[0]
            log.info(f"  -> {csv_filename}")
            log.info(f"     Tabella: {RAW_SCHEMA}.{q(table_name)}")

            with zf.open(csv_filename) as f:
                raw_bytes = f.read()

            # Lettura sicura con pulizia e intercettazione righe malformate
            df, n_bad = read_csv_safe(raw_bytes, csv_filename)

            raw_cols = [c.strip() for c in df.columns]
            df.columns = raw_cols
            key_cols = identify_keys(raw_cols)

            log.info(f"     Righe valide: {len(df)} | Colonne: {len(df.columns)}")
            log.info(f"     Chiavi: {key_cols}")
            if n_bad > 0:
                log.warning(f"     Righe skippate per formato non valido: {n_bad}")

            with conn.cursor() as cur:
                ensure_table(cur, RAW_SCHEMA, table_name,
                             raw_cols, key_cols, zip_name)
                ins, disc = ingest_csv(cur, RAW_SCHEMA, table_name,
                                       df, key_cols, zip_name)
                conn.commit()

            total_ins  += ins
            total_disc += disc
            total_bad  += n_bad

            parts = [f"inseriti={ins}"]
            if disc > 0:
                parts.append(f"duplicati scartati={disc}")
            if n_bad > 0:
                parts.append(f"righe malformate skippate={n_bad}")
            log.info(f"     {' | '.join(parts)}")

    log.info(
        f"  Riepilogo '{zip_name}': "
        f"inseriti={total_ins} | "
        f"duplicati={total_disc} | "
        f"righe_malformate={total_bad}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not os.path.isdir(INBOUND_PATH):
        raise FileNotFoundError(f"Cartella inbound non trovata: {INBOUND_PATH}")

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
