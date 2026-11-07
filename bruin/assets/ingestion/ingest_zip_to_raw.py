""" @bruin
name: ingestion.ingest_zip_to_raw
type: python
depends:
  - ingestion.ingest_xlsx_to_ref
description: >
  Legge tutti i file ZIP da /project/datalake/in_source_pprod/,
  estrae i CSV e li carica nello schema raw di PostgreSQL.
  Nome tabella = nome file CSV senza estensione e senza sottocartelle dello ZIP
  (virgolette doppie); le banche fornitori
  sono sempre caricate come S_SUPP_BANK#..., anche se l'ERP usa il nome SAP S_SUPPL_BANK#...
  (TABLE_PREFIX; l'export dei fornitori riscrive il nome SAP nello ZIP).
  Nomi colonne = intestazioni originali invariate (virgolette doppie).
  Colonne con (k) nel suffisso → UNIQUE INDEX sulla tabella.
  Duplicati su chiave: scartati e loggati prima dell'insert.
  Righe malformate (campi in eccesso): skippate e loggatecon numero riga.
  Codifica: UTF-8; i file non UTF-8 validi sono letti come Windows-1252 (es. 'Ø' nelle
  descrizioni materiali) e loggati. In raw, e quindi negli export, il testo è UTF-8.
  Caratteri non stampabili: rimossi e loggati.
  Codice materiale senza spazi iniziali e finali (TRIM_COLS): materiali DM03A, codici
  doganali S_MARITC, distinte base, info record acquisti. Il numero di valori corretti è loggato.
  Righe con un numero di campi diverso dall'intestazione (quelle con campi in eccesso sono
  scartate, quelle con campi mancanti caricate con i valori vuoti in coda) e duplicati sono
  anche scritti in
  out_source_mdg/discarded/ingestione_scarti.csv (ZIP;CSV;TABELLA;RIGA;CHIAVE;MOTIVO;CONTENUTO),
  riscritto a ogni run (rimosso se non ci sono scarti).
  Colonne audit: _source TEXT, _loaded_at TIMESTAMPTZ.
  Strategia: DELETE selettivo per _source + INSERT.
@bruin """

import os
import re
import csv
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

INBOUND_PATH = "/project/datalake/in_source_pprod"
REJECTS_FILE = "/project/datalake/out_source_mdg/discarded/ingestione_scarti.csv"
REJECTS_HEADER = ["ZIP", "CSV", "TABELLA", "RIGA", "CHIAVE", "MOTIVO", "CONTENUTO"]
REJECTS: list[list] = []   # righe scartate in ingestione, scritte in REJECTS_FILE a fine run
RAW_SCHEMA   = "raw"

# Prefissi del nome CSV ricondotti al nome tabella usato da MDG: l'ERP chiama le banche
# fornitori S_SUPP_BANK#... (struttura SAP: S_SUPPL_BANK); se corregge il nome, le tabelle
# restano le stesse
TABLE_PREFIX = {"S_SUPPL_BANK#": "S_SUPP_BANK#"}

# Colonne con il codice materiale da caricare senza spazi iniziali e finali (l'ERP
# riempie il codice con spazi fino a 20 caratteri): tabella raw -> colonne
MATERIAL_TABLES = ["S_MARA", "S_MAKT", "S_MARC", "S_MARD", "S_MARM", "S_MBEW", "S_MLAN",
                   "S_MVKE", "S_QMAT", "S_CLF_MAT_AUSP_CHAR", "S_CLF_MAT_AUSP_NUM",
                   "S_CLF_MAT_KSSK_HEAD"]                         # DM03A_MATERIALI
TRIM_COLS = {
    **{t: ["PRODUCT(k/*)"] for t in MATERIAL_TABLES},
    "S_MARITC":           ["MATNR(k/*)"],                         # DM03C codici doganali
    "S_BOM_HEADER":       ["MATNR(k/*)"],                         # distinte base
    "S_BOM_ITEM":         ["MATNR(k/*)", "IDNRK"],
    "S_EINA#INFORMATFOR": ["MATNR"],                              # info record acquisti
}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def table_name_for(csv_filename: str) -> str:
    """
    Nome della tabella raw per un CSV: nome file senza estensione e senza le eventuali
    sottocartelle dello ZIP (es. DM03A_MATERIALI/S_MARA.csv -> S_MARA), prefissi in TABLE_PREFIX.
    """
    name = os.path.splitext(os.path.basename(csv_filename))[0]
    for prefix, table_prefix in TABLE_PREFIX.items():
        if name.startswith(prefix):
            return table_prefix + name[len(prefix):]
    return name


def trim_columns(df: pd.DataFrame, table: str) -> None:
    """Toglie gli spazi iniziali e finali dalle colonne TRIM_COLS della tabella (in place)."""
    for col in TRIM_COLS.get(table, []):
        if col not in df.columns:
            continue
        trimmed = df[col].str.strip()
        n = int((trimmed != df[col]).sum())
        df[col] = trimmed
        if n:
            log.info(f"     [TRIM] {col}: tolti spazi iniziali/finali da {n} valori")


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
    1. Decodifica con UTF-8; se il file non è UTF-8 valido lo decodifica come
       Windows-1252 (codifica dei file dell'ERP con caratteri speciali, es. 'Ø').
       I byte non definiti in Windows-1252 diventano U+FFFD (invece di crashare).
    2. Rimuove caratteri di controllo non stampabili (eccetto
       tab, newline, carriage return che sono legittimi nei CSV).
    3. Logga la codifica usata e quante sostituzioni sono avvenute.
    """
    try:
        text_raw = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text_raw = raw_bytes.decode("cp1252", errors="replace")
        n_special = sum(1 for b in raw_bytes if b >= 0x80)
        log.warning(
            f"    [CODIFICA] {csv_filename}: non UTF-8, letto come Windows-1252 "
            f"({n_special} caratteri non ASCII convertiti in UTF-8)"
        )
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

    # Controlla se ci sono stati replacement U+FFFD (byte non decodificabili)
    replacements = text_clean.count("\ufffd")
    if replacements > 0:
        log.warning(
            f"    [PULIZIA] {csv_filename}: {replacements} byte non decodificabili "
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


def read_csv_safe(raw_bytes: bytes, csv_filename: str, zip_name: str = "", table: str = "") -> tuple:
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
        # pandas scarta le righe con campi in eccesso, ma carica quelle con campi mancanti
        # completandole con valori vuoti: i valori successivi risultano spostati di colonna
        REJECTS.extend(
            [zip_name, csv_filename, table, b["line_num"], "",
             (f"Scartata: attesi {b['expected']} campi, trovati {b['found']}" if b["found"] > b["expected"] else
              f"Caricata con campi mancanti: attesi {b['expected']}, trovati {b['found']} "
              f"(i valori possono risultare spostati di colonna)"),
             b["preview"]]
            for b in bad_lines
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
            "_source" TEXT,
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
    for col, ctype in [("_source", "TEXT"), ("_loaded_at", "TIMESTAMPTZ")]:
        if col not in existing:
            cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS {q(col)} {ctype}")

    if key_cols:
        safe_name = re.sub(r"[^a-z0-9]", "_", table.lower())[:40]
        idx_name  = f"uidx_{safe_name}_key"
        key_defs  = ", ".join(q(c) for c in key_cols)
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {q(idx_name)}
            ON {fqt} ({key_defs}, "_source")
        """)

    cur.execute(f'DELETE FROM {fqt} WHERE "_source" = %s', (zip_name,))
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
            REJECTS.append([zip_name, "", table, "",
                            " | ".join(f"{k}={v}" for k, v in key_values.items()),
                            "Duplicato sulla chiave: caricata solo la prima riga", ""])

    if df_clean.empty:
        log.warning("    Nessun record da inserire dopo deduplicazione.")
        return 0, discarded

    fqt     = f'{schema}.{q(table)}'
    now_utc = datetime.now(timezone.utc)
    df_ins  = df_clean.copy()
    df_ins["_source"] = zip_name
    df_ins["_loaded_at"]  = now_utc

    col_list = ", ".join(q(c) for c in df_ins.columns)
    # execute_values interpreta % come format character psycopg2.
    # Se nomi di tabella/colonna contengono %, vanno escaped come %%.
    # Sostituiamo tutti i % nel template TRANNE il segnaposto %s finale.
    sql_raw  = f"INSERT INTO {fqt} ({col_list}) VALUES %s"
    # Escapa i % che non sono seguiti da 's' (il placeholder VALUES %s)
    import re as _re
    sql_safe = _re.sub(r'%(?!s(?:\s|$))', '%%', sql_raw)
    rows     = [tuple(r) for r in df_ins.itertuples(index=False, name=None)]

    execute_values(cur, sql_safe, rows, page_size=500)
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
            table_name = table_name_for(csv_filename)
            log.info(f"  -> {csv_filename}")
            log.info(f"     Tabella: {RAW_SCHEMA}.{q(table_name)}")

            with zf.open(csv_filename) as f:
                raw_bytes = f.read()

            # Lettura sicura con pulizia e intercettazione righe malformate
            df, n_bad = read_csv_safe(raw_bytes, csv_filename, zip_name, table_name)

            raw_cols = [c.strip() for c in df.columns]
            df.columns = raw_cols
            trim_columns(df, table_name)
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

def write_rejects():
    """Scrive (o rimuove, se non ci sono scarti) il file delle righe scartate in ingestione."""
    if os.path.exists(REJECTS_FILE):
        os.remove(REJECTS_FILE)
    if not REJECTS:
        return
    os.makedirs(os.path.dirname(REJECTS_FILE), exist_ok=True)
    with open(REJECTS_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(REJECTS_HEADER)
        writer.writerows(REJECTS)
    log.warning(f"  [SCARTI INGESTIONE] {len(REJECTS)} righe con anomalie (scartate, duplicate o caricate incomplete): {REJECTS_FILE}")


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
        write_rejects()
        return

    log.info(f"ZIP da processare: {len(zip_files)}")
    conn = get_connection()
    try:
        for zip_path in zip_files:
            process_zip(zip_path, conn)
    finally:
        conn.close()
    write_rejects()

    log.info("=== Ingestion completata ===")


if __name__ == "__main__":
    main()
