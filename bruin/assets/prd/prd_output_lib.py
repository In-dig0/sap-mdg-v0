"""
Nome degli ZIP di output di MDG: <nome del file in ingresso>_MDGOUT_<YYYYMMDD_HHMI>.zip, con data
e ora di avvio del run (es. DM11_BOM.zip -> DM11_BOM_MDGOUT_20261107_2129.zip): tutti gli ZIP di
un run hanno lo stesso suffisso.
Per ogni flusso resta solo lo ZIP dell'ultimo run: prima di scriverlo vengono rimossi quelli
dello stesso flusso di run precedenti, compresi quelli con i nomi delle versioni precedenti
(_MDG-OUT_YYYYMMDD o senza suffisso).
Non è un asset Bruin — viene importata dagli export.
"""
import os
import re
from datetime import datetime
from functools import lru_cache

import psycopg2

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}
PIPELINE_NAME = "mdg_migration_pipeline"   # righe di stg.pipeline_runs scritte dalla pipeline
STAMP_FORMAT  = "%Y%m%d_%H%M"

OUT_SUFFIX = "_MDGOUT_"
OUT_NAME_RE = re.compile(r"^(?P<stem>.+)(_MDGOUT_\d{8}_\d{4}|_MDG-OUT_\d{8})\.zip$", re.IGNORECASE)


@lru_cache(maxsize=1)
def run_stamp() -> str:
    """Data e ora di avvio del run in corso (stg.pipeline_runs), YYYYMMDD_HHMI; ora attuale se non disponibile."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT started_at FROM stg.pipeline_runs
                    WHERE pipeline_name = %s AND status = 'running'
                    ORDER BY started_at DESC LIMIT 1
                """, (PIPELINE_NAME,))
                row = cur.fetchone()
        finally:
            conn.close()
        if row and row[0]:
            return row[0].strftime(STAMP_FORMAT)
    except psycopg2.Error:
        pass
    return datetime.now().strftime(STAMP_FORMAT)


def zip_stem(name: str) -> str:
    """Nome del flusso senza estensione e senza suffisso MDGOUT (es. DM11_BOM)."""
    m = OUT_NAME_RE.match(name)
    return m.group("stem") if m else os.path.splitext(name)[0]


def output_zip_name(source_name: str) -> str:
    """Nome dello ZIP di output per il file in ingresso source_name, con data e ora di avvio del run."""
    return f"{zip_stem(source_name)}{OUT_SUFFIX}{run_stamp()}.zip"


def remove_previous_zips(directory: str, source_name: str) -> list[str]:
    """Rimuove dalla cartella gli ZIP di output dello stesso flusso (con qualunque data o senza suffisso)."""
    stem, removed = zip_stem(source_name), []
    if not os.path.isdir(directory):
        return removed
    for name in os.listdir(directory):
        if name.lower().endswith(".zip") and zip_stem(name) == stem:
            os.remove(os.path.join(directory, name))
            removed.append(name)
    return removed


def output_zip_path(directory: str, source_name: str) -> str:
    """Percorso dello ZIP di output, dopo aver rimosso le versioni precedenti dello stesso flusso."""
    remove_previous_zips(directory, source_name)
    return os.path.join(directory, output_zip_name(source_name))
