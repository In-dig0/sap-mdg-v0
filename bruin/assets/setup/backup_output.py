""" @bruin
name: setup.backup_output
type: python
depends:
  - setup.pipeline_run_open   # se il run viene rifiutato (altro run in corso) non si tocca nulla
description: >
  Backup dell'output del run precedente, prima che la pipeline lo riscriva o lo cancelli.
  Per ogni cartella di uscita con almeno un file (out_source_mdg e le sottocartelle discarded,
  report, test) crea in out_source_mdg/backup/ uno ZIP con i file della cartella (non le
  sottocartelle): BKP_<cartella>_YYYYMMDD_HHMI.zip, con data e ora di avvio del run che ha
  prodotto i file (lo stesso suffisso dei suoi ZIP _MDGOUT_): l'ultimo run di stg.pipeline_runs
  avviato prima della modifica più recente dei file; se non c'è, la data di modifica dei file.
  Conserva i backup degli ultimi 10 run e cancella quelli più vecchi.
@bruin """

import os
import re
import zipfile
from datetime import datetime

import psycopg2

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}
PIPELINE_NAME = "mdg_migration_pipeline"

OUTPUT_DIR = "/project/datalake/out_source_mdg"
BACKUP_DIR = os.path.join(OUTPUT_DIR, "backup")
# cartella -> nome nel file di backup
FOLDERS = {
    OUTPUT_DIR:                           "out_source_mdg",
    os.path.join(OUTPUT_DIR, "discarded"): "discarded",
    os.path.join(OUTPUT_DIR, "report"):    "report",
    os.path.join(OUTPUT_DIR, "test"):      "test",
}
KEEP_RUNS = 10
# YYYYMMDD_HHMI (anche YYYY-MM-DD_HHMI delle versioni precedenti)
BACKUP_RE = re.compile(r"^BKP_.+_(?P<ts>\d{4}-?\d{2}-?\d{2}_\d{4})\.zip$")


def folder_files(folder: str) -> list[str]:
    if not os.path.isdir(folder):
        return []
    return sorted(f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f)))


def output_stamp() -> str | None:
    """
    Data e ora (YYYYMMDD_HHMI) del run che ha prodotto i file di uscita: avvio dell'ultimo run
    della pipeline iniziato prima della modifica più recente dei file. None se non ci sono file.
    """
    mtimes = [os.path.getmtime(os.path.join(folder, f)) for folder in FOLDERS for f in folder_files(folder)]
    if not mtimes:
        return None
    last_write = datetime.fromtimestamp(max(mtimes))
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(started_at) FROM stg.pipeline_runs
                    WHERE pipeline_name = %s AND started_at <= %s
                """, (PIPELINE_NAME, last_write))
                started = cur.fetchone()[0]
        finally:
            conn.close()
        if started:
            return started.strftime("%Y%m%d_%H%M")
    except psycopg2.Error:
        pass
    return last_write.strftime("%Y%m%d_%H%M")


def backup_folder(folder: str, label: str, stamp: str) -> int:
    """Crea lo ZIP di backup della cartella; restituisce il numero di file salvati (0 = nessun backup)."""
    files = folder_files(folder)
    if not files:
        return 0
    os.makedirs(BACKUP_DIR, exist_ok=True)
    path = os.path.join(BACKUP_DIR, f"BKP_{label}_{stamp}.zip")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in files:
            zf.write(os.path.join(folder, name), arcname=name)
    print(f"  [BACKUP] {label}: {len(files)} file -> {path} ({os.path.getsize(path) / 1048576:.1f} MB)")
    return len(files)


def remove_old_backups():
    """Conserva i backup degli ultimi KEEP_RUNS run (un run = una data e ora)."""
    if not os.path.isdir(BACKUP_DIR):
        return
    by_run = {}
    for name in os.listdir(BACKUP_DIR):
        m = BACKUP_RE.match(name)
        if m:
            by_run.setdefault(m.group("ts").replace("-", ""), []).append(name)
    for stamp in sorted(by_run)[:-KEEP_RUNS]:
        for name in by_run[stamp]:
            os.remove(os.path.join(BACKUP_DIR, name))
        print(f"  [BACKUP] rimosso il backup del run {stamp} (conservati gli ultimi {KEEP_RUNS})")


def main():
    stamp = output_stamp()
    if not stamp:
        print("[OK] Nessun file nelle cartelle di uscita — nessun backup")
    else:
        for folder, label in FOLDERS.items():
            backup_folder(folder, label, stamp)
    remove_old_backups()


if __name__ == "__main__":
    main()
