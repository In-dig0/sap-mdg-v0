"""
Nome degli ZIP di output di MDG: <nome del file in ingresso>_MDGOUT_<YYYYMMDD_HHMI>.zip, con data
e ora di avvio del run (es. DM11_BOM.zip -> DM11_BOM_MDGOUT_20261107_2129.zip): tutti gli ZIP di
un run hanno lo stesso suffisso.
Per ogni flusso resta solo lo ZIP dell'ultimo run: prima di scriverlo vengono rimossi quelli
dello stesso flusso di run precedenti, compresi quelli con i nomi delle versioni precedenti
(_MDG-OUT_YYYYMMDD o senza suffisso).
Opzioni del run (pagina Gestione pipeline, salvate in stg.pipeline_runs da setup.pipeline_run_open):
estrazione "full" (opt_full: non esclusi gli oggetti già in SAP) e codice originale
(opt_original_codes: BP senza trascodifica nel codice SAP). Con le opzioni attive il nome ha
un'etichetta dopo _MDGOUT_ (es. DM11_BOM_MDGOUT_FULL_20261120_0900.zip, _MDGOUT_ORIG_,
_MDGOUT_FULL_ORIG_), per non confonderlo con i file da caricare in SAP.
I file Excel per SAP (GruppoProdotti, Clienti_*, Fornitori_ContoLavoro) vanno nella sottocartella
excel/ di out_source_mdg (excel_output_path); quelli delle versioni precedenti nella cartella
principale vengono rimossi.
Non è un asset Bruin — viene importata dagli export.
"""
import os
import sys
import re
from datetime import datetime
from functools import lru_cache

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from mdg_env import DB_CONFIG, OUT_MDG  # noqa: E402
PIPELINE_NAME = "mdg_migration_pipeline"   # righe di stg.pipeline_runs scritte dalla pipeline
STAMP_FORMAT  = "%Y%m%d_%H%M"

OUT_SUFFIX = "_MDGOUT_"
OUT_NAME_RE = re.compile(r"^(?P<stem>.+)(_MDGOUT_(?:[A-Z]+_)*\d{8}_\d{4}|_MDG-OUT_\d{8})\.(zip|xlsx)$",
                         re.IGNORECASE)
# Opzioni del run: colonna di stg.pipeline_runs -> etichetta nel nome dei file di output
RUN_OPTIONS = {"opt_full": "FULL", "opt_original_codes": "ORIG"}


@lru_cache(maxsize=1)
def run_options() -> dict:
    """Opzioni del run in corso ({colonna: bool}); tutte False se non disponibili (run da terminale)."""
    options = dict.fromkeys(RUN_OPTIONS, False)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT {", ".join(RUN_OPTIONS)} FROM stg.pipeline_runs
                    WHERE pipeline_name = %s AND status = 'running'
                    ORDER BY started_at DESC LIMIT 1
                """, (PIPELINE_NAME,))
                row = cur.fetchone()
        finally:
            conn.close()
        if row:
            options = {col: bool(v) for col, v in zip(RUN_OPTIONS, row)}
    except psycopg2.Error:
        pass
    return options


def full_extraction() -> bool:
    """Estrazione "full": gli oggetti già presenti in SAP non sono esclusi dall'output."""
    return run_options()["opt_full"]


def original_codes() -> bool:
    """Codice originale: i BP restano con il codice del vecchio ERP (nessuna trascodifica nel codice SAP)."""
    return run_options()["opt_original_codes"]


def run_options_label() -> str:
    """Etichetta delle opzioni attive nel nome dei file (es. "FULL_ORIG_"), vuota senza opzioni."""
    return "".join(f"{tag}_" for col, tag in RUN_OPTIONS.items() if run_options()[col])


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
    """
    Nome del file di output per source_name, con data e ora di avvio del run; stessa estensione
    di source_name (ZIP per SAP, oppure ad es. GruppoProdotti.xlsx).
    """
    ext = os.path.splitext(source_name)[1].lower() or ".zip"
    return f"{zip_stem(source_name)}{OUT_SUFFIX}{run_options_label()}{run_stamp()}{ext}"


def remove_previous_zips(directory: str, source_name: str) -> list[str]:
    """Rimuove dalla cartella i file di output dello stesso flusso e tipo (con qualunque data o senza suffisso)."""
    stem, removed = zip_stem(source_name), []
    ext = os.path.splitext(source_name)[1].lower() or ".zip"
    if not os.path.isdir(directory):
        return removed
    for name in os.listdir(directory):
        if name.lower().endswith(ext) and zip_stem(name) == stem:
            os.remove(os.path.join(directory, name))
            removed.append(name)
    return removed


OUTPUT_ROOT = OUT_MDG
EXCEL_DIR   = os.path.join(OUTPUT_ROOT, "excel")   # file Excel per SAP, separati dagli ZIP


def remove_previous_excel(name: str) -> list[str]:
    """Rimuove il file Excel di output name di run precedenti, in excel/ e nella cartella principale."""
    return remove_previous_zips(EXCEL_DIR, name) + remove_previous_zips(OUTPUT_ROOT, name)


def excel_output_path(name: str) -> str:
    """Percorso del file Excel di output in out_source_mdg/excel, dopo aver rimosso le versioni precedenti."""
    os.makedirs(EXCEL_DIR, exist_ok=True)
    remove_previous_excel(name)
    return os.path.join(EXCEL_DIR, output_zip_name(name))


def output_zip_path(directory: str, source_name: str) -> str:
    """Percorso dello ZIP di output, dopo aver rimosso le versioni precedenti dello stesso flusso."""
    remove_previous_zips(directory, source_name)
    return os.path.join(directory, output_zip_name(source_name))
