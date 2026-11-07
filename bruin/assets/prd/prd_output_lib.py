"""
Nome degli ZIP di output di MDG: <nome del file in ingresso>_MDG-OUT_<YYYYMMDD>.zip, con la data
del run (es. DM11_BOM.zip -> DM11_BOM_MDG-OUT_20261107.zip).
Per ogni flusso resta solo lo ZIP dell'ultimo run: prima di scriverlo vengono rimossi quelli
dello stesso flusso con data precedente e quello con il vecchio nome senza suffisso.
Non è un asset Bruin — viene importata dagli export.
"""
import os
import re
from datetime import date

OUT_SUFFIX = "_MDG-OUT_"
OUT_NAME_RE = re.compile(r"^(?P<stem>.+)_MDG-OUT_\d{8}\.zip$", re.IGNORECASE)


def zip_stem(name: str) -> str:
    """Nome del flusso senza estensione e senza suffisso MDG-OUT (es. DM11_BOM)."""
    m = OUT_NAME_RE.match(name)
    return m.group("stem") if m else os.path.splitext(name)[0]


def output_zip_name(source_name: str) -> str:
    """Nome dello ZIP di output per il file in ingresso source_name, con la data di oggi."""
    return f"{zip_stem(source_name)}{OUT_SUFFIX}{date.today():%Y%m%d}.zip"


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
