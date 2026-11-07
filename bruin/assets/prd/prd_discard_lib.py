"""
Scarti degli export e file Excel di output.

Per ogni tabella con righe escluse dagli ZIP: out_source_mdg/discarded/<tabella>_discarded.xlsx con
  - foglio "discarded": CONTROLLO, MOTIVO e le colonne del file di origine;
  - foglio "original":  le stesse righe nel formato del file di origine (da ricaricare).
Le righe sono raccolte durante l'export (add_discards) e scritte a fine export (flush_discards),
perché un xlsx non si può accodare. Nessuna tabella riceve scarti da due export diversi.
Tutti i valori sono scritti come testo (i codici mantengono gli zeri iniziali).
Non è un asset Bruin — viene importata dagli export.
"""
import os
import logging

from openpyxl import Workbook, load_workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

log = logging.getLogger(__name__)

OUTPUT_DIR    = "/project/datalake/out_source_mdg"
DISCARDED_DIR = os.path.join(OUTPUT_DIR, "discarded")
REPORT_DIR    = os.path.join(OUTPUT_DIR, "report")
DISCARD_PREFIX = ["CONTROLLO", "MOTIVO"]

_collected: dict[str, dict] = {}   # tabella -> {"cols": [...], "rows": [(controllo, motivo, riga)]}

HEADER_FONT = Font(bold=True, color="FFFFFF")
HEADER_FILL = PatternFill("solid", fgColor="1F4E78")


def text(val) -> str:
    """Valore di cella come testo: None, NaN e caratteri non ammessi da Excel diventano vuoti."""
    if val is None:
        return ""
    s = str(val)
    if s.strip() == "NaN":
        return ""
    return ILLEGAL_CHARACTERS_RE.sub("", s)


def write_xlsx(path: str, sheets: list[tuple[str, list, list]]):
    """Scrive un xlsx con i fogli (nome, intestazione, righe): intestazione in grassetto, bloccata e con filtri."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    wb = Workbook(write_only=True)
    for name, header, rows in sheets:
        ws = wb.create_sheet(name)
        widths = [len(h) for h in header]
        rows = [[text(v) for v in row] for row in rows]
        for row in rows[:500]:
            widths = [max(w, len(v)) for w, v in zip(widths, row)]
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = min(max(w + 2, 8), 60)
        ws.freeze_panes = "A2"
        if header:
            ws.auto_filter.ref = f"A1:{get_column_letter(len(header))}{len(rows) + 1}"
        cells = []
        for h in header:
            c = WriteOnlyCell(ws, value=h)
            c.font, c.fill = HEADER_FONT, HEADER_FILL
            cells.append(c)
        ws.append(cells)
        for row in rows:
            ws.append(row)
    wb.save(path)


def read_xlsx(path: str, sheet: str | None = None) -> tuple[list, list]:
    """(intestazione, righe) di un foglio (il primo se sheet è None)."""
    wb = load_workbook(path, read_only=True)
    try:
        ws = wb[sheet] if sheet else wb.worksheets[0]
        rows = [["" if v is None else str(v) for v in r] for r in ws.iter_rows(values_only=True)]
    finally:
        wb.close()
    return (rows[0], rows[1:]) if rows else ([], [])


def discarded_path(table: str) -> str:
    safe = table.replace("#", "_").replace("/", "_")
    return os.path.join(DISCARDED_DIR, f"{safe}_discarded.xlsx")


def reset_discards(tables: list[str]):
    """
    A inizio export: cancella i file di scarto delle tabelle gestite (anche quelli CSV delle versioni
    precedenti), altrimenti resterebbero gli scarti di run precedenti se oggi non ce ne sono.
    """
    for table in tables:
        path = discarded_path(table)
        for p in (path, path[:-len(".xlsx")] + ".csv"):
            if os.path.exists(p):
                os.remove(p)
                log.info("  [DISCARDED] rimosso file di scarto precedente: %s", p)


def add_discards(table: str, cols: list, rows: list, check_id: str, reason):
    """
    Registra righe escluse dall'output. reason: testo del motivo, oppure funzione (riga) -> testo
    quando il motivo dipende dalla riga.
    """
    if not rows:
        return
    entry = _collected.setdefault(table, {"cols": list(cols), "rows": []})
    for row in rows:
        entry["rows"].append((check_id, reason(row) if callable(reason) else reason, tuple(row)))
    log.warning("  [DISCARDED] %s: %d righe escluse (%s: %s)", table, len(rows), check_id,
                reason if not callable(reason) else "motivo per riga")


def flush_discards():
    """A fine export: scrive un <tabella>_discarded.xlsx per ogni tabella con righe escluse."""
    for table, entry in _collected.items():
        cols, rows = entry["cols"], entry["rows"]
        path = discarded_path(table)
        write_xlsx(path, [
            ("discarded", DISCARD_PREFIX + cols, [[c, m, *r] for c, m, r in rows]),
            ("original", cols, [r for _, _, r in rows]),
        ])
        log.warning("  [DISCARDED] %s: %d righe scritte in %s", table, len(rows), path)
    _collected.clear()


def count_discarded(table: str) -> int:
    """Righe del foglio "discarded" del file di scarto della tabella (0 se il file non c'è)."""
    path = discarded_path(table)
    if not os.path.exists(path):
        return 0
    return len(read_xlsx(path, "discarded")[1])
