""" @bruin
name: prd.export_materiali_zip
type: python
description: >
  Genera gli archivi ZIP di output per i materiali: uno per ogni file di
  origine (_source) presente in raw.S_MARA, con lo stesso nome del file in
  ingresso (come gli export di distinte base, cicli, inforecord).
  Tutte le tabelle vengono lette dallo schema raw — nessuna esclusione di record
  orfani (i check CK segnalano le anomalie nel report qualita, ma non bloccano la
  scrittura dell'output).
  Materiali già migrati in SAP: i materiali presenti in ref.SAP_EXPORT_MARC (confronto
  per MATNR, senza spazi, in qualunque divisione) sono esclusi da tutte le tabelle;
  riepilogo in report/<nome ZIP>_gia_in_SAP.xlsx (righe escluse per tabella ed elenco dei
  materiali). Non esclusi con l'estrazione "full" della pagina Gestione pipeline o se
  SAP_EXPORT_MARC non è caricata.
  CK016 (tipo materiale obbligatorio e presente in SAP_EXPORT_T134): con l'esclusione dall'output
  attiva nel catalogo (default disattivata) i materiali segnalati sono esclusi da tutte le tabelle
  dello ZIP e scritti in discarded/<tabella>_discarded.xlsx (solo quelli non già in SAP).
  CK527 (distinte base con materiale padre o componente non in SAP): con l'esclusione dall'output
  attiva nel catalogo le distinte non sono esportate (prd.export_bom_zip) e qui il loro materiale
  padre esce con lo stato "08" (materiale incompleto): S_MARA.MSTAE e S_MVKE.VMSTA su tutto il
  materiale, S_MARC.MMSTA solo nella divisione della distinta. Solo nel file per SAP (raw invariata);
  elenco in report/<nome ZIP>_stato_08_CK527.xlsx. Riguarda solo i padri esportati, cioè non già in
  SAP_EXPORT_MARC (salvo estrazione "full").
  Il CSV usa separatore ";" e non include colonne audit.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
  Se non ci sono materiali non viene creato nulla. Il vecchio file a nome fisso
  DM08_MATERIALI.zip viene rimosso se ancora presente.
depends:
  - ingestion.ingest_zip_to_raw   # prima girava a inizio pipeline, su raw non ancora caricata
  - ingestion.ingest_xlsx_to_ref  # SAP_EXPORT_MARC: materiali già migrati in SAP
  - stg.ck016_mara_mtart          # esclusione dall'output dei materiali con tipo materiale non valido
  - stg.ck527_bom_materiale_in_sap_marc  # stato 08 sui padri delle distinte escluse
@bruin """

import os
import io
import csv
import zipfile
import logging
import math
import sys
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from prd_output_lib import output_zip_path, full_extraction
from prd_discard_lib import REPORT_DIR, write_xlsx, add_discards, flush_discards, reset_discards
from prd_merge_lib import exclusion_enabled, failing_keys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from mdg_env import DB_CONFIG, OUT_MDG  # noqa: E402

OUTPUT_DIR  = OUT_MDG
MASTER_TABLE = "S_MARA"                    # i file di origine si leggono da qui
LEGACY_OUTPUT = "DM08_MATERIALI.zip"       # vecchio nome fisso, non più generato
AUDIT_COLS  = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}
SAP_MARC    = "SAP_EXPORT_MARC"            # materiali già migrati in SAP (MATNR)
MATERIAL_COLS = ("PRODUCT(k/*)", "MATNR(k/*)", "MATNR")   # colonna del materiale nelle tabelle
EXCLUSION_CHECKS = ["CK016"]               # check con esclusione dall'output dei materiali
# Stato "materiale incompleto" forzato sui padri delle distinte escluse da CK527:
# tabella -> (colonna dello stato, colonna della divisione o None = tutto il materiale)
FORCED_STATUS = "08"
STATUS_COLS = {"S_MARA": ("MSTAE", None), "S_MARC": ("MMSTA", "WERKS(k/*)"), "S_MVKE": ("VMSTA", None)}

RAW_TABLES = [
    "S_MARA",
    "S_MAKT",
    "S_MARC",
    "S_MARD",
    "S_MARM",
    "S_MBEW",
    "S_MLAN",
    "S_MVKE",
    "S_QMAT",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn):
    with conn.cursor() as cur:
        cur.execute(f'SELECT DISTINCT "_source" FROM raw.{q(MASTER_TABLE)} WHERE "_source" IS NOT NULL ORDER BY 1')
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'raw' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def table_exists(conn, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables"
            " WHERE table_schema='raw' AND table_name=%s", (table,)
        )
        return cur.fetchone() is not None


def material_col(cols):
    return next((c for c in MATERIAL_COLS if c in cols), None)


def load_sap_materials(conn) -> bool:
    """
    Tabella temporanea sap_mat con i materiali già in SAP (MATNR di ref.SAP_EXPORT_MARC).
    False (nessuna esclusione) con l'estrazione "full" o se SAP_EXPORT_MARC non è caricata.
    """
    if full_extraction():
        log.info("Estrazione full: materiali già in SAP (%s) non esclusi", SAP_MARC)
        return False
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"ref.{q(SAP_MARC)}",))
        if cur.fetchone()[0] is None:
            log.warning("ref.%s non caricata — materiali già in SAP non esclusi", SAP_MARC)
            return False
        cur.execute(f"""CREATE TEMP TABLE sap_mat (matnr TEXT PRIMARY KEY) ON COMMIT PRESERVE ROWS;
                        INSERT INTO sap_mat SELECT DISTINCT TRIM("MATNR") FROM ref.{q(SAP_MARC)}
                        WHERE COALESCE(TRIM("MATNR"), '') <> '';
                        ANALYZE sap_mat""")
    return True


def load_check_exclusions(conn) -> dict:
    """
    Materiali da escludere per i check con esclusione dall'output attiva (EXCLUSION_CHECKS):
    tabella temporanea ck_mat (matnr, check_id, message). Restituisce {check_id: n. materiali}.
    """
    counts = {}
    with conn.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE IF NOT EXISTS ck_mat (matnr TEXT, check_id TEXT, message TEXT)
                       ON COMMIT PRESERVE ROWS; TRUNCATE ck_mat""")
        for check_id in EXCLUSION_CHECKS:
            if not exclusion_enabled(cur, check_id):
                continue
            cur.execute("""
                INSERT INTO ck_mat
                SELECT DISTINCT ON (TRIM(object_key)) TRIM(object_key), check_id, message
                FROM stg.check_results
                WHERE check_id = %s AND status <> 'Ok'
                  AND run_id = (SELECT MAX(run_id) FROM stg.check_results WHERE check_id = %s)
                ORDER BY TRIM(object_key)
            """, (check_id, check_id))
            counts[check_id] = cur.rowcount
            if cur.rowcount:
                log.warning("  *** [%s] %d materiali ESCLUSI dall'output (esclusione attiva nel catalogo)",
                            check_id, cur.rowcount)
        cur.execute("ANALYZE ck_mat")
    return counts


def discard_check_exclusions(conn, table, cols, source_name, exclude_sap=False):
    """Righe dei materiali esclusi dai check (non già in SAP) negli scarti della tabella."""
    mat = material_col(cols)
    if not mat:
        return
    not_sap = (f" AND NOT EXISTS (SELECT 1 FROM sap_mat s WHERE s.matnr = TRIM(x.{q(mat)}))"
               if exclude_sap else "")
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT c.check_id, c.message, {", ".join("x." + q(c) for c in cols)} '
            f'FROM raw.{q(table)} x JOIN ck_mat c ON c.matnr = TRIM(x.{q(mat)}) '
            f'WHERE x."_source" = %s{not_sap} ORDER BY 3',
            (source_name,))
        rows = cur.fetchall()
    for check_id in sorted({r[0] for r in rows}):
        sel = [r for r in rows if r[0] == check_id]
        messages = {tuple(r[2:]): r[1] for r in sel}
        add_discards(table, cols, [list(r[2:]) for r in sel], check_id,
                     lambda row, m=messages: m.get(tuple(row), ""))


def load_ck527_parents(conn) -> set:
    """
    (materiale, divisione) padri delle distinte segnalate da CK527 (testata o posizione), se
    l'esclusione dall'output di CK527 è attiva: le distinte non sono esportate e il padre esce
    con lo stato FORCED_STATUS. Chiave dell'esito: MATNR|WERKS|STLAN|STLAL[|ITMID].
    """
    with conn.cursor() as cur:
        if not exclusion_enabled(cur, "CK527"):
            return set()
        parents = set()
        for key in failing_keys(cur, "CK527"):
            parts = key.split("|")
            if len(parts) >= 2 and parts[0].strip():
                parents.add((parts[0].strip(), parts[1].strip()))
    log.info("  [CK527] %d materiali padre di distinte escluse: stato %s nel file per SAP",
             len({m for m, _ in parents}), FORCED_STATUS)
    return parents


def force_status(table, cols, rows, parents, changed):
    """Stato FORCED_STATUS sulle righe dei materiali padre (STATUS_COLS); changed riceve le modifiche."""
    if not parents or table not in STATUS_COLS:
        return rows
    status_col, plant_col = STATUS_COLS[table]
    mat = material_col(cols)
    if not mat or status_col not in cols or (plant_col and plant_col not in cols):
        return rows
    i_mat, i_st = cols.index(mat), cols.index(status_col)
    i_pl = cols.index(plant_col) if plant_col else None
    materials = {m for m, _ in parents}
    out = []
    for row in rows:
        m = (row[i_mat] or "").strip()
        hit = (m, (row[i_pl] or "").strip()) in parents if plant_col else m in materials
        if hit and (row[i_st] or "").strip() != FORCED_STATUS:
            row = list(row)
            changed.append([table, m, (row[i_pl] or "").strip() if plant_col else "", status_col,
                            row[i_st] or "", FORCED_STATUS])
            row[i_st] = FORCED_STATUS
        out.append(row)
    return out


def write_forced_status(source_name, changed):
    """report/<ZIP>_stato_08_CK527.xlsx: stati forzati sui padri delle distinte escluse da CK527."""
    path = os.path.join(REPORT_DIR, f"{os.path.splitext(source_name)[0]}_stato_08_CK527.xlsx")
    if not changed:
        if os.path.exists(path):
            os.remove(path)
        return
    os.makedirs(REPORT_DIR, exist_ok=True)
    write_xlsx(path, [("stato_08", ["TABELLA", "MATERIALE", "DIVISIONE", "CAMPO", "STATO_ORIGINALE",
                                    "STATO_SAP"], changed)])
    log.warning("  [CK527] stato %s forzato su %d righe (%d materiali): %s", FORCED_STATUS,
                len(changed), len({r[1] for r in changed}), path)


def fetch_table_csv(conn, table, cols, source_name, exclude_sap=False, exclude_checks=False,
                    parents=None, changed=None):
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    mat      = material_col(cols)
    where    = ""
    if exclude_sap and mat:
        where = f" AND NOT EXISTS (SELECT 1 FROM sap_mat s WHERE s.matnr = TRIM({q(mat)}))"
    if exclude_checks and mat:
        where += f" AND NOT EXISTS (SELECT 1 FROM ck_mat c WHERE c.matnr = TRIM({q(mat)}))"
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s{where} ORDER BY 1',
            (source_name,)
        )
        rows = cur.fetchall()
    if parents and changed is not None:
        rows = force_status(table, cols, rows, parents, changed)

    def sanitize(val):
        if val is None: return ""
        if isinstance(val, float) and math.isnan(val): return ""
        if isinstance(val, str) and val.strip() == "NaN": return ""
        return val

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    return buf.getvalue().encode("utf-8")


def already_in_sap_path(source_name):
    return os.path.join(REPORT_DIR, f"{os.path.splitext(source_name)[0]}_gia_in_SAP.xlsx")


def write_already_in_sap(conn, source_name, cols_map):
    """
    report/<ZIP>_gia_in_SAP.xlsx: righe escluse per tabella (foglio gia_in_SAP, letto da
    prd.reconcile_zip) ed elenco dei materiali già in SAP (foglio materiali).
    """
    per_table, materials = [], set()
    with conn.cursor() as cur:
        for table in RAW_TABLES:
            mat = material_col(cols_map.get(table) or [])
            if not mat:
                continue
            cur.execute(f"""SELECT TRIM(x.{q(mat)}), COUNT(*) FROM raw.{q(table)} x
                            JOIN sap_mat s ON s.matnr = TRIM(x.{q(mat)})
                            WHERE x."_source" = %s GROUP BY 1""", (source_name,))
            rows = cur.fetchall()
            if rows:
                per_table.append([table, f"{len(rows)} materiali", "", sum(n for _, n in rows)])
                materials.update(m for m, _ in rows)
    path = already_in_sap_path(source_name)
    if not per_table:
        if os.path.exists(path):
            os.remove(path)
        return
    write_xlsx(path, [("gia_in_SAP", ["TABELLA", "CODICE", "CODICE_SAP", "RIGHE"], per_table),
                      ("materiali", ["MATNR"], [[m] for m in sorted(materials)])])
    log.info("  [GIA' IN SAP] %d materiali di '%s' già presenti in %s, esclusi dallo ZIP: %s",
             len(materials), source_name, SAP_MARC, path)


def build_zip(conn, source_name, cols_map, exclude_sap=False, exclude_checks=False,
              parents=None, changed=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in RAW_TABLES:
            cols = cols_map.get(table)
            if not cols:
                log.warning("  SKIP raw.%s — tabella non trovata o senza colonne", table)
                continue
            if exclude_checks:
                discard_check_exclusions(conn, table, cols, source_name, exclude_sap)
            csv_bytes = fetch_table_csv(conn, table, cols, source_name, exclude_sap, exclude_checks,
                                        parents, changed)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe — skip", table)
                continue
            zf.writestr(f"{table}.csv", csv_bytes)
            log.info("  + raw.%s: %d righe", table, n_rows)
    buf.seek(0)
    return buf.read()


def remove_legacy_output(sources):
    """Rimuove il vecchio DM08_MATERIALI.zip a nome fisso (spesso vuoto), se non è più un output attuale."""
    path = os.path.join(OUTPUT_DIR, LEGACY_OUTPUT)
    if LEGACY_OUTPUT not in sources and os.path.exists(path):
        os.remove(path)
        log.warning("  Rimosso %s (vecchio output a nome fisso).", path)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reset_discards(RAW_TABLES)
    conn = get_connection()
    try:
        if not table_exists(conn, MASTER_TABLE):
            log.warning("raw.%s non trovata — export annullato.", MASTER_TABLE)
            remove_legacy_output([])
            return
        sources = get_distinct_sources(conn)
        remove_legacy_output(sources)
        if not sources:
            log.info("Nessun materiale in raw.%s — nessuno ZIP da generare.", MASTER_TABLE)
            return

        cols_map = {}
        for table in RAW_TABLES:
            if table_exists(conn, table):
                cols = get_columns(conn, table)
                if cols:
                    cols_map[table] = cols
                else:
                    log.warning("  raw.%s — nessuna colonna, verra omessa.", table)
            else:
                log.warning("  raw.%s — tabella non trovata, verra omessa.", table)

        exclude_sap = load_sap_materials(conn)
        exclude_checks = any(load_check_exclusions(conn).values())
        parents = load_ck527_parents(conn)
        log.info("Sorgenti trovate: %s", sources)
        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)
            if exclude_sap:
                write_already_in_sap(conn, source_name, cols_map)
            elif os.path.exists(already_in_sap_path(source_name)):
                os.remove(already_in_sap_path(source_name))
            changed     = []
            zip_bytes   = build_zip(conn, source_name, cols_map, exclude_sap, exclude_checks,
                                    parents, changed)
            write_forced_status(source_name, changed)
            output_path = output_zip_path(OUTPUT_DIR, source_name)
            if not zipfile.ZipFile(io.BytesIO(zip_bytes)).namelist():
                log.warning("  ✗  %s: nessun materiale da esportare (tutti già in SAP) — ZIP non generato",
                            source_name)
                continue
            with open(output_path, "wb") as f:
                f.write(zip_bytes)
            log.info("  ✓  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)
        flush_discards()
    finally:
        conn.close()
    log.info("=== export_materiali_zip completato ===")


if __name__ == "__main__":
    main()
