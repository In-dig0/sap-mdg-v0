""" @bruin
name: prd.export_cicli_lavoro_zip
type: python
description: >
  Genera l'archivio ZIP di output per i cicli di lavoro (DM15_CICLI): S_GROUP, S_PLKO, S_MAPL,
  S_SEQUENCE, S_OPERATION, nell'ordine del modello SAP.
  S_OPERATION, S_PLKO e S_MAPL sono lette da prd (prd.merge_cicli: operazioni sostituite o
  aggiunte da stg.S_OPERATION_STG e cicli alternativi nuovi), le altre da raw; se la tabella prd
  non c'è si usa raw. Righe ordinate per le colonne chiave (PLNNR, PLNAL, PLNFL, VORNR). Nessuna esclusione di record orfani (CK408 segnala le anomalie nel report
  qualita, ma non blocca la scrittura dell'output).
  CK537: se attivo con il flag "Escludi dall'output" (default disattivato), le operazioni con
  centro di lavoro WORK_CNTR + WERKS_WORK_CNTR non presente in ref.SAP_EXPORT_CRHD (ARBPL + WERKS)
  non sono scritte in S_OPERATION.csv e finiscono in discarded/S_OPERATION_discarded.xlsx.
  La condizione è valutata sulle righe finali (anche quelle da S_OPERATION_STG, con PLNNR già
  ricavato da S_MAPL), con la stessa regola del controllo.
  Il CSV usa separatore ";" e non include colonne audit.
  Un ZIP separato per ogni valore distinto di _source (da S_OPERATION).
  Output: /project/datalake/out_source_mdg/<_source>
depends:
  - prd.merge_cicli
  - stg.ck408_cicli_lavoro_orphans
  - stg.ck502_marc_mapl
  - stg.ck537_cicli_centro_lavoro_crhd
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
from prd_output_lib import output_zip_path
from prd_merge_lib import exclusion_enabled
from prd_discard_lib import add_discards, flush_discards, reset_discards

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

OUTPUT_DIR   = "/project/datalake/out_source_mdg"
MASTER_TABLE = "S_OPERATION"
AUDIT_COLS   = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

# Ordine tabelle nel ZIP (come nel modello SAP)
RAW_TABLES = [
    "S_GROUP",
    "S_PLKO",
    "S_MAPL",
    "S_SEQUENCE",
    "S_OPERATION",   # master
]
# Tabelle lette da prd se presenti (prd.merge_cicli), altrimenti da raw
PRD_TABLES = {"S_OPERATION", "S_PLKO", "S_MAPL"}
SCHEMA = {}   # tabella -> schema da cui leggerla, impostato in main()

# CK537: operazione con centro di lavoro indicato ma assente nell'anagrafica SAP dei centri di lavoro
CRHD = "SAP_EXPORT_CRHD"
CK537_MISSING = f"""(COALESCE(TRIM("WORK_CNTR"), '') <> '' AND NOT EXISTS (
    SELECT 1 FROM ref."{CRHD}" c
    WHERE TRIM(c."ARBPL") = TRIM("WORK_CNTR") AND TRIM(c."WERKS") = TRIM("WERKS_WORK_CNTR")))"""
EXCLUDE_CK537 = False   # impostato in main()


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn):
    """Recupera i valori distinti di _source dalla tabella master S_OPERATION."""
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT DISTINCT "_source" FROM {SCHEMA[MASTER_TABLE]}."S_OPERATION"'
            ' WHERE "_source" IS NOT NULL ORDER BY 1'
        )
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (SCHEMA.get(table, "raw"), table))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def table_exists(conn, table, schema="raw"):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables"
            " WHERE table_schema=%s AND table_name=%s", (schema, table)
        )
        return cur.fetchone() is not None


def order_by(cols) -> str:
    """Ordine delle righe: colonne chiave (k) nell'ordine del file (PLNNR, PLNAL, PLNFL, VORNR...)."""
    keys = [c for c in cols if "(k" in c] or cols[:1]
    return ", ".join(f"TRIM({q(c)})" for c in keys)


def fetch_table_csv(conn, table, source_name, cols):
    fqt      = SCHEMA.get(table, "raw") + "." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    exclude  = EXCLUDE_CK537 and table == MASTER_TABLE
    with conn.cursor() as cur:
        if exclude:
            cur.execute(
                f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s AND {CK537_MISSING} ORDER BY {order_by(cols)}',
                (source_name,)
            )
            add_discards(table, cols, cur.fetchall(), "CK537",
                         lambda r: f"Centro di lavoro [{(r[cols.index('WORK_CNTR')] or '').strip()}] nella divisione "
                                   f"[{(r[cols.index('WERKS_WORK_CNTR')] or '').strip()}] non presente in SAP "
                                   f"({CRHD}: ARBPL + WERKS)")
        cur.execute(
            f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s'
            + (f" AND NOT {CK537_MISSING}" if exclude else "")
            + f" ORDER BY {order_by(cols)}",
            (source_name,)
        )
        rows = cur.fetchall()

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


def build_zip(conn, source_name, cols_map):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in RAW_TABLES:
            cols = cols_map.get(table)
            if not cols:
                log.warning("  SKIP raw.%s — tabella non trovata o senza colonne", table)
                continue
            csv_bytes = fetch_table_csv(conn, table, source_name, cols)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe per '%s' — skip", table, source_name)
                continue
            zf.writestr(f"{table}.csv", csv_bytes)
            master_label = " [master]" if table == MASTER_TABLE else ""
            log.info("  + %s.%s%s: %d righe", SCHEMA.get(table, "raw"), table, master_label, n_rows)
    buf.seek(0)
    return buf.read()


def main():
    global EXCLUDE_CK537
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reset_discards([MASTER_TABLE])
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            EXCLUDE_CK537 = table_exists(conn, CRHD, "ref") and exclusion_enabled(cur, "CK537")
        log.info("CK537 esclusione operazioni con centro di lavoro non in SAP: %s",
                 "attiva" if EXCLUDE_CK537 else "non attiva")
        for table in RAW_TABLES:
            SCHEMA[table] = "prd" if table in PRD_TABLES and table_exists(conn, table, "prd") else "raw"
        if not table_exists(conn, MASTER_TABLE, SCHEMA[MASTER_TABLE]):
            log.warning("raw.%s non trovata — export annullato.", MASTER_TABLE)
            return

        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun _source trovato in raw.%s — nulla da esportare.", MASTER_TABLE)
            return

        # Carica colonne una volta sola per tutte le tabelle
        cols_map = {}
        for table in RAW_TABLES:
            if table_exists(conn, table, SCHEMA[table]):
                cols = get_columns(conn, table)
                if cols:
                    cols_map[table] = cols
                else:
                    log.warning("  raw.%s — nessuna colonna, verra omessa.", table)
            else:
                log.warning("  raw.%s — tabella non trovata, verra omessa.", table)

        log.info("Sorgenti trovate: %s", sources)

        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)
            zip_bytes   = build_zip(conn, source_name, cols_map)
            output_path = output_zip_path(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(zip_bytes)
            log.info("  ✓  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)

    finally:
        conn.close()
        flush_discards()

    log.info("=== export_cicli_lavoro_zip completato ===")


if __name__ == "__main__":
    main()
