""" @bruin
name: prd.export_ordini_acquisto_zip
type: python
description: >
  Genera l'archivio ZIP di output per gli ordini di acquisto (flusso DM27_MM-OrdAcq).
  Tabelle coinvolte (quelle caricate, nello stesso ordine del file in ingresso):
    - S_EKKO       (testate, master)
    - S_EKKO_TEXT  (testi di testata)
    - S_EKPO       (posizioni)
    - S_EKET       (schedulazioni)
    - S_COMP       (componenti di conto lavoro)
    - S_EKKN       (imputazioni)
  I contratti DM14 sono in tabelle separate (#CONTRATTI) e non sono esportati qui.
  Centro di costo: in S_EKKN il KOSTL (codice DBCOGE) è trascodificato con COD_SAP_DEF di
  ref.SAP_TRASCO_CDC (COD_DBCOGE = KOSTL, DIV = WERKS della posizione in S_EKPO).
  Le imputazioni senza trascodifica valida (record assente, COD_SAP_DEF vuoto o '??', posizione
  non trovata) sono segnalate da CK536 ed esportate con il KOSTL originale di DBCOGE.
  Nessuna esclusione: i controlli sugli ordini (CK060-062, CK222-225, CK415, CK520-524, CK529,
  CK536) sono solo segnalazioni.
  Il CSV usa separatore ";" e non include colonne audit.
  Un ZIP separato per ogni valore distinto di _source.
  Output: /project/datalake/out_source_mdg/<_source>_MDGOUT_<YYYYMMDD_HHMI>.zip
depends:
  - stg.ck536_po_kostl_trasco_cdc
  - ingestion.ingest_xlsx_to_ref
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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stg"))
from prd_output_lib import output_zip_path
from ck_query_lib import po_cdc_sql, PO_ACCOUNT_TABLE, CDC_TRASCO

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
AUDIT_COLS   = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}
MASTER_TABLE = "S_EKKO"
RAW_TABLES   = ["S_EKKO", "S_EKKO_TEXT", "S_EKPO", "S_EKET", "S_COMP", "S_EKKN"]
ACCOUNT_KEY_COLS = ["EBELN(k/*)", "EBELP(k/*)", "ZEKKN(k/*)"]   # object_key di CK536
KOSTL_COL    = "KOSTL"


def q(name):
    return '"' + name.replace('"', '""') + '"'


def table_exists(cur, schema, table):
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{q(table)}",))
    return cur.fetchone()[0] is not None


def get_columns(cur, table):
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def get_cdc_map(cur) -> dict:
    """object_key di S_EKKN (EBELN|EBELP|ZEKKN) -> codice SAP del centro di costo (un solo codice valido)."""
    if not table_exists(cur, "ref", CDC_TRASCO):
        log.warning("  ref.%s non caricata — KOSTL non trascodificato", CDC_TRASCO)
        return {}
    cur.execute(f"SELECT src, object_key, sap FROM ({po_cdc_sql()}) c WHERE sap IS NOT NULL")
    return {(src, key): sap for src, key, sap in cur.fetchall()}


def sanitize(val):
    if val is None: return ""
    if isinstance(val, float) and math.isnan(val): return ""
    if isinstance(val, str) and val.strip() == "NaN": return ""
    return val


def fetch_table_csv(cur, table, source_name, cols, cdc_map) -> tuple[bytes, int]:
    cur.execute(f'SELECT {", ".join(q(c) for c in cols)} FROM raw.{q(table)} WHERE "_source" = %s ORDER BY 1',
                (source_name,))
    rows = cur.fetchall()
    if table == PO_ACCOUNT_TABLE and cdc_map:
        key_idx, kostl, n = [cols.index(c) for c in ACCOUNT_KEY_COLS], cols.index(KOSTL_COL), 0
        for i, row in enumerate(rows):
            sap = cdc_map.get((source_name, "|".join(row[j] or "" for j in key_idx)))
            if sap:
                rows[i] = row = list(row)
                row[kostl], n = sap, n + 1
        log.info("  [TRASCODIFICA] %s: KOSTL sostituito con COD_SAP_DEF di %s su %d righe, %d senza "
                 "trascodifica (KOSTL originale, CK536)", table, CDC_TRASCO, n,
                 sum(1 for row in rows if (row[kostl] or "").strip()) - n)

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    log.info("  + raw.%s: %d righe", table, len(rows))
    return buf.getvalue().encode("utf-8"), len(rows)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            if not table_exists(cur, "raw", MASTER_TABLE):
                log.warning("raw.%s non trovata — export annullato.", MASTER_TABLE)
                return
            cols_map = {}
            for table in RAW_TABLES:
                if table_exists(cur, "raw", table):
                    cols_map[table] = get_columns(cur, table)
                else:
                    log.warning("  raw.%s — tabella non trovata, verrà omessa.", table)
            cur.execute(f'SELECT DISTINCT "_source" FROM raw.{q(MASTER_TABLE)} WHERE "_source" IS NOT NULL ORDER BY 1')
            sources = [r[0] for r in cur.fetchall()]
            cdc_map = get_cdc_map(cur) if PO_ACCOUNT_TABLE in cols_map else {}
            for source_name in sources:
                log.info("=== Generazione ZIP: %s ===", source_name)
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for table, cols in cols_map.items():
                        csv_bytes, n_rows = fetch_table_csv(cur, table, source_name, cols, cdc_map)
                        if n_rows:
                            zf.writestr(f"{table}.csv", csv_bytes)
                output_path = output_zip_path(OUTPUT_DIR, source_name)
                with open(output_path, "wb") as f:
                    f.write(buf.getvalue())
                log.info("  ✓  %s  (%.1f KB)", output_path, len(buf.getvalue()) / 1024)
    finally:
        conn.close()
    log.info("=== export_ordini_acquisto_zip completato ===")


if __name__ == "__main__":
    main()
