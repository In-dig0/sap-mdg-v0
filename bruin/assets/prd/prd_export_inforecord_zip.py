""" @bruin
name: prd.export_inforecord_zip
type: python
description: >
  Genera l'archivio ZIP di output per gli inforecord acquisti.
  Tabelle coinvolte:
    - S_EINA#INFORMATFOR   (master)
    - S_EINE#INFORDATIACQ
    - S_SCALES#INFORSCALES
    - S_COND#INFORCOND
  Nessuna esclusione di record orfani (CK407 segnala le anomalie nel report
  qualita, ma non blocca la scrittura dell'output).
  In S_EINA#INFORMATFOR il fornitore LIFNR(*) (codice del vecchio ERP) è trascodificato con
  il codice SAP (PARTNER di ref.SAP_EXPORT_BUT000): tramite BU_SORT1 oppure, per i fornitori
  del flusso 05a (BP creato come cliente), tramite REF_KUNNR e BU_SORT2.
  Gli info record con fornitore senza codice SAP (CK526, se l'esclusione dall'output è attiva
  nel catalogo) non sono esportati, in nessuna delle quattro tabelle: finiscono in
  <tabella>_discarded.xlsx. Se BUT000 non è caricata LIFNR(*)
  resta invariato e non si esclude nulla per CK526.
  Allo stesso modo, con l'esclusione dall'output di CK528 attiva nel catalogo (default:
  disattivata), non sono esportati gli info record con materiale non ancora migrato in SAP.
  Il CSV usa separatore ";" e non include colonne audit.
  Un ZIP separato per ogni valore distinto di _source.
  Output: /project/datalake/out_source_mdg/<_source>
depends:
  - stg.ck407_inforecord_orphans
  - stg.ck512_inforecord_matnr_lifnr
  - stg.ck526_inforecord_lifnr_sap
  - stg.ck528_inforecord_materiale_in_sap_marc
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
from prd_output_lib import output_zip_path
from prd_export_clienti_zip import get_but000_map
from prd_discard_lib import add_discards, flush_discards, reset_discards
from prd_merge_lib import exclusion_enabled, failing_keys

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

OUTPUT_DIR = "/project/datalake/out_source_mdg"
AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

MASTER_TABLE = "S_EINA#INFORMATFOR"
LIFNR_COL    = "LIFNR(*)"
INFNR_COL    = "INFNR(k/*)"
CLI_SUPPLIER_TABLE = "S_SUPPL_GEN#AddDatiGenerali"   # flusso 05a: fornitori estesi da cliente

RAW_TABLES = [
    "S_EINA#INFORMATFOR",
    "S_EINE#INFORDATIACQ",
    "S_SCALES#INFORSCALES",
    "S_COND#INFORCOND",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn):
    with conn.cursor() as cur:
        cur.execute(
            'SELECT DISTINCT "_source" FROM raw."S_EINA#INFORMATFOR"'
            ' WHERE "_source" IS NOT NULL ORDER BY 1'
        )
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


def get_lifnr_map(conn):
    """
    Vecchio codice fornitore -> codice SAP: BU_SORT1 di BUT000 oppure, per i fornitori 05a
    che in SAP sono il BP del cliente, REF_KUNNR tramite BU_SORT2.
    """
    lifnr_map = get_but000_map(conn, sort_cols=("BU_SORT1",))
    if not lifnr_map or not table_exists(conn, CLI_SUPPLIER_TABLE):
        return lifnr_map
    customer_codes = get_but000_map(conn, sort_cols=("BU_SORT2",))
    with conn.cursor() as cur:
        cur.execute(f'SELECT DISTINCT TRIM("LIFNR(k/*)"), TRIM("REF_KUNNR") FROM raw.{q(CLI_SUPPLIER_TABLE)}')
        for lifnr, kunnr in cur.fetchall():
            if kunnr in customer_codes:
                lifnr_map.setdefault(lifnr, customer_codes[kunnr])
    return lifnr_map


def get_excluded_infnr(conn, lifnr_map):
    """
    Info record da escludere, per i check con esclusione dall'output attiva nel catalogo:
    - CK526: fornitore senza codice SAP (solo se BUT000 è caricata);
    - CK528: materiale non ancora migrato in SAP (ultimo run del check).
    Restituisce {INFNR: (check, motivo)} per i file di scarto.
    """
    excluded = {}
    with conn.cursor() as cur:
        if lifnr_map and exclusion_enabled(cur, "CK526"):
            cur.execute(f'SELECT {q(INFNR_COL)}, {q(LIFNR_COL)} FROM raw.{q(MASTER_TABLE)}')
            missing = {infnr: (lifnr or "").strip() for infnr, lifnr in cur.fetchall()
                       if (lifnr or "").strip() not in lifnr_map}
            if missing:
                log.warning("  *** [CK526] %d info record ESCLUSI, fornitori senza codice SAP in BUT000: %s",
                            len(missing), ", ".join(sorted(set(missing.values()))))
            excluded.update({infnr: ("CK526", f"Fornitore {lifnr or '(vuoto)'} senza codice SAP in BUT000")
                             for infnr, lifnr in missing.items()})
        if exclusion_enabled(cur, "CK528"):
            keys = failing_keys(cur, "CK528")
            if keys:
                log.warning("  *** [CK528] %d info record ESCLUSI, materiale non ancora migrato in SAP",
                            len(keys))
            for infnr in keys:
                check, reason = excluded.get(infnr, ("", ""))
                excluded[infnr] = (", ".join(filter(None, [check, "CK528"])),
                                   "; ".join(filter(None, [reason, "Materiale non ancora migrato in SAP"])))
    return excluded


def transcode_lifnr(cols, rows, lifnr_map):
    """Sostituisce LIFNR(*) con il codice SAP (senza BUT000 il codice resta invariato)."""
    idx = cols.index(LIFNR_COL)
    out = []
    for row in rows:
        row = list(row)
        row[idx] = lifnr_map.get((row[idx] or "").strip(), row[idx])
        out.append(row)
    return out


def fetch_table_csv(conn, table, source_name, cols, lifnr_map, excluded):
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s ORDER BY 1',
            (source_name,)
        )
        rows = cur.fetchall()
    idx = cols.index(INFNR_COL)
    groups = {}
    for row in rows:
        if row[idx] in excluded:
            groups.setdefault(excluded[row[idx]], []).append(row)
    for (check, reason), discarded in groups.items():
        add_discards(table, cols, discarded, check, reason)
    if groups:
        rows = [row for row in rows if row[idx] not in excluded]
    if table == MASTER_TABLE:
        rows = transcode_lifnr(cols, rows, lifnr_map)

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


def build_zip(conn, source_name, cols_map, lifnr_map, excluded):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in RAW_TABLES:
            cols = cols_map.get(table)
            if not cols:
                log.warning("  SKIP raw.%s — tabella non trovata o senza colonne", table)
                continue
            csv_bytes = fetch_table_csv(conn, table, source_name, cols, lifnr_map, excluded)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe per '%s' — skip", table, source_name)
                continue
            zf.writestr(f"{table}.csv", csv_bytes)
            log.info("  + raw.%s: %d righe", table, n_rows)
    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reset_discards(RAW_TABLES)
    conn = get_connection()
    try:
        if not table_exists(conn, MASTER_TABLE):
            log.warning("raw.%s non trovata — export annullato.", MASTER_TABLE)
            return
        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun _source trovato in raw.%s — nulla da esportare.", MASTER_TABLE)
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
        lifnr_map = get_lifnr_map(conn)
        excluded  = get_excluded_infnr(conn, lifnr_map)
        log.info("Sorgenti trovate: %s", sources)
        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)
            zip_bytes   = build_zip(conn, source_name, cols_map, lifnr_map, excluded)
            output_path = output_zip_path(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(zip_bytes)
            log.info("  ✓  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)
        flush_discards()
    finally:
        conn.close()
    log.info("=== export_inforecord_zip completato ===")


if __name__ == "__main__":
    main()
