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
  il codice SAP (PARTNER di ref.SAP_EXPORT_BUT000, solo BP dei gruppi ZBP e ZIC, non i
  destinatari merci ZDM): tramite BU_SORT1 oppure, per i fornitori del flusso 05a (BP creato
  come cliente), tramite REF_KUNNR e BU_SORT2.
  Gli info record con fornitore senza codice SAP (CK526, se l'esclusione dall'output è attiva
  nel catalogo) non sono esportati, in nessuna delle quattro tabelle: finiscono in
  <tabella>_discarded.xlsx. Se BUT000 non è caricata LIFNR(*)
  resta invariato e non si esclude nulla per CK526.
  Allo stesso modo, con l'esclusione dall'output di CK528 attiva nel catalogo (default:
  disattivata), non sono esportati gli info record con materiale non ancora migrato in SAP.
  CK421 (esclusione dall'output attiva nel catalogo): gli scaglioni senza condizione padre con
  la stessa chiave in S_COND non sono esportati (solo lo scaglione, l'info record resta).
  Tempo di consegna (CK516): se in S_EINE#INFORDATIACQ APLFZ è vuoto o zero e anche PLIFZ del
  materiale (SAP_EXPORT_MARC o, se la coppia materiale/divisione non è in SAP, S_MARC) è vuoto
  o zero, nel file per SAP APLFZ = 999: SAP crea l'info record e il valore si riconosce per
  la correzione. Vale anche con CK516 disattivato.
  Divisa CNY: il Migration Cockpit di SAP non la gestisce bene, quindi nel file per SAP
  S_EINE#INFORDATIACQ.WAERS e S_COND#INFORCOND.KONWA con CNY diventano EUR (segnalato nel log);
  gli importi restano quelli del file di origine, senza conversione.
  Il CSV usa separatore ";" e non include colonne audit.
  Un ZIP separato per ogni valore distinto di _source.
  Output: /project/datalake/out_source_mdg/<_source>
depends:
  - stg.ck407_inforecord_orphans
  - stg.ck512_inforecord_matnr_lifnr
  - stg.ck526_inforecord_lifnr_sap
  - stg.ck528_inforecord_materiale_in_sap_marc
  - stg.ck421_inforecord_scales_cond
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
from prd_export_clienti_zip import get_but000_map
from prd_discard_lib import add_discards, flush_discards, reset_discards
from prd_merge_lib import exclusion_enabled, failing_keys
from ck_query_lib import lead_time_missing_sql, LEAD_TIME_PLACEHOLDER, LEAD_TIME_TABLES

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
SCALES_TABLE = "S_SCALES#INFORSCALES"
# Chiave degli scaglioni nell'object_key di CK421
SCALES_KEY_COLS = ["INFNR(k/*)", "EKORG(k/*)", "ESOKZ(k/*)", "WERKS(k)", "KOPOS(k/*)",
                   "COND_TYPE(k/*)", "VALID_FROM(k/*)", "LINE_NO(k/*)"]
INFNR_COL    = "INFNR(k/*)"
PURCHASING_TABLE = "S_EINE#INFORDATIACQ"
PURCHASING_KEY_COLS = ["INFNR(k/*)", "EKORG(k/*)", "ESOKZ(k/*)", "WERKS(k)"]
# Divise non gestite bene dal Migration Cockpit: sostituite nel file per SAP (importi invariati)
CURRENCY_OVERRIDES = {"CNY": "EUR"}
CURRENCY_COLS = {PURCHASING_TABLE: "WAERS", "S_COND#INFORCOND": "KONWA"}
CLI_SUPPLIER_TABLE = "S_SUPPL_GEN#AddDatiGenerali"   # flusso 05a: fornitori estesi da cliente
SUPPLIER_BU_GROUPS = ("ZBP", "ZIC")   # gruppi BP dei fornitori in BUT000 (esclusi i ZDM)

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
    che in SAP sono il BP del cliente, REF_KUNNR tramite BU_SORT2. Solo BP dei gruppi ZBP e ZIC.
    """
    lifnr_map = get_but000_map(conn, sort_cols=("BU_SORT1",), bu_groups=SUPPLIER_BU_GROUPS)
    if not lifnr_map or not table_exists(conn, CLI_SUPPLIER_TABLE):
        return lifnr_map
    customer_codes = get_but000_map(conn, sort_cols=("BU_SORT2",), bu_groups=SUPPLIER_BU_GROUPS)
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


def get_excluded_scales(conn) -> dict:
    """
    Scaglioni senza condizione padre in S_COND (CK421), se l'esclusione dall'output è attiva nel
    catalogo: {chiave dello scaglione: messaggio del check}.
    """
    with conn.cursor() as cur:
        if not exclusion_enabled(cur, "CK421"):
            return {}
        cur.execute("""
            SELECT object_key, message FROM stg.check_results
            WHERE check_id = 'CK421' AND status <> 'Ok'
              AND run_id = (SELECT MAX(run_id) FROM stg.check_results WHERE check_id = 'CK421')
        """)
        scales = dict(cur.fetchall())
    if scales:
        log.warning("  *** [CK421] %d scaglioni ESCLUSI, senza condizione padre in S_COND", len(scales))
    return scales


def transcode_lifnr(cols, rows, lifnr_map):
    """Sostituisce LIFNR(*) con il codice SAP (senza BUT000 il codice resta invariato)."""
    idx = cols.index(LIFNR_COL)
    out = []
    for row in rows:
        row = list(row)
        row[idx] = lifnr_map.get((row[idx] or "").strip(), row[idx])
        out.append(row)
    return out


def get_missing_lead_times(conn) -> set:
    """Righe di S_EINE senza tempo di consegna né nell'info record né nel materiale (chiave completa)."""
    if not all(table_exists(conn, t) for t in LEAD_TIME_TABLES):
        return set()
    with conn.cursor() as cur:
        cur.execute(f"SELECT infnr, ekorg, esokz, werks FROM ({lead_time_missing_sql(cur)}) b WHERE forza")
        return set(cur.fetchall())


def force_lead_time(cols, rows, missing, source_name):
    """Scrive il valore riconoscibile in APLFZ sulle righe senza tempo di consegna."""
    key_idx, aplfz = [cols.index(c) for c in PURCHASING_KEY_COLS], cols.index("APLFZ")
    out, forced = [], 0
    for row in rows:
        if tuple(row[i] for i in key_idx) in missing:
            row = list(row)
            row[aplfz] = LEAD_TIME_PLACEHOLDER
            forced += 1
        out.append(row)
    if forced:
        log.warning("  [CK516] %s: APLFZ = %s su %d righe senza tempo di consegna ('%s')",
                    PURCHASING_TABLE, LEAD_TIME_PLACEHOLDER, forced, source_name)
    return out


def force_currency(table, cols, rows, source_name):
    """Sostituisce le divise di CURRENCY_OVERRIDES e segnala nel log le righe e gli info record toccati."""
    col = CURRENCY_COLS[table]
    idx, infnr_idx = cols.index(col), cols.index(INFNR_COL)
    out, forced = [], {}
    for row in rows:
        old = (row[idx] or "").strip()
        if old in CURRENCY_OVERRIDES:
            forced.setdefault(old, []).append(row[infnr_idx])
            row = list(row)
            row[idx] = CURRENCY_OVERRIDES[old]
        out.append(row)
    for old, infnrs in forced.items():
        log.warning("  [DIVISA] %s.%s: %s sostituita con %s su %d righe, importi non convertiti ('%s'); info record: %s",
                    table, col, old, CURRENCY_OVERRIDES[old], len(infnrs), source_name,
                    ", ".join(sorted(set(infnrs))))
    return out


def fetch_table_csv(conn, table, source_name, cols, lifnr_map, excluded, excluded_scales, missing_lead_times):
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
    if table == SCALES_TABLE and excluded_scales:
        key_idx = [cols.index(c) for c in SCALES_KEY_COLS]
        scale_key = lambda row: "|".join(row[i] or "" for i in key_idx)
        dropped = [row for row in rows if scale_key(row) in excluded_scales]
        add_discards(table, cols, dropped, "CK421", lambda row: excluded_scales[scale_key(row)])
        rows = [row for row in rows if scale_key(row) not in excluded_scales]
    if table == MASTER_TABLE:
        rows = transcode_lifnr(cols, rows, lifnr_map)
    if table == PURCHASING_TABLE:
        rows = force_lead_time(cols, rows, missing_lead_times, source_name)
    if table in CURRENCY_COLS:
        rows = force_currency(table, cols, rows, source_name)

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


def build_zip(conn, source_name, cols_map, lifnr_map, excluded, excluded_scales, missing_lead_times):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in RAW_TABLES:
            cols = cols_map.get(table)
            if not cols:
                log.warning("  SKIP raw.%s — tabella non trovata o senza colonne", table)
                continue
            csv_bytes = fetch_table_csv(conn, table, source_name, cols, lifnr_map, excluded, excluded_scales,
                                        missing_lead_times)
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
        excluded_scales = get_excluded_scales(conn)
        missing_lead_times = get_missing_lead_times(conn)
        log.info("Sorgenti trovate: %s", sources)
        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)
            zip_bytes   = build_zip(conn, source_name, cols_map, lifnr_map, excluded, excluded_scales, missing_lead_times)
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
