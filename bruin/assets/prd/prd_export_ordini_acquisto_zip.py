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
  Fornitore: in S_EKKO LIFNR(*) (codice del vecchio ERP) è trascodificato con il BP SAP come negli
  info record acquisti (BU_SORT1 dei BP fornitori ZBP/ZIC o, per i fornitori 05a, REF_KUNNR tramite
  BU_SORT2); un codice che è già un BP SAP resta invariato. I fornitori senza codice SAP sono
  segnalati da CK062 ed esportati con il codice originale. Con l'opzione "codice BP originale"
  della pagina Gestione pipeline LIFNR(*) non è trascodificato.
  Incoterm: se in S_EKKO INCO1 è vuoto, INCO1 e INCO2_L sono presi dall'anagrafica del fornitore
  (S_SUPPL_PURCHASING ZBP_OrganAcq, poi ZBP_AddOrganAcq, con LIFNR del vecchio ERP + EKORG, prima della
  trascodifica del fornitore); INCO2_L solo se vuota e se INCO1 coincide con quello del fornitore.
  Ordini completati in report/<nome ZIP>_incoterms_da_BP.xlsx; i rimasti senza Incoterm sono
  segnalati da CK222.
  Le imputazioni senza trascodifica valida (record assente, COD_SAP_DEF vuoto o '??', posizione
  non trovata) sono segnalate da CK536 ed esportate con il KOSTL originale di DBCOGE.
  Nessuna esclusione: i controlli sugli ordini (CK060-062, CK222-225, CK415, CK520-524, CK529,
  CK536) sono solo segnalazioni.
  Estrazione ristretta (test): se stg.export_filter_ordini contiene numeri d'ordine (EBELN), viene
  generato anche lo ZIP con i soli ordini indicati, in tutte le tabelle, in out_source_mdg/test
  (lo ZIP completo non cambia). Gli ordini dell'elenco non trovati sono segnalati nel log.
  Il CSV usa separatore ";" e non include colonne audit.
  Un ZIP separato per ogni valore distinto di _source.
  Output: /project/datalake/out_source_mdg/<_source>_MDGOUT_<YYYYMMDD_HHMI>.zip
depends:
  - stg.ck536_po_kostl_trasco_cdc
  - stg.ck062_po_lifnr_but000
  - stg.ck222_po_incoterm
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
from prd_output_lib import output_zip_path, original_codes
from ck_query_lib import po_cdc_sql, po_bp_incoterms_sql, PO_ACCOUNT_TABLE, CDC_TRASCO
from prd_export_inforecord_zip import get_lifnr_map
from prd_discard_lib import REPORT_DIR, write_xlsx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from mdg_env import DB_CONFIG, OUT_MDG  # noqa: E402

OUTPUT_DIR   = OUT_MDG
TEST_DIR     = os.path.join(OUTPUT_DIR, "test")   # estrazione ristretta (stg.export_filter_ordini)
FILTER_TABLE = "export_filter_ordini"
AUDIT_COLS   = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}
MASTER_TABLE = "S_EKKO"
RAW_TABLES   = ["S_EKKO", "S_EKKO_TEXT", "S_EKPO", "S_EKET", "S_COMP", "S_EKKN"]
ACCOUNT_KEY_COLS = ["EBELN(k/*)", "EBELP(k/*)", "ZEKKN(k/*)"]   # object_key di CK536
KOSTL_COL    = "KOSTL"
LIFNR_COL    = "LIFNR(*)"   # fornitore in S_EKKO, trascodificato con il BP SAP
EKORG_COL    = "EKORG(*)"
INCO1_COL, INCO2_COL = "INCO1", "INCO2_L"


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


def get_supplier_map(cur) -> dict:
    """Codice fornitore del vecchio ERP -> BP SAP; i codici che sono già BP SAP restano invariati."""
    if not table_exists(cur, "ref", "SAP_EXPORT_BUT000"):
        log.warning("  ref.SAP_EXPORT_BUT000 non caricata — LIFNR(*) non trascodificato")
        return {}
    supplier_map = get_lifnr_map(cur.connection)
    cur.execute('SELECT DISTINCT TRIM("PARTNER") FROM ref."SAP_EXPORT_BUT000" WHERE COALESCE(TRIM("PARTNER"), \'\') <> \'\'')
    supplier_map.update({p: p for (p,) in cur.fetchall()})
    return supplier_map


def transcode_supplier(cols, rows, supplier_map, source_name):
    """LIFNR(*) di S_EKKO -> BP SAP; i fornitori non trascodificabili restano invariati (CK062)."""
    idx, out, changed, missing = cols.index(LIFNR_COL), [], 0, set()
    for row in rows:
        old = (row[idx] or "").strip()
        new = supplier_map.get(old)
        if new and new != row[idx]:
            row = list(row)
            row[idx], changed = new, changed + 1
        elif not new and old:
            missing.add(old)
        out.append(row)
    log.info("  [TRASCODIFICA] S_EKKO: LIFNR(*) sostituito con il BP SAP su %d ordini ('%s')", changed, source_name)
    if missing:
        log.warning("  [CK062] %d fornitori senza codice SAP, ordini esportati con il codice originale: %s",
                    len(missing), ", ".join(sorted(missing)))
    return out


def get_incoterm_map(cur) -> dict:
    """(LIFNR del vecchio ERP, EKORG) -> (INCO1, INCO2, tabella) dall'anagrafica del fornitore."""
    sql = po_bp_incoterms_sql(cur)
    if not sql:
        log.warning("  Anagrafica acquisti del fornitore non caricata — Incoterm vuoti non completati")
        return {}
    cur.execute(sql)
    return {(lifnr, ekorg): (inco1, inco2, src) for lifnr, ekorg, inco1, inco2, src in cur.fetchall()}


def fill_incoterms(cols, rows, incoterm_map, source_name, filled=None):
    """
    INCO1 vuoto -> INCO1 (e INCO2_L se vuota) dell'anagrafica del fornitore; INCO2_L vuota con INCO1
    uguale a quello del fornitore -> INCO2 del fornitore. Da applicare prima della trascodifica di LIFNR.
    filled: lista in cui annotare gli ordini completati (per il report).
    """
    if not all(c in cols for c in (LIFNR_COL, EKORG_COL, INCO1_COL, INCO2_COL)):
        return rows
    ebeln, lifnr, ekorg = cols.index(ebeln_col(cols)), cols.index(LIFNR_COL), cols.index(EKORG_COL)
    i1, i2 = cols.index(INCO1_COL), cols.index(INCO2_COL)
    out, n1, n2 = [], 0, 0
    for row in rows:
        bp = incoterm_map.get(((row[lifnr] or "").strip(), (row[ekorg] or "").strip()))
        if bp:
            bp_inco1, bp_inco2, src = bp
            new1 = (row[i1] or "").strip() or bp_inco1
            set1 = not (row[i1] or "").strip()
            set2 = not (row[i2] or "").strip() and bp_inco2 and new1 == bp_inco1
            if set1 or set2:
                row = list(row)
                if set1:
                    row[i1], n1 = bp_inco1, n1 + 1
                if set2:
                    row[i2], n2 = bp_inco2, n2 + 1
                if filled is not None:
                    filled.append([row[ebeln], row[lifnr], row[ekorg], bp_inco1 if set1 else "",
                                   bp_inco2 if set2 else "", src])
        out.append(row)
    if n1 or n2:
        log.info("  [INCOTERM] S_EKKO: INCO1 preso dall'anagrafica del fornitore su %d ordini, INCO2_L su %d ('%s')",
                 n1, n2, source_name)
    return out


def incoterm_report_path(source_name):
    return os.path.join(REPORT_DIR, f"{os.path.splitext(source_name)[0]}_incoterms_da_BP.xlsx")


def write_incoterm_report(source_name, filled):
    """report/<ZIP>_incoterms_da_BP.xlsx: ordini con Incoterm presi dall'anagrafica del fornitore."""
    path = incoterm_report_path(source_name)
    if not filled:
        if os.path.exists(path):
            os.remove(path)
        return
    os.makedirs(REPORT_DIR, exist_ok=True)
    write_xlsx(path, [("incoterms_da_BP", ["EBELN", "LIFNR", "EKORG", "INCO1", "INCO2_L", "ORIGINE"],
                       sorted(filled))])
    log.info("  [INCOTERM] %d ordini completati dall'anagrafica del fornitore: %s", len(filled), path)


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


def get_order_filter(cur) -> set:
    """Numeri d'ordine dell'estrazione ristretta (stg.export_filter_ordini); vuoto = nessun filtro."""
    if not table_exists(cur, "stg", FILTER_TABLE):
        return set()
    cur.execute(f"SELECT TRIM(kunnr) FROM stg.{FILTER_TABLE} WHERE COALESCE(TRIM(kunnr), '') <> ''")
    return {r[0] for r in cur.fetchall()}


def ebeln_col(cols):
    """Colonna del numero ordine: EBELN(k/*) o EBELN(k) a seconda del file."""
    return next(c for c in cols if c.split("(")[0] == "EBELN")


def fetch_table_csv(cur, table, source_name, cols, cdc_map, orders=None, supplier_map=None,
                    incoterm_map=None, filled=None) -> tuple[bytes, int]:
    cur.execute(f'SELECT {", ".join(q(c) for c in cols)} FROM raw.{q(table)} WHERE "_source" = %s ORDER BY 1',
                (source_name,))
    rows = cur.fetchall()
    if orders is not None:
        idx = cols.index(ebeln_col(cols))
        rows = [row for row in rows if (row[idx] or "").strip() in orders]
    if table == MASTER_TABLE and incoterm_map:   # con il codice fornitore del vecchio ERP
        rows = fill_incoterms(cols, rows, incoterm_map, source_name, filled)
    if table == MASTER_TABLE and supplier_map:
        rows = transcode_supplier(cols, rows, supplier_map, source_name)
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


def build_zip(cur, source_name, cols_map, cdc_map, supplier_map, orders=None,
              incoterm_map=None, filled=None) -> bytes:
    """ZIP con le tabelle del flusso (orders: solo questi numeri d'ordine, None = tutti)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table, cols in cols_map.items():
            csv_bytes, n_rows = fetch_table_csv(cur, table, source_name, cols, cdc_map, orders, supplier_map,
                                                incoterm_map, filled)
            if n_rows:
                zf.writestr(f"{table}.csv", csv_bytes)
    return buf.getvalue()


def write_test_zip(cur, source_name, cols_map, cdc_map, supplier_map, orders: set, incoterm_map=None):
    """Estrazione ristretta: rimuove lo ZIP di test precedente e scrive quello con i soli ordini indicati."""
    test_path = output_zip_path(TEST_DIR, source_name)   # rimuove anche il precedente
    if not orders:
        return
    cur.execute(f'SELECT DISTINCT TRIM({q(ebeln_col(cols_map[MASTER_TABLE]))}) FROM raw.{q(MASTER_TABLE)} '
                'WHERE "_source" = %s', (source_name,))
    found = {r[0] for r in cur.fetchall()} & orders
    missing = sorted(orders - found)
    if missing:
        log.warning("  [TEST] %d ordini dell'elenco non presenti in %s di '%s': %s",
                    len(missing), MASTER_TABLE, source_name, ", ".join(missing))
    if not found:
        log.info("  [TEST] %s: nessun ordine dell'elenco — ZIP di test non generato", source_name)
        return
    log.info("=== Estrazione ristretta (test): %d ordini ===", len(found))
    zip_bytes = build_zip(cur, source_name, cols_map, cdc_map, supplier_map, found, incoterm_map)
    os.makedirs(TEST_DIR, exist_ok=True)
    with open(test_path, "wb") as f:
        f.write(zip_bytes)
    log.info("  [TEST] ✓  %s  (%d ordini: %s)", test_path, len(found), ", ".join(sorted(found)))


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
            orders = get_order_filter(cur)
            # Opzione "codice BP originale": LIFNR(*) resta il codice del vecchio ERP
            supplier_map = {} if original_codes() else get_supplier_map(cur)
            incoterm_map = get_incoterm_map(cur)
            for source_name in sources:
                log.info("=== Generazione ZIP: %s ===", source_name)
                filled = []
                zip_bytes = build_zip(cur, source_name, cols_map, cdc_map, supplier_map,
                                      incoterm_map=incoterm_map, filled=filled)
                write_incoterm_report(source_name, filled)
                output_path = output_zip_path(OUTPUT_DIR, source_name)
                with open(output_path, "wb") as f:
                    f.write(zip_bytes)
                log.info("  ✓  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)
                write_test_zip(cur, source_name, cols_map, cdc_map, supplier_map, orders, incoterm_map)
    finally:
        conn.close()
    log.info("=== export_ordini_acquisto_zip completato ===")

if __name__ == "__main__":
    main()
