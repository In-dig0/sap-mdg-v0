""" @bruin
name: prd.export_custmat_zip
type: python
description: >
  Genera l'archivio ZIP di output per gli info record cliente-materiale (flusso DM08_SD-InfoVen):
  la tabella raw S_CUSTMAT#INFORMATFOR, unico file dell'archivio, in
  <file in ingresso>_MDGOUT_YYYYMMDD_HHMI.zip (un ZIP per ogni valore distinto di _source).
  Le righe segnalate da un controllo sugli info record cliente-materiale attivo e con
  l'esclusione dall'output attiva nel catalogo non sono esportate e finiscono in
  discarded/S_CUSTMAT_INFORMATFOR_discarded.xlsx con controllo e motivo:
    - CK531: materiale non ancora migrato in SAP nella divisione;
    - CK532: organizzazione commerciale/canale non tra le aree di vendita del cliente;
    - CK533: organizzazione commerciale/canale non tra i dati di vendita del materiale.
  Il CSV usa separatore ";" e non include colonne audit. I duplicati sulla chiave sono già
  scartati dal caricamento (discarded/ingestione_scarti.xlsx).
  Output: /project/datalake/out_source_mdg/<_source>_MDGOUT_<YYYYMMDD_HHMI>.zip
depends:
  - stg.ck531_custmat_materiale_in_sap_marc
  - stg.ck532_custmat_area_vendite_cliente
  - stg.ck533_custmat_area_vendite_materiale
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
from prd_discard_lib import add_discards, flush_discards, reset_discards
from prd_merge_lib import exclusion_enabled

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
TABLE      = "S_CUSTMAT#INFORMATFOR"
AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}
# object_key dei controlli: SALESORGANIZATION|DISTRIBUTIONCHANNEL|CUSTOMER|MATERIAL
KEY_COLS   = ["SALESORGANIZATION(k/*)", "DISTRIBUTIONCHANNEL(k/*)", "CUSTOMER(k/*)", "MATERIAL(k/*)"]
# Controlli con esclusione dall'output, nell'ordine in cui è attribuito il motivo
CHECKS     = ["CK531", "CK532", "CK533"]


def q(name):
    return '"' + name.replace('"', '""') + '"'


def get_columns(cur):
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = %s
        ORDER BY ordinal_position
    """, (TABLE,))
    return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def get_excluded(cur) -> dict:
    """object_key -> (controllo, messaggio) delle righe da escludere (primo controllo che le segnala)."""
    excluded = {}
    for check_id in CHECKS:
        if not exclusion_enabled(cur, check_id):
            continue
        cur.execute("""
            SELECT object_key, MIN(message) FROM stg.check_results
            WHERE check_id = %s AND status <> 'Ok'
              AND run_id = (SELECT MAX(run_id) FROM stg.check_results WHERE check_id = %s)
            GROUP BY object_key
        """, (check_id, check_id))
        rows = cur.fetchall()
        for key, message in rows:
            excluded.setdefault(key, (check_id, message))
        log.info("  [%s] %d info record segnalati, esclusi dall'output", check_id, len(rows))
    return excluded


def build_zip(cur, source_name, cols, excluded) -> tuple[bytes, int]:
    cur.execute(f'SELECT {", ".join(q(c) for c in cols)} FROM raw.{q(TABLE)} WHERE "_source" = %s ORDER BY 1, 2, 3, 4',
                (source_name,))
    rows = cur.fetchall()
    key_idx = [cols.index(c) for c in KEY_COLS]
    row_key = lambda row: "|".join(row[i] or "" for i in key_idx)

    kept, dropped = [], {}
    for row in rows:
        hit = excluded.get(row_key(row))
        if hit:
            dropped.setdefault(hit[0], []).append(row)
        else:
            kept.append(row)
    for check_id, discarded in dropped.items():
        add_discards(TABLE, cols, discarded, check_id, lambda row: excluded[row_key(row)][1])

    def sanitize(val):
        if val is None: return ""
        if isinstance(val, float) and math.isnan(val): return ""
        if isinstance(val, str) and val.strip() == "NaN": return ""
        return val

    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in kept])
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{TABLE}.csv", out.getvalue().encode("utf-8"))
    log.info("  + raw.%s: %d righe esportate, %d escluse", TABLE, len(kept), len(rows) - len(kept))
    return buf.getvalue(), len(kept)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reset_discards([TABLE])
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"raw.{q(TABLE)}",))
            if cur.fetchone()[0] is None:
                log.warning("raw.%s non trovata — export annullato.", TABLE)
                return
            cols = get_columns(cur)
            cur.execute(f'SELECT DISTINCT "_source" FROM raw.{q(TABLE)} WHERE "_source" IS NOT NULL ORDER BY 1')
            sources = [r[0] for r in cur.fetchall()]
            excluded = get_excluded(cur)
            for source_name in sources:
                log.info("=== Generazione ZIP: %s ===", source_name)
                zip_bytes, n_rows = build_zip(cur, source_name, cols, excluded)
                output_path = output_zip_path(OUTPUT_DIR, source_name)
                if n_rows == 0:
                    log.warning("  ✗  %s: nessuna riga da esportare — ZIP non generato", source_name)
                    continue
                with open(output_path, "wb") as f:
                    f.write(zip_bytes)
                log.info("  ✓  %s  (%.1f KB)", output_path, len(zip_bytes) / 1024)
        flush_discards()
    finally:
        conn.close()
    log.info("=== export_custmat_zip completato ===")


if __name__ == "__main__":
    main()
