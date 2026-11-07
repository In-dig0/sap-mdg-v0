""" @bruin
name: prd.export_maritc_zip
type: python
description: >
  Genera l'archivio ZIP di output per i codici doganali (S_MARITC).
  La tabella viene letta dallo schema raw.
  I record che non superano i controlli con esclusione dall'output attiva nel catalogo
  vengono esclusi dall'output, scritti in
  S_MARITC_discarded.xlsx e segnalati in modo evidente nel log:
    - CK034: codice doganale COMCO vuoto o assente in SAP_CommodityCodesEU01;
    - CK506: coppia MATNR(k/*)+PLANT(k/*) assente in S_MARC;
    - CK511: MATNR(k/*) assente in S_MARA;
    - CK525: materiale non ancora migrato in SAP nella divisione (SAP_EXPORT_MARC).
  Gli esiti sono letti dall'ultimo run di ciascun check in stg.check_results.
  Nel CSV di output il codice materiale MATNR(k/*) è senza spazi iniziali e finali.
  Il CSV usa separatore ";" e non include colonne audit.
  Output: /project/datalake/out_source_mdg/<_source>
depends:
  - stg.ck034_maritc_comco
  - stg.ck506_maritc_in_marc
  - stg.ck511_maritc_mara
  - stg.ck525_materiale_in_sap_marc
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
from prd_output_lib import output_zip_path, remove_previous_zips
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
RAW_TABLE  = "S_MARITC"
# Controlli le cui righe in errore sono escluse dall'output:
# check_id -> (espressione SQL uguale all'object_key del check, descrizione del motivo)
EXCLUDE_CHECKS = {
    "CK034": ('"MATNR(k/*)" || \'|\' || "PLANT(k/*)"',
              "codice doganale COMCO vuoto o assente in SAP_CommodityCodesEU01"),
    "CK506": ('"MATNR(k/*)" || \'|\' || "PLANT(k/*)"',
              "coppia MATNR+PLANT assente in S_MARC (PRODUCT(k/*)+WERKS(k/*))"),
    "CK511": ('"MATNR(k/*)"', "MATNR assente in S_MARA (PRODUCT(k/*))"),
    "CK525": ('"MATNR(k/*)" || \'|\' || "PLANT(k/*)"',
              "materiale non ancora migrato in SAP nella divisione (SAP_EXPORT_MARC)"),
}
MATNR_COL  = "MATNR(k/*)"        # scritto senza spazi iniziali e finali

# Colonne audit da escludere dai CSV di output
AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sanitize(val):
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    if isinstance(val, str) and val.strip() == "NaN":
        return ""
    return val


def rows_to_csv(cols: list[str], rows: list, strip_cols: set = frozenset()) -> bytes:
    """CSV con separatore ";"; le colonne in strip_cols sono scritte senza spazi iniziali e finali."""
    idx = [i for i, c in enumerate(cols) if c in strip_cols]
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    for row in rows:
        row = [sanitize(v) for v in row]
        for i in idx:
            if isinstance(row[i], str):
                row[i] = row[i].strip()
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def exclusion_condition(excluded: dict[str, set]) -> tuple[str, list]:
    """Condizione SQL delle righe in errore in almeno un check di EXCLUDE_CHECKS, con i parametri."""
    parts, params = [], []
    for check_id, keys in excluded.items():
        expr = EXCLUDE_CHECKS[check_id][0]
        parts.append(f'{expr} IN ({",".join(["%s"] * len(keys))})')
        params += sorted(keys)
    return " OR ".join(parts), params


def write_discarded_csv(conn, source_name: str, cols: list, excluded: dict[str, set]):
    """
    Registra i record esclusi (file discarded/S_MARITC_discarded.xlsx, valori come in raw) con i
    check che li escludono e il motivo. Se non ci sono esclusioni non fa nulla.
    """
    if not excluded:
        return
    cond, params = exclusion_condition(excluded)
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {", ".join(q(c) for c in cols)} FROM raw.{q(RAW_TABLE)}'
            f' WHERE "_source" = %s AND ({cond}) ORDER BY 1, 2',
            [source_name, *params],
        )
        rows = cur.fetchall()
    i_mat, i_plant = cols.index(MATNR_COL), cols.index("PLANT(k/*)")
    groups = {}
    for row in rows:
        keys = {"MATNR": row[i_mat], "MATNR|PLANT": f"{row[i_mat]}|{row[i_plant]}"}
        checks = [c for c, k in excluded.items()
                  if keys["MATNR|PLANT" if "PLANT" in EXCLUDE_CHECKS[c][0] else "MATNR"] in k]
        groups.setdefault((", ".join(checks), "; ".join(EXCLUDE_CHECKS[c][1] for c in checks)), []).append(row)
    for (check, reason), discarded in groups.items():
        add_discards(RAW_TABLE, cols, discarded, check, reason)


def get_distinct_sources(conn) -> list[str]:
    """Recupera i valori distinti di _source dalla tabella raw."""
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT DISTINCT "_source" FROM raw.{q(RAW_TABLE)}'
            ' WHERE "_source" IS NOT NULL ORDER BY 1'
        )
        return [r[0] for r in cur.fetchall()]


def get_columns(conn) -> list[str]:
    """Recupera le colonne della tabella escludendo le audit."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'raw' AND table_name = %s
            ORDER BY ordinal_position
        """, (RAW_TABLE,))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def get_excluded_keys(conn) -> dict[str, set]:
    """
    Chiavi in errore (status diverso da Ok) dell'ultimo run di ciascun check attivo di
    EXCLUDE_CHECKS sulle righe di S_MARITC:
    {check_id: {object_key}}. Ignorati i check disattivati o con l'esclusione dall'output
    disattivata nel catalogo.
    """
    excluded = {}
    with conn.cursor() as cur:
        for check_id in EXCLUDE_CHECKS:
            if not exclusion_enabled(cur, check_id):
                continue
            cur.execute("""
                SELECT DISTINCT object_key
                FROM stg.check_results
                WHERE check_id = %s
                  AND source_table = %s
                  AND status  != 'Ok'
                  AND run_id  = (SELECT MAX(run_id) FROM stg.check_results WHERE check_id = %s)
            """, (check_id, RAW_TABLE, check_id))
            keys = {r[0] for r in cur.fetchall()}
            if keys:
                excluded[check_id] = keys
    return excluded


def log_excluded(excluded: dict[str, set], source_name: str):
    """Logga in modo evidente le chiavi escluse, per check."""
    sep = "=" * 72
    log.warning(sep)
    log.warning("  *** Record ESCLUSI dall'output per '%s'", source_name)
    for check_id, keys in excluded.items():
        log.warning("  [%s] %d chiavi — %s", check_id, len(keys), EXCLUDE_CHECKS[check_id][1])
        for i, key in enumerate(sorted(keys), 1):
            log.warning("    %-6d %s", i, key)
    log.warning(sep)


def fetch_csv(conn, source_name: str, cols: list[str], excluded: dict[str, set]) -> bytes:
    """Legge i record filtrati per _source, escludendo in SQL le chiavi in errore."""
    sql    = f'SELECT {", ".join(q(c) for c in cols)} FROM raw.{q(RAW_TABLE)} WHERE "_source" = %s'
    params = [source_name]
    if excluded:
        cond, cond_params = exclusion_condition(excluded)
        sql    += f" AND NOT ({cond})"
        params += cond_params
    with conn.cursor() as cur:
        cur.execute(sql + " ORDER BY 1, 2", params)
        rows = cur.fetchall()
    return rows_to_csv(cols, rows, strip_cols={MATNR_COL})


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reset_discards([RAW_TABLE])
    conn = get_connection()

    try:
        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun valore di _source trovato in raw.%s — nulla da esportare.", RAW_TABLE)
            return

        cols = get_columns(conn)
        if not cols:
            log.warning("Nessuna colonna trovata per raw.%s — skip.", RAW_TABLE)
            return

        # Esclusioni caricate una volta sola (globali, non dipendono dalla sorgente)
        excluded = get_excluded_keys(conn)
        if not excluded:
            log.info("  Nessun record da escludere — output completo.")

        log.info("Sorgenti trovate: %s", sources)

        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)

            if excluded:
                log_excluded(excluded, source_name)
                write_discarded_csv(conn, source_name, cols, excluded)

            csv_bytes = fetch_csv(conn, source_name, cols, excluded)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe per '%s' — skip", RAW_TABLE, source_name)
                remove_previous_zips(OUTPUT_DIR, source_name)
                continue

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{RAW_TABLE}.csv", csv_bytes)
            buf.seek(0)

            output_path = output_zip_path(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(buf.read())
            log.info("  ✓  %s  (%d righe, %.1f KB)", output_path, n_rows, len(csv_bytes) / 1024)
        flush_discards()

    finally:
        conn.close()

    log.info("=== export_maritc_zip completato ===")


if __name__ == "__main__":
    main()
