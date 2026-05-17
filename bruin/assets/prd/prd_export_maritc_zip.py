""" @bruin
name: prd.export_maritc_zip
type: python
description: >
  Genera l'archivio ZIP di output per i codici doganali (S_MARITC).
  La tabella viene letta dallo schema raw.
  I record con coppia MATNR(k/*)+PLANT(k/*) orfana (CK506: coppia assente
  in S_MARC) vengono esclusi dall'output e segnalati in modo evidente nel log.
  Il CSV usa separatore ";" e non include colonne audit.
  Output: /project/datalake/out_source_mdg/<_source>
depends:
  - stg.ck511_maritc_mara
@bruin """

import os
import io
import csv
import zipfile
import logging
import math
import psycopg2

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
CHECK_ID   = "CK511"
FK_COL     = "MATNR(k/*)"        # colonna usata come chiave orfani in check_results

# Colonne audit da escludere dai CSV di output
AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def write_discarded_csv(conn, table: str, source_name: str,
                        cols: list, orphan_keys: set, fk_col: str):
    """
    Scrive i record scartati (orfani) in un file CSV nella cartella OUTPUT_DIR.
    Nome file: <table>_discarded.csv  (es. S_ROLES#ZBP_RuoliFornitori_discarded.csv)
    Se non ci sono orfani o la colonna chiave non è presente, non scrive nulla.
    """
    if not orphan_keys or fk_col not in cols:
        return
    fqt      = "raw." + q(table)
    col_list = ", ".join(q(c) for c in cols)
    placeholders = ",".join(["%s"] * len(orphan_keys))
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_list} FROM {fqt}'
            f' WHERE "_source" = %s'
            f' AND {q(fk_col)} IN ({placeholders})'
            f' ORDER BY 1',
            (source_name, *orphan_keys)
        )
        rows = cur.fetchall()
    if not rows:
        return
    import io as _io, csv as _csv, math as _math
    def sanitize(val):
        if val is None: return ""
        if isinstance(val, float) and _math.isnan(val): return ""
        if isinstance(val, str) and val.strip() == "NaN": return ""
        return val
    buf = _io.StringIO()
    writer = _csv.writer(buf, delimiter=";", quoting=_csv.QUOTE_MINIMAL,
                         lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    safe_table = table.replace("#", "_").replace("/", "_")
    out_path   = os.path.join(OUTPUT_DIR, f"{safe_table}_discarded.csv")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    log.warning(
        "  [DISCARDED] %s: %d record orfani scritti in %s",
        table, len(rows), out_path
    )


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


def get_orphan_keys(conn) -> set:
    """
    Recupera le chiavi orfane dall'ultimo run di CK506 in stg.check_results.
    CK506 segnala coppie MATNR+PLANT assenti in S_MARC — come object_key
    viene usato MATNR(k/*), quindi filtriamo su quello.
    Ritorna set vuoto se il check non è attivo o non ha risultati.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(is_active, FALSE)
            FROM stg.check_catalog WHERE check_id = %s
        """, (CHECK_ID,))
        row = cur.fetchone()
        if not row or not row[0]:
            return set()
        cur.execute("""
            SELECT DISTINCT object_key
            FROM stg.check_results
            WHERE check_id = %s
              AND status  != 'Ok'
              AND run_id  = (
                  SELECT MAX(run_id) FROM stg.check_results
                  WHERE check_id = %s
              )
        """, (CHECK_ID, CHECK_ID))
        return {r[0] for r in cur.fetchall()}


def log_orphans(orphan_keys: set, source_name: str):
    """Logga in modo evidente le chiavi orfane rimosse."""
    sep = "=" * 72
    log.warning(sep)
    log.warning(
        "  *** [%s] %d record ESCLUSI dall'output per '%s'",
        CHECK_ID, len(orphan_keys), source_name
    )
    log.warning(
        "  Motivo: %s — MATNR assente in S_MARA (PRODUCT(k/*))", CHECK_ID
    )
    log.warning("  %-6s %-40s", "#", FK_COL)
    log.warning("  %s %s", "-" * 6, "-" * 40)
    for i, key in enumerate(sorted(orphan_keys), 1):
        log.warning("  %-6d %s", i, key)
    log.warning("  Totale: %d record rimossi dall'archivio ZIP.", len(orphan_keys))
    log.warning(sep)


def fetch_csv(conn, source_name: str, cols: list[str],
              orphan_keys: set) -> bytes:
    """
    Legge i record filtrati per _source, escludendo in SQL le chiavi orfane.
    """
    fqt      = f'raw.{q(RAW_TABLE)}'
    col_list = ", ".join(q(c) for c in cols)

    with conn.cursor() as cur:
        if orphan_keys:
            placeholders = ",".join(["%s"] * len(orphan_keys))
            cur.execute(
                f'SELECT {col_list} FROM {fqt}'
                f' WHERE "_source" = %s'
                f' AND {q(FK_COL)} NOT IN ({placeholders})'
                f' ORDER BY 1',
                (source_name, *orphan_keys)
            )
        else:
            cur.execute(
                f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s ORDER BY 1',
                (source_name,)
            )
        rows = cur.fetchall()

    def sanitize(val):
        if val is None:
            return ""
        if isinstance(val, float) and math.isnan(val):
            return ""
        if isinstance(val, str) and val.strip() == "NaN":
            return ""
        return val

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    writer.writerow(cols)
    writer.writerows([tuple(sanitize(v) for v in row) for row in rows])
    return buf.getvalue().encode("utf-8")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
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

        # Carica orfani una volta sola (sono globali, non dipendono dalla sorgente)
        orphan_keys = get_orphan_keys(conn)
        if orphan_keys:
            log.warning(
                "  [%s] %d chiavi orfane trovate — verranno escluse da tutti i ZIP.",
                CHECK_ID, len(orphan_keys)
            )
        else:
            log.info("  [%s] Nessuna chiave orfana — output completo.", CHECK_ID)

        log.info("Sorgenti trovate: %s", sources)

        for source_name in sources:
            log.info("=== Generazione ZIP: %s ===", source_name)

            if orphan_keys:
                log_orphans(orphan_keys, source_name)
                write_discarded_csv(conn, RAW_TABLE, source_name,
                                   cols, orphan_keys, FK_COL)

            csv_bytes = fetch_csv(conn, source_name, cols, orphan_keys)
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info("  raw.%s: 0 righe per '%s' — skip", RAW_TABLE, source_name)
                continue

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{RAW_TABLE}.csv", csv_bytes)
            buf.seek(0)

            output_path = os.path.join(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(buf.read())
            log.info("  ✓  %s  (%d righe, %.1f KB)", output_path, n_rows, len(csv_bytes) / 1024)

    finally:
        conn.close()

    log.info("=== export_maritc_zip completato ===")


if __name__ == "__main__":
    main()
