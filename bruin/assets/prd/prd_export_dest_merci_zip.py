""" @bruin
name: prd.export_dest_merci_zip
type: python
description: >
  Genera gli archivi ZIP di output per i destinatari merci combinando:
    - prd."S_CUST_GEN#ZDM_DatiGenerali"  (dati anagrafici con indirizzi normalizzati)
    - tutte le altre tabelle dest. merci da schema raw
  Un ZIP separato viene creato per ogni valore distinto di _source
  trovato in prd."S_CUST_GEN#ZDM_DatiGenerali".
  I CSV dentro il ZIP usano separatore ";" e non includono colonne audit.
  Output: /project/datalake/out_source_mdg/<nome_source>.zip
depends:
  - prd.merge_dest_merci
  - prd.merge_taxnumbers_dest_merci
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

AUDIT_COLS = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}

# Tabelle da schema prd che sostituiscono le rispettive versioni raw.
# L'ordine determina la sequenza di scrittura nel ZIP.
PRD_TABLES = [
    ("prd", "S_CUST_GEN#ZDM_DatiGenerali"),
    ("prd", "S_CUST_TAXNUMBERS#ZDM-CodiciFisc"),
]

# Tabelle da raw (filtrate per _source)
RAW_TABLES = [
    "S_CUST_GEN#ZDM-DatiGenerali",            # sostituita da prd
    "S_CUST_SALES_DATA#ZDM-DatiVendite",
    "S_CUST_SALES_PARTNER#ZDM-Partner",
    "S_CUST_TAXNUMBERS#ZDM-CodiciFisc",        # sostituita da prd
    "S_ROLES#ZBP-RuoliDM",
]

# Mappa: nome raw → tabella prd sostitutiva
RAW_REPLACED_BY_PRD = {
    "S_CUST_GEN#ZDM-DatiGenerali",         # con -
    "S_CUST_TAXNUMBERS#ZDM-CodiciFisc",
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_distinct_sources(conn) -> list[str]:
    schema, table = PRD_TABLES[0]
    fqt = f'{schema}.{q(table)}'
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT DISTINCT "_source" FROM {fqt} WHERE "_source" IS NOT NULL ORDER BY 1'
        )
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall() if r[0] not in AUDIT_COLS]


def get_orphan_keys(conn, check_id: str) -> set:
    """
    Recupera le chiavi (LIFNR o KUNNR) orfane dall'ultimo run del check_id
    specificato in stg.check_results (status != 'Ok').
    Ritorna un set vuoto se il check non è attivo o non ha risultati.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(is_active, FALSE)
            FROM stg.check_catalog WHERE check_id = %s
        """, (check_id,))
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
        """, (check_id, check_id))
        return {r[0] for r in cur.fetchall()}


def fetch_table_csv(conn, schema: str, table: str,
                    source_filter: str, cols: list[str],
                    excluded_keys: set | None = None,
                    fk_col: str | None = None) -> bytes:
    fqt      = f'{schema}.{q(table)}'
    col_list = ", ".join(q(c) for c in cols)
    with conn.cursor() as cur:
        # Esclude in SQL i record orfani (solo tabelle secondarie)
        if excluded_keys and fk_col and fk_col in cols:
            fk_quoted = q(fk_col)
            placeholders = ",".join(["%s"] * len(excluded_keys))
            cur.execute(
                f'SELECT {col_list} FROM {fqt}'
                f' WHERE "_source" = %s'
                f' AND {fk_quoted} NOT IN ({placeholders})'
                f' ORDER BY 1',
                (source_filter, *excluded_keys)
            )
            n_excl = len(excluded_keys)
            log.warning(
                "  [%s] %s.%s: %d record orfani esclusi (_source=%s)",
                fk_col, schema, table, n_excl, source_filter
            )
        else:
            cur.execute(
                f'SELECT {col_list} FROM {fqt} WHERE "_source" = %s ORDER BY 1',
                (source_filter,)
            )
        rows = cur.fetchall()

    def sanitize(val):
        """Converte None, float NaN e stringa 'NaN' in stringa vuota."""
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


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        return cur.fetchone() is not None


def build_zip(conn, source_name: str) -> bytes:
    buf = io.BytesIO()

    # Carica le chiavi orfane (CK403) — solo tabelle secondarie raw
    orphan_keys = get_orphan_keys(conn, "CK403")
    if orphan_keys:
        log.warning(
            "  *** [CK403] %d chiavi orfane per '%s' "
            "— verranno escluse dalle tabelle secondarie: %s",
            len(orphan_keys), source_name, sorted(orphan_keys)
        )
    else:
        log.info("  [CK403] Nessuna chiave orfana per '%s'.", source_name)

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

        # 1. Tabelle da prd (ordine definito in PRD_TABLES)
        for prd_schema, prd_table in PRD_TABLES:
            prd_cols = get_columns(conn, prd_schema, prd_table)
            if prd_cols:
                csv_bytes = fetch_table_csv(conn, prd_schema, prd_table, source_name, prd_cols)
                n_rows = csv_bytes.count(b"\r\n") - 1
                zf.writestr(f"{prd_table}.csv", csv_bytes)
                log.info(f"  + {prd_schema}.{prd_table}: {n_rows} righe")
            else:
                log.warning(f"  Nessuna colonna trovata per {prd_schema}.{prd_table}")

        # 2. Tabelle da raw (skip quelle già prese da prd)
        for raw_table in RAW_TABLES:
            if raw_table in RAW_REPLACED_BY_PRD:
                log.info(f"  SKIP raw.{raw_table} — sostituito da prd")
                continue

            if not table_exists(conn, "raw", raw_table):
                log.warning(f"  SKIP raw.{raw_table} — tabella non trovata")
                continue

            raw_cols = get_columns(conn, "raw", raw_table)
            if not raw_cols:
                log.warning(f"  SKIP raw.{raw_table} — nessuna colonna")
                continue

            _is_master = raw_table in {'S_CUST_GEN#ZDM-DatiGenerali'}
            if not _is_master and orphan_keys:
                write_discarded_csv(conn, raw_table, source_name,
                                   raw_cols, orphan_keys, "KUNNR(k/*)")
            csv_bytes = fetch_table_csv(
                conn, "raw", raw_table, source_name, raw_cols,
                excluded_keys=None if _is_master else orphan_keys,
                fk_col=None if _is_master else "KUNNR(k/*)",
            )
            n_rows = csv_bytes.count(b"\r\n") - 1
            if n_rows <= 0:
                log.info(f"  raw.{raw_table}: 0 righe per '{source_name}' — skip")
                continue

            zf.writestr(f"{raw_table}.csv", csv_bytes)
            log.info(f"  + raw.{raw_table}: {n_rows} righe")

    buf.seek(0)
    return buf.read()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_connection()

    try:
        sources = get_distinct_sources(conn)
        if not sources:
            log.warning("Nessun valore di _source trovato in prd — nulla da esportare.")
            return

        log.info(f"Sorgenti trovate: {sources}")

        for source_name in sources:
            log.info(f"=== Generazione ZIP: {source_name} ===")
            zip_bytes   = build_zip(conn, source_name)
            output_path = os.path.join(OUTPUT_DIR, source_name)
            with open(output_path, "wb") as f:
                f.write(zip_bytes)
            size_kb = len(zip_bytes) / 1024
            log.info(f"  ✓  {output_path}  ({size_kb:.1f} KB)")

    finally:
        conn.close()

    log.info("=== export_dest_merci_zip completato ===")


if __name__ == "__main__":
    main()
