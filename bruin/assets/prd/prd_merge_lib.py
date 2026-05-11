"""
Libreria comune per i 3 asset prd.merge_*.
Non è un asset Bruin — viene importata dagli asset merge.
"""
import os
import re
import logging
import psycopg2

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

PRD_SCHEMA     = "prd"
STG_AUDIT_COLS = {"_status", "_source", "_loaded_at", "_xlsx_source"}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_columns(cur, schema: str, table: str) -> list[str]:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return [r[0] for r in cur.fetchall()]


def get_key_columns(cols: list[str]) -> list[str]:
    return [c for c in cols if re.search(r"\(.*?k.*?\)", c)]


def ensure_prd_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PRD_SCHEMA}")


def check_vat_vies_exists(cur) -> bool:
    """Verifica che stg.check_vat_vies esista."""
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'stg' AND table_name = 'check_vat_vies'
    """)
    return cur.fetchone()[0] > 0


def _log_vies_discards(cur, raw_fqt: str, raw_cols: list[str],
                       vies_lifnr_col: str):
    """
    Logga i record scartati dal filtro VIES: uno per LIFNR+TAXTYPE+TAXNUM.
    Usa GROUP BY per evitare duplicati nel caso check_vat_vies abbia
    più righe per lo stesso entity_id.
    """
    taxtype_col = "TAXTYPE(k/*)"
    taxnum_col  = "TAXNUM(*)"
    has_taxtype = taxtype_col in raw_cols
    has_taxnum  = taxnum_col  in raw_cols

    select_parts = [f'raw.{q(vies_lifnr_col)}']
    if has_taxtype:
        select_parts.append(f'raw.{q(taxtype_col)}')
    if has_taxnum:
        select_parts.append(f'raw.{q(taxnum_col)}')
    select_parts.append("MIN(v.vat_number) AS vat_number")

    group_parts = [f'raw.{q(vies_lifnr_col)}']
    if has_taxtype:
        group_parts.append(f'raw.{q(taxtype_col)}')
    if has_taxnum:
        group_parts.append(f'raw.{q(taxnum_col)}')

    taxtype_filter = (
        f"AND raw.{q(taxtype_col)} LIKE '%0'" if has_taxtype else ""
    )

    cur.execute(f"""
        SELECT {', '.join(select_parts)}
        FROM {raw_fqt} raw
        JOIN stg.check_vat_vies v
          ON v.entity_id  = raw.{q(vies_lifnr_col)}
         AND v.is_eu      = TRUE
         AND v.vies_valid = FALSE
        {taxtype_filter}
        GROUP BY {', '.join(group_parts)}
        ORDER BY raw.{q(vies_lifnr_col)}
    """)
    discarded = cur.fetchall()

    if not discarded:
        log.info("  ✓  VIES — nessun record scartato.")
        return

    col_headers = [vies_lifnr_col]
    if has_taxtype: col_headers.append(taxtype_col)
    if has_taxnum:  col_headers.append(taxnum_col)
    col_headers.append("vat_number (VIES)")

    log.warning(
        f"  ⚠  VIES — {len(discarded)} record scartati da "
        f"{raw_fqt} (TAXTYPE LIKE '%0', is_eu=true, vies_valid=false):"
    )
    header = "  " + " | ".join(f"{h:<30}" for h in col_headers)
    log.warning(header)
    log.warning("  " + "-" * (len(header) - 2))
    for row in discarded:
        log.warning("  " + " | ".join(f"{str(v):<30}" for v in row))


def merge_table(cur, raw_schema: str, raw_table: str,
                stg_schema: str, stg_table: str,
                prd_table: str,
                vies_filter: bool = False,
                vies_lifnr_col: str | None = None):
    """
    Merge raw + stg -> prd.
    - Colonne comuni: COALESCE(stg.col, raw.col) — preferenza a stg
    - _source e _loaded_at: sempre da raw
    - _status esclusa da prd
    - Record stg con _status='DELETED' esclusi dal JOIN
    - Se la tabella STG non esiste, usa solo raw (nessun JOIN).
    - vies_filter=True: esclude TAXTYPE LIKE '%0' con PIVA EU non valida.
    """
    raw_fqt = f'{raw_schema}.{q(raw_table)}'
    stg_fqt = f'{stg_schema}.{q(stg_table)}'
    prd_fqt = f'{PRD_SCHEMA}.{q(prd_table)}'

    log.info(f"=== Merge: {raw_fqt} + {stg_fqt} -> {prd_fqt} ===")

    raw_cols = get_columns(cur, raw_schema, raw_table)
    stg_cols = get_columns(cur, stg_schema, stg_table)

    if not raw_cols:
        log.warning(f"  {raw_fqt} non trovata — skip.")
        return

    has_stg = bool(stg_cols)
    if not has_stg:
        log.warning(f"  {stg_fqt} non trovata — uso solo raw (nessun JOIN STG).")

    key_cols = get_key_columns(raw_cols)
    if not key_cols:
        log.warning(f"  Nessuna colonna chiave in {raw_fqt} — skip.")
        return
    log.info(f"  Chiavi: {key_cols}")

    stg_data_cols = [
        c for c in stg_cols
        if c not in STG_AUDIT_COLS and c not in key_cols
    ]
    raw_col_set = set(raw_cols)
    common_cols = set(c for c in stg_data_cols if c in raw_col_set)
    log.info(f"  Colonne comuni raw<->stg (override da stg): {len(common_cols)}")

    # ---- SELECT ----
    select_parts = []
    for col in raw_cols:
        if col in ("_source", "_loaded_at"):
            select_parts.append(f'raw.{q(col)}')
        elif col in common_cols:
            select_parts.append(
                f'COALESCE(NULLIF(stg.{q(col)}, \'\'), raw.{q(col)}) AS {q(col)}'
            )
        else:
            select_parts.append(f'raw.{q(col)}')
    select_sql = ",\n        ".join(select_parts)

    # ---- JOIN e WHERE condizionali alla presenza di STG ----
    if has_stg:
        join_clause = (
            f"LEFT JOIN {stg_fqt} stg ON "
            + " AND ".join(f'raw.{q(k)} = stg.{q(k)}' for k in key_cols)
            + " AND stg.\"_status\" != 'DELETED'"
        )
        deleted_cond = " AND ".join(f'd.{q(k)} = raw.{q(k)}' for k in key_cols)
        where_clauses = [f"""NOT EXISTS (
            SELECT 1 FROM {stg_fqt} d
            WHERE {deleted_cond}
              AND d."_status" = 'DELETED'
        )"""]
    else:
        join_clause   = ""
        where_clauses = []

    # ---- Filtro VIES ----
    if vies_filter and vies_lifnr_col:
        if check_vat_vies_exists(cur):
            taxtype_col = "TAXTYPE(k/*)"
            if taxtype_col in raw_cols:
                log.info(
                    f"  Filtro VIES attivo su {q(vies_lifnr_col)} "
                    f"(solo TAXTYPE LIKE '%0')"
                )
                where_clauses.append(f"""NOT (
            raw.{q(taxtype_col)} LIKE '%0'
            AND EXISTS (
                SELECT 1 FROM stg.check_vat_vies v
                WHERE v.entity_id = raw.{q(vies_lifnr_col)}
                  AND v.is_eu      = TRUE
                  AND v.vies_valid = FALSE
            )
        )""")
            else:
                log.info(
                    f"  Filtro VIES attivo su {q(vies_lifnr_col)} "
                    f"(colonna TAXTYPE assente — filtro su tutti i record)"
                )
                where_clauses.append(f"""NOT EXISTS (
            SELECT 1 FROM stg.check_vat_vies v
            WHERE v.entity_id = raw.{q(vies_lifnr_col)}
              AND v.is_eu      = TRUE
              AND v.vies_valid = FALSE
        )""")
        else:
            log.warning("  stg.check_vat_vies non trovata — filtro VIES saltato.")

    where_clause = ("WHERE " + "\n          AND ".join(where_clauses)
                    if where_clauses else "")

    # ---- Log record scartati da VIES (prima del DROP, un record per PIVA) ----
    if vies_filter and vies_lifnr_col and check_vat_vies_exists(cur):
        _log_vies_discards(cur, raw_fqt, raw_cols, vies_lifnr_col)

    # ---- DROP + CREATE ----
    cur.execute(f"DROP TABLE IF EXISTS {prd_fqt}")
    cur.execute(f"""
        CREATE TABLE {prd_fqt} AS
        SELECT
        {select_sql}
        FROM {raw_fqt} raw
        {join_clause}
        {where_clause}
    """)

    cur.execute(f"SELECT COUNT(*) FROM {prd_fqt}")
    n = cur.fetchone()[0]
    log.info(f"  ✓  {prd_fqt}: {n} record")
