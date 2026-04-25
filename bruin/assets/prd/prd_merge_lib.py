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

PRD_SCHEMA    = "prd"
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


def merge_table(cur, raw_schema: str, raw_table: str,
                stg_schema: str, stg_table: str,
                prd_table: str):
    """
    Merge raw + stg → prd.
    - Colonne comuni: COALESCE(stg.col, raw.col) — preferenza a stg
    - _source e _loaded_at: sempre da raw
    - _status esclusa da prd
    - Record stg con _status = 'DELETED' ignorati nel JOIN
      (il record raw viene portato in prd senza override stg)
    """
    raw_fqt = f'{raw_schema}.{q(raw_table)}'
    stg_fqt = f'{stg_schema}.{q(stg_table)}'
    prd_fqt = f'{PRD_SCHEMA}.{q(prd_table)}'

    log.info(f"=== Merge: {raw_fqt} + {stg_fqt} → {prd_fqt} ===")

    raw_cols = get_columns(cur, raw_schema, raw_table)
    stg_cols = get_columns(cur, stg_schema, stg_table)

    if not raw_cols:
        log.warning(f"  {raw_fqt} non trovata — skip.")
        return
    if not stg_cols:
        log.warning(f"  {stg_fqt} non trovata — uso solo raw.")
        stg_cols = []

    key_cols = get_key_columns(raw_cols)
    if not key_cols:
        log.warning(f"  Nessuna colonna chiave in {raw_fqt} — skip.")
        return
    log.info(f"  Chiavi: {key_cols}")

    stg_data_cols = [
        c for c in stg_cols
        if c not in STG_AUDIT_COLS and c not in key_cols
    ]
    raw_col_set  = set(raw_cols)
    common_cols  = set(c for c in stg_data_cols if c in raw_col_set)
    log.info(f"  Colonne comuni raw↔stg (override da stg): {len(common_cols)}")

    # SELECT: chiavi + colonne comuni con COALESCE + colonne solo raw
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

    # JOIN sulle chiavi + filtro DELETED:
    # i record stg con _status='DELETED' non partecipano al JOIN
    # → il raw viene portato in prd senza sovrascrittura
    join_cond = " AND ".join(f'raw.{q(k)} = stg.{q(k)}' for k in key_cols)
    join_cond += " AND stg.\"_status\" != 'DELETED'"

    # Escludi da prd i record raw la cui chiave è presente in stg con _status='DELETED'
    deleted_subquery = " AND ".join(
        f'd.{q(k)} = raw.{q(k)}' for k in key_cols
    )
    where_clause = f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM {stg_fqt} d
            WHERE {deleted_subquery}
              AND d."_status" = 'DELETED'
        )
    """

    cur.execute(f"DROP TABLE IF EXISTS {prd_fqt}")
    cur.execute(f"""
        CREATE TABLE {prd_fqt} AS
        SELECT
        {select_sql}
        FROM {raw_fqt} raw
        LEFT JOIN {stg_fqt} stg ON {join_cond}
        {where_clause}
    """)

    cur.execute(f"SELECT COUNT(*) FROM {prd_fqt}")
    n = cur.fetchone()[0]
    log.info(f"  ✓  {prd_fqt}: {n} record")
