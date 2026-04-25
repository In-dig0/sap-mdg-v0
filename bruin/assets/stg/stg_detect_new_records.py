""" @bruin
name: stg.detect_new_records
type: python
description: >
  Confronta le tabelle raw (dati ERP) con le corrispondenti tabelle stg
  (indirizzi normalizzati) per rilevare nuovi record ad ogni run.
  Logica:
    1. Tutti i record _status='NEW' in stg → aggiornati a 'EXISTING'
    2. Record presenti in raw ma assenti in stg (per chiave k) → INSERT con _status='NEW'
  Le colonne chiave sono rilevate automaticamente dal suffisso (k) nel nome.
depends:
  - ingestion.ingest_others_to_stg
@bruin """

import os
import re
import logging
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

# Coppie (raw_table, stg_table) da confrontare
# Nota: i nomi sono case-sensitive e vanno tra virgolette doppie in PG
TABLE_PAIRS = [
    (
        'raw',  'S_SUPPL_GEN#ZBP_DatiGenerali',
        'stg',  'S_SUPPL_GEN#ZBP_DatiGenerali_STG',
    ),
    (
        'raw',  'S_CUST_GEN#ZBP-DatiGenerali',
        'stg',  'S_CUST_GEN#ZBP_DatiGenerali_STG',
    ),
    (
        'raw',  'S_CUST_GEN#ZDM-DatiGenerali',
        'stg',  'S_CUST_GEN#ZDM_DatiGenerali_STG',
    ),
]


def q(name: str) -> str:
    """Virgolette doppie per identificatori PostgreSQL."""
    return '"' + name.replace('"', '""') + '"'


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_key_columns(cur, schema: str, table: str) -> list[str]:
    """
    Recupera le colonne chiave della tabella (quelle con '(k' nel nome).
    Le cerca prima in stg, poi in raw se stg non le ha.
    """
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    all_cols = [r[0] for r in cur.fetchall()]
    key_cols = [c for c in all_cols if re.search(r"\(.*?k.*?\)", c)]
    return key_cols


def get_stg_columns(cur, stg_schema: str, stg_table: str) -> list[str]:
    """Ritorna tutte le colonne della tabella stg (esclude le colonne audit)."""
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (stg_schema, stg_table))
    return [
        r[0] for r in cur.fetchall()
        if not r[0].startswith("_")
    ]


def reset_new_to_existing(cur, stg_schema: str, stg_table: str) -> int:
    """Step 1: aggiorna tutti i NEW → EXISTING."""
    fqt = f'{stg_schema}.{q(stg_table)}'
    cur.execute(f"""
        UPDATE {fqt}
        SET "_status" = 'EXISTING'
        WHERE "_status" = 'NEW'
    """)
    return cur.rowcount


def insert_new_records(cur, raw_schema: str, raw_table: str,
                        stg_schema: str, stg_table: str,
                        key_cols: list[str], stg_cols: list[str]) -> int:
    """
    Step 2: inserisce in stg i record presenti in raw ma assenti in stg
    per chiave, con _status='NEW'.
    Solo le colonne presenti in entrambe le tabelle vengono copiate.
    """
    raw_fqt = f'{raw_schema}.{q(raw_table)}'
    stg_fqt = f'{stg_schema}.{q(stg_table)}'

    # Colonne disponibili in raw
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (raw_schema, raw_table))
    raw_cols_set = {r[0] for r in cur.fetchall()}

    # Intersezione: colonne presenti in entrambe (escludendo audit)
    common_cols = [c for c in stg_cols if c in raw_cols_set]

    if not common_cols:
        log.warning(f"  Nessuna colonna comune tra {raw_table} e {stg_table} — skip.")
        return 0

    # Condizione JOIN sulle chiavi
    join_cond = " AND ".join(
        f'raw.{q(k)} = stg.{q(k)}'
        for k in key_cols
    )
    # Condizione NOT EXISTS: record in raw senza corrispondenza in stg
    not_exists_cond = " AND ".join(
        f'stg.{q(k)} = raw.{q(k)}'
        for k in key_cols
    )

    col_list    = ", ".join(q(c) for c in common_cols)
    col_list_raw = ", ".join(f'raw.{q(c)}' for c in common_cols)

    source_value = f"{raw_schema}.{raw_table}"

    sql = f"""
        INSERT INTO {stg_fqt} ({col_list}, "_source", "_status", "_loaded_at")
        SELECT {col_list_raw}, %s, 'NEW', NOW()
        FROM {raw_fqt} raw
        WHERE NOT EXISTS (
            SELECT 1 FROM {stg_fqt} stg
            WHERE {not_exists_cond}
        )
    """
    cur.execute(sql, (source_value,))
    return cur.rowcount


def ensure_audit_columns(cur, stg_schema: str, stg_table: str):
    """Aggiunge _source, _status, _loaded_at se non esistono."""
    fqt = f'{stg_schema}.{q(stg_table)}'
    cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS \"_source\" TEXT")
    cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS \"_status\" TEXT DEFAULT 'EXISTING'")
    cur.execute(f"ALTER TABLE {fqt} ADD COLUMN IF NOT EXISTS \"_loaded_at\" TIMESTAMPTZ")


def process_pair(conn, raw_schema: str, raw_table: str,
                  stg_schema: str, stg_table: str):
    raw_fqt = f'{raw_schema}.{q(raw_table)}'
    stg_fqt = f'{stg_schema}.{q(stg_table)}'
    log.info(f"=== {raw_fqt}  →  {stg_fqt} ===")

    with conn.cursor() as cur:
        # Sicurezza: assicura che le colonne audit esistano
        ensure_audit_columns(cur, stg_schema, stg_table)
        conn.commit()

        # Recupera chiavi da stg (hanno il suffisso (k))
        key_cols = get_key_columns(cur, stg_schema, stg_table)
        if not key_cols:
            # Fallback: cerca le chiavi in raw
            key_cols = get_key_columns(cur, raw_schema, raw_table)
        if not key_cols:
            log.warning(f"  Nessuna colonna chiave trovata — skip.")
            return
        log.info(f"  Chiavi: {key_cols}")

        # Recupera colonne stg (senza audit)
        stg_cols = get_stg_columns(cur, stg_schema, stg_table)

        # Step 1: NEW → EXISTING
        n_reset = reset_new_to_existing(cur, stg_schema, stg_table)
        log.info(f"  Step 1 — NEW → EXISTING: {n_reset} record aggiornati")

        # Step 2: inserisci nuovi da raw
        n_new = insert_new_records(
            cur, raw_schema, raw_table,
            stg_schema, stg_table,
            key_cols, stg_cols,
        )
        log.info(f"  Step 2 — Nuovi record inseriti: {n_new}")
        conn.commit()


def main():
    conn = get_connection()
    try:
        for raw_schema, raw_table, stg_schema, stg_table in TABLE_PAIRS:
            process_pair(conn, raw_schema, raw_table, stg_schema, stg_table)
    finally:
        conn.close()
    log.info("=== detect_new_records completato ===")


if __name__ == "__main__":
    main()
