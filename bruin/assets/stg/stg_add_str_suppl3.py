""" @bruin
name: stg.add_str_suppl3
type: python
description: >
  Aggiunge la colonna STR_SUPPL3 (compilata a mano quando l'indirizzo supera i 60
  caratteri di STREET) alle tabelle STG dei dati generali di clienti, destinatari
  merci e fornitori, subito dopo STREET. Idempotente: agisce solo sulle tabelle in cui
  la colonna manca. Per ciascuna, in un'unica transazione:
    - backup della tabella (stg."<tabella>__backup_<AAAAMMGG>");
    - nuova tabella con lo stesso schema e STR_SUPPL3 dopo STREET (stesso tipo);
    - copia dei dati e verifica (stesso numero di righe, nessuna differenza);
    - sostituzione della tabella e ricreazione degli indici.
  STR_SUPPL3 passa poi a prd con il merge (override da STG) e negli ZIP di output.
depends:
  - ingestion.ingest_others_to_stg
@bruin """

import os
import logging
from datetime import date

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

SCHEMA     = "stg"
TABLES     = [
    "S_CUST_GEN#ZBP_DatiGenerali_STG",
    "S_CUST_GEN#ZDM_DatiGenerali_STG",
    "S_SUPPL_GEN#ZBP_DatiGenerali_STG",
]
NEW_COL    = "STR_SUPPL3"
AFTER_COL  = "STREET"


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_columns(cur, table: str) -> list[tuple]:
    """[(nome, tipo, not_null, default)] nell'ordine della tabella."""
    cur.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
               pg_get_expr(d.adbin, d.adrelid)
        FROM pg_attribute a
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """, (f"{SCHEMA}.{q(table)}",))
    return cur.fetchall()


def add_column(cur, table: str):
    fqt  = f"{SCHEMA}.{q(table)}"
    cols = get_columns(cur, table)
    names = [c[0] for c in cols]
    if NEW_COL in names:
        log.info(f"  {fqt}: {NEW_COL} già presente — nulla da fare")
        return
    if AFTER_COL not in names:
        raise RuntimeError(f"{fqt}: colonna {AFTER_COL} non trovata")

    # 1. Backup
    backup = f"{table}__backup_{date.today():%Y%m%d}"
    cur.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.{q(backup)}",))
    if cur.fetchone()[0] is not None:
        raise RuntimeError(f"{SCHEMA}.{backup} esiste già: rinominarla o eliminarla prima di procedere")
    cur.execute(f"CREATE TABLE {SCHEMA}.{q(backup)} AS SELECT * FROM {fqt}")
    cur.execute(f"SELECT COUNT(*) FROM {fqt}")
    n_rows = cur.fetchone()[0]
    log.info(f"  {fqt}: backup in {SCHEMA}.{backup} ({n_rows} righe)")

    # 2. Nuova tabella con NEW_COL dopo AFTER_COL (stesso tipo)
    cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
                (SCHEMA, table))
    indexes = cur.fetchall()
    defs = []
    for name, typ, not_null, default in cols:
        defs.append(f"{q(name)} {typ}" + (" NOT NULL" if not_null else "")
                    + (f" DEFAULT {default}" if default else ""))
        if name == AFTER_COL:
            defs.append(f"{q(NEW_COL)} {typ}")
    tmp = f"{table}__new"
    cur.execute(f"CREATE TABLE {SCHEMA}.{q(tmp)} ({', '.join(defs)})")
    col_list = ", ".join(q(n) for n in names)
    cur.execute(f"INSERT INTO {SCHEMA}.{q(tmp)} ({col_list}) SELECT {col_list} FROM {fqt}")

    # 3. Verifica: stesso numero di righe e nessuna differenza sulle colonne esistenti
    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{q(tmp)}")
    n_new = cur.fetchone()[0]
    cur.execute(f"""
        SELECT COUNT(*) FROM (
            (SELECT {col_list} FROM {fqt} EXCEPT ALL SELECT {col_list} FROM {SCHEMA}.{q(tmp)})
            UNION ALL
            (SELECT {col_list} FROM {SCHEMA}.{q(tmp)} EXCEPT ALL SELECT {col_list} FROM {fqt})
        ) d
    """)
    n_diff = cur.fetchone()[0]
    if n_new != n_rows or n_diff:
        raise RuntimeError(f"{fqt}: verifica fallita (righe {n_rows} -> {n_new}, differenze {n_diff})")

    # 4. Sostituzione e indici
    cur.execute(f"DROP TABLE {fqt}")
    cur.execute(f"ALTER TABLE {SCHEMA}.{q(tmp)} RENAME TO {q(table)}")
    for _, indexdef in indexes:
        cur.execute(indexdef)
    log.info(f"  {fqt}: {NEW_COL} aggiunta dopo {AFTER_COL} — {n_new} righe verificate, "
             f"{len(indexes)} indici ricreati")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        for table in TABLES:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f"{SCHEMA}.{q(table)}",))
                if cur.fetchone()[0] is None:
                    log.info(f"  {SCHEMA}.{table}: tabella non presente — skip")
                    continue
                try:
                    add_column(cur, table)
                    conn.commit()
                except Exception:
                    conn.rollback()   # la tabella resta com'era
                    raise
    finally:
        conn.close()
    log.info("=== stg.add_str_suppl3 completato ===")


if __name__ == "__main__":
    main()
