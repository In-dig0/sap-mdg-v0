"""
Libreria comune per i controlli Python basati su una query (es. ordini di acquisto CK062-CK524,
società partner CK063, CK416, CK417).
Non è un asset Bruin — viene importata dagli asset ckNNN.

Il controllo esegue una query che restituisce una riga per oggetto controllato con le colonne
(source_table, object_key, message, zip_source): message NULL = Ok, altrimenti testo dell'errore,
scritto con la gravità del catalogo. Se una delle tabelle richieste non è caricata il controllo
viene saltato (il flusso può non essere presente in tutti gli ambienti).
"""

import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{q(table)}",))
    return cur.fetchone()[0] is not None


def run_query_check(check_id: str, sql, required: list, category: str = "PO"):
    """
    required: tabelle necessarie come (schema, tabella); se ne manca una il controllo è saltato.
    sql: query con le colonne (source_table, object_key, message, zip_source), oppure funzione
         che riceve il cursore e restituisce la query (None = niente da controllare).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(is_active, FALSE), severity FROM stg.check_catalog WHERE check_id = %s",
                        (check_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                print(f"[SKIP] {check_id} disattivato nel catalogo")
                return
            severity = row[1] or "Error"
            missing = [f"{s}.{t}" for s, t in required if not table_exists(cur, s, t)]
            if missing:
                print(f"[SKIP] {check_id} — tabelle non caricate: {', '.join(missing)}")
                return

            cur.execute("SELECT run_id FROM stg.pipeline_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1")
            run_id = cur.fetchone()[0]
            now = datetime.now(timezone.utc)

            if callable(sql):
                sql = sql(cur)
                if not sql:
                    print(f"[SKIP] {check_id} — nessuna tabella da controllare caricata")
                    return
            cur.execute(sql)
            results, n_err = [], 0
            for source_table, key, msg, source in cur.fetchall():
                if msg:
                    n_err += 1
                    results.append((source_table, category, key, check_id, msg, severity, run_id, source, now))
                else:
                    results.append((source_table, category, key, check_id, "Ok", "Ok", run_id, source, now))

            if results:
                execute_values(cur, """
                    INSERT INTO stg.check_results (
                        source_table, category, object_key, check_id,
                        message, status, run_id, zip_source, created_at
                    ) VALUES %s
                """, results, page_size=500)
        conn.commit()
        print(f"[{'WARN' if n_err else 'OK'}] {check_id}: {len(results)} righe, {n_err} segnalazioni")
    finally:
        conn.close()
