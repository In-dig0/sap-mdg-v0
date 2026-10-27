"""
Libreria comune per i controlli SAP_REF scritti in Python (CK057, CK058, CK059).
Non è un asset Bruin — viene importata dagli asset ck05x.

Un controllo verifica un campo di una tabella raw rispetto a una tabella di controllo
SAP in ref: errore se il campo è vuoto, contiene un valore segnaposto (es. '???') o un
valore non presente nella tabella SAP. Se la tabella SAP non è caricata, sono verificati
solo vuoti e segnaposto (il messaggio lo indica). Un esito per riga.
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


def run_ref_check(check_id: str, raw_table: str, key_col: str, field: str,
                  ref_table: str, ref_col: str, placeholders: tuple = (),
                  output_note: dict | None = None, skip_if: dict | None = None):
    """
    output_note: testo aggiunto al messaggio per tipo di errore, con ciò che viene
    scritto nell'output: {"vuoto": ..., "segnaposto": ..., "non_ammesso": ...}.
    skip_if: righe escluse dal controllo, {colonna: (valori, ...)} (confronto su TRIM).
    """
    note = output_note or {}
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
            if not table_exists(cur, "raw", raw_table):
                print(f"[SKIP] {check_id} — raw.{raw_table} non caricata")
                return

            allowed = None
            if table_exists(cur, "ref", ref_table):
                cur.execute(f'SELECT DISTINCT TRIM({q(ref_col)}) FROM ref.{q(ref_table)} '
                            f'WHERE COALESCE(TRIM({q(ref_col)}), \'\') <> \'\'')
                allowed = {r[0] for r in cur.fetchall()}

            cur.execute("SELECT run_id FROM stg.pipeline_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1")
            run_id = cur.fetchone()[0]
            now = datetime.now(timezone.utc)

            where, params = "", []
            if skip_if:
                where = " WHERE " + " AND ".join(
                    f"COALESCE(TRIM({q(col)}), '') <> ALL(%s)" for col in skip_if)
                params = [list(vals) for vals in skip_if.values()]
            cur.execute(f'SELECT {q(key_col)}, COALESCE(TRIM({q(field)}), \'\'), "_source" '
                        f'FROM raw.{q(raw_table)}{where}', params or None)
            results, n_err = [], 0
            for key, value, source in cur.fetchall():
                if value == "":
                    msg = f"{field} vuoto" + (f" {note['vuoto']}" if note.get("vuoto") else "")
                elif value in placeholders:
                    msg = f"{field} [{value}] non valido" + (f" {note['segnaposto']}" if note.get("segnaposto") else "")
                elif allowed is not None and value not in allowed:
                    msg = (f"{field} [{value}] non presente in {ref_table}"
                           + (f" {note['non_ammesso']}" if note.get("non_ammesso") else ""))
                else:
                    results.append((raw_table, "BP", key, check_id, "Ok", "Ok", run_id, source, now))
                    continue
                if allowed is None:
                    msg += f" (ref.{ref_table} non caricata: valori ammessi non verificati)"
                n_err += 1
                results.append((raw_table, "BP", key, check_id, msg, severity, run_id, source, now))

            if results:
                execute_values(cur, """
                    INSERT INTO stg.check_results (
                        source_table, category, object_key, check_id,
                        message, status, run_id, zip_source, created_at
                    ) VALUES %s
                """, results, page_size=500)
        conn.commit()
        print(f"[{'WARN' if n_err else 'OK'}] {check_id} — {raw_table}.{field}: {len(results)} righe, {n_err} errori"
              + ("" if allowed is not None else f" (ref.{ref_table} non caricata)"))
    finally:
        conn.close()
