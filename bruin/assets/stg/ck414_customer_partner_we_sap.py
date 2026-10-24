""" @bruin
name: stg.ck414_customer_partner_we_sap
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK414 — CROSS_TABLE: Clienti (S_CUST_SALES_PARTNER#ZBP-Partner), partner destinatario
  merci PARVW(k/*) = 'WE'. Nello ZIP clienti il partner KUNN2(k) è trascodificato con il
  codice SAP del DM (ref.SAP_EXPORT_BUT000, campo BU_SORT2): segnala le righe WE
  - con KUNN2(k) vuoto;
  - che puntano a un destinatario merci (S_CUST_GEN#ZDM-DatiGenerali) senza codice SAP
    in SAP_EXPORT_BUT000 (al primo caricamento, prima dell'estrazione BUT000, tutte):
    nello ZIP clienti resterebbe il vecchio codice.
  Un esito per riga WE (object_key = KUNNR, partner nel messaggio).
@bruin """

import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

CHECK_ID      = "CK414"
PARTNER_TABLE = "S_CUST_SALES_PARTNER#ZBP-Partner"
DM_TABLE      = "S_CUST_GEN#ZDM-DatiGenerali"
BUT000_TABLE  = "SAP_EXPORT_BUT000"

def q(name): return '"' + name.replace('"', '""') + '"'

def table_exists(cur, schema, table):
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{q(table)}",))
    return cur.fetchone()[0] is not None

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = %s", (CHECK_ID,))
            row = cur.fetchone()
            if not row or not row[0]:
                print(f"[SKIP] {CHECK_ID} disattivato nel catalogo")
                return
            if not table_exists(cur, "raw", PARTNER_TABLE):
                print(f"[SKIP] {CHECK_ID} — raw.{PARTNER_TABLE} non caricata")
                return

            cur.execute("SELECT run_id FROM stg.pipeline_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1")
            run_id = cur.fetchone()[0]
            now    = datetime.now(timezone.utc)

            cur.execute("SELECT severity FROM stg.check_catalog WHERE check_id = %s", (CHECK_ID,))
            sev_row  = cur.fetchone()
            severity = sev_row[0] if sev_row else "Warning"

            # Codici DM e codici DM con nuovo codice SAP (vuoti se le tabelle non sono caricate)
            dm_codes = set()
            if table_exists(cur, "raw", DM_TABLE):
                cur.execute(f'SELECT "KUNNR(k/*)" FROM raw.{q(DM_TABLE)}')
                dm_codes = {r[0] for r in cur.fetchall()}
            sap_codes = {}
            but000_loaded = table_exists(cur, "ref", BUT000_TABLE)
            if but000_loaded:
                cur.execute(f'''SELECT TRIM("BU_SORT2"), "PARTNER" FROM ref.{q(BUT000_TABLE)}
                                WHERE COALESCE(TRIM("BU_SORT2"), '') <> \'\'''')
                sap_codes = dict(cur.fetchall())

            cur.execute(f'''
                SELECT "KUNNR(k/*)", COALESCE(TRIM("KUNN2(k)"), ''), "_source"
                FROM raw.{q(PARTNER_TABLE)}
                WHERE "PARVW(k/*)" = 'WE'
            ''')
            results = []
            n_issue = 0
            for kunnr, kunn2, source in cur.fetchall():
                if kunn2 == "":
                    message = "Partner WE senza destinatario merci (KUNN2 vuoto)"
                elif kunn2 in dm_codes and kunn2 not in sap_codes:
                    message = (
                        f"Partner WE [{kunn2}]: destinatario merci senza codice SAP in {BUT000_TABLE} "
                        + ("(BU_SORT2)" if but000_loaded else "(tabella non caricata)")
                        + " — nello ZIP clienti resta il codice originale"
                    )
                else:
                    results.append((PARTNER_TABLE, "BP", kunnr, CHECK_ID, "Ok", "Ok", run_id, source, now))
                    continue
                n_issue += 1
                results.append((PARTNER_TABLE, "BP", kunnr, CHECK_ID, message, severity, run_id, source, now))

            if results:
                execute_values(cur, """
                    INSERT INTO stg.check_results (
                        source_table, category, object_key, check_id,
                        message, status, run_id, zip_source, created_at
                    ) VALUES %s
                """, results, page_size=500)

        conn.commit()
        print(f"[{'WARN' if n_issue else 'OK'}] {CHECK_ID} — righe WE: {len(results)}, da verificare: {n_issue}"
              + ("" if but000_loaded else f" (ref.{BUT000_TABLE} non caricata)"))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
