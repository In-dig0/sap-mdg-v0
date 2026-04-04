""" @bruin
name: prd.pipeline_run_close
type: python
depends:
  - stg.chk01_supplier_country
  - stg.chk02_supplier_country_region
  - stg.chk03_supplier_partita_iva
  - stg.chk04_supplier_taxnum_duplicati
  - stg.chk01_customer_country
  - stg.chk02_customer_country_region
  - stg.chk03_customer_partita_iva
  - stg.chk04_customer_taxnum_duplicati
description: >
  Chiude il record in stg.pipeline_runs al termine del run.
  Legge il run_id dal file semaforo /tmp/mdg_run_id.txt.
  Calcola i totali da stg.check_results e aggiorna il record.
@bruin """

import os
import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

SEMAPHORE_PATH = "/tmp/mdg_run_id.txt"

def main():
    # Leggi run_id dal semaforo se disponibile, altrimenti dal DB
    if os.path.exists(SEMAPHORE_PATH):
        with open(SEMAPHORE_PATH) as f:
            run_id = int(f.read().strip())
    else:
        # Fallback: leggi l'ultimo run con status 'running' dal DB
        conn_tmp = psycopg2.connect(**DB_CONFIG)
        try:
            with conn_tmp.cursor() as cur:
                cur.execute("""
                    SELECT run_id FROM stg.pipeline_runs
                    WHERE status = 'running'
                    ORDER BY started_at DESC LIMIT 1
                """)
                row = cur.fetchone()
                if not row:
                    raise RuntimeError("Nessun run attivo trovato in stg.pipeline_runs")
                run_id = row[0]
        finally:
            conn_tmp.close()

    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:

            # Totali check dall'ultimo run (check_results è già troncata a inizio run)
            cur.execute("""
                SELECT
                    COUNT(*)                                   AS checks_run,
                    COUNT(*) FILTER (WHERE status = 'Error')  AS checks_error,
                    COUNT(*) FILTER (WHERE status = 'Ok')     AS checks_ok
                FROM stg.check_results
                WHERE run_id = %s
            """, (run_id,))
            row = cur.fetchone()
            checks_run   = row[0] if row else 0
            checks_error = row[1] if row else 0
            checks_ok    = row[2] if row else 0

            # Totale record nelle tabelle raw
            cur.execute("""
                SELECT COALESCE(SUM(n_live_tup), 0)::integer
                FROM pg_stat_user_tables
                WHERE schemaname = 'raw'
            """)
            records_loaded = cur.fetchone()[0]

            # Numero tabelle raw
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'raw'
            """)
            tables_raw = cur.fetchone()[0]

            status = "success" if checks_error == 0 else "done_with_errors"

            cur.execute("""
                UPDATE stg.pipeline_runs SET
                    finished_at    = %s,
                    status         = %s,
                    records_loaded = %s,
                    checks_run     = %s,
                    checks_error   = %s,
                    checks_warning = 0,
                    notes          = %s
                WHERE run_id = %s
            """, (
                now,
                status,
                records_loaded,
                checks_run,
                checks_error,
                f"Ok: {checks_ok} | Error: {checks_error} | Tabelle raw: {tables_raw}",
                run_id,
            ))

        conn.commit()
        print(f"[OK] Pipeline run #{run_id} chiuso")
        print(f"     Status:        {status}")
        print(f"     Records raw:   {records_loaded}")
        print(f"     Checks totali: {checks_run}")
        print(f"     Checks ok:     {checks_ok}")
        print(f"     Checks error:  {checks_error}")

        os.remove(SEMAPHORE_PATH)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
