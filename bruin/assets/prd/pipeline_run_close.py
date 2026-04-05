""" @bruin
name: prd.pipeline_run_close
type: python
depends:
  - stg.ck001_supplier_country
  - stg.ck002_supplier_country_region
  - stg.ck003_customer_country
  - stg.ck004_customer_country_region
  - stg.ck201_supplier_piva
  - stg.ck202_supplier_taxnum_duplicati
  - stg.ck203_customer_piva
  - stg.ck204_customer_taxnum_duplicati
  - stg.ck401_zbp_vettori
  - stg.ck402_zbp_fornitori
  - stg.ck403_zdm_clienti
  - stg.ck404_zbp_clienti
description: >
  Chiude il record in stg.pipeline_runs al termine del run.
  Legge il run_id dal file semaforo /tmp/mdg_run_id.txt.
  Fallback: legge l'ultimo run attivo dal DB.
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
    if os.path.exists(SEMAPHORE_PATH):
        with open(SEMAPHORE_PATH) as f:
            run_id = int(f.read().strip())
    else:
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

            cur.execute("""
                SELECT COALESCE(SUM(n_live_tup), 0)::integer
                FROM pg_stat_user_tables WHERE schemaname = 'raw'
            """)
            records_loaded = cur.fetchone()[0]

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
                now, status, records_loaded, checks_run, checks_error,
                f"Ok: {checks_ok} | Error: {checks_error} | Tabelle raw: {tables_raw}",
                run_id,
            ))

        conn.commit()
        print(f"[OK] Run #{run_id} chiuso — status={status} | checks={checks_run} | errors={checks_error}")

        if os.path.exists(SEMAPHORE_PATH):
            os.remove(SEMAPHORE_PATH)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
