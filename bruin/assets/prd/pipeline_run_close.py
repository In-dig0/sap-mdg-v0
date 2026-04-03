""" @bruin
name: prd.pipeline_run_close
type: python
depends:
  - stg.chk01_country
  - stg.chk02_country_region
  - stg.chk03_partita_iva
description: >
  Chiude il record in stg.pipeline_runs al termine del run.
  Legge il run_id dal file semaforo creato da pipeline_run_open,
  calcola i totali da stg.check_results e aggiorna il record.
  Deve essere l'ultimo asset ad eseguire.
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
    # Leggi run_id dal semaforo
    if not os.path.exists(SEMAPHORE_PATH):
        raise FileNotFoundError(
            f"File semaforo non trovato: {SEMAPHORE_PATH}. "
            "Verificare che setup.pipeline_run_open sia eseguito prima."
        )

    with open(SEMAPHORE_PATH) as f:
        run_id = f.read().strip()

    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Calcola totali da check_results
            cur.execute("""
                SELECT
                    COUNT(*)                                        AS checks_run,
                    COUNT(*) FILTER (WHERE status = 'Error')       AS checks_error,
                    COUNT(*) FILTER (WHERE status = 'Ok')          AS checks_ok
                FROM stg.check_results
                WHERE run_id = %s
            """, (run_id,))
            row = cur.fetchone()
            checks_run   = row[0] if row else 0
            checks_error = row[1] if row else 0
            checks_ok    = row[2] if row else 0

            # Calcola record caricati nello schema raw
            cur.execute("""
                SELECT COUNT(*) AS records_loaded
                FROM stg.check_results
                WHERE run_id = %s
            """, (run_id,))
            row2 = cur.fetchone()
            records_loaded = row2[0] if row2 else 0

            # Aggiorna pipeline_runs
            status = "success" if checks_error == 0 else "completed_with_errors"
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
                f"Ok: {checks_ok} | Error: {checks_error}",
                run_id,
            ))
        conn.commit()
        print(f"[OK] Pipeline run chiuso: {run_id} | status={status} | "
              f"checks={checks_run} | errors={checks_error}")

        # Rimuovi semaforo
        os.remove(SEMAPHORE_PATH)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
