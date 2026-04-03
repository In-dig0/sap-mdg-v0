""" @bruin
name: setup.pipeline_run_open
type: python
description: >
  Apre un nuovo record in stg.pipeline_runs all'inizio del run.
  Genera il run_id come timestamp YYYYMMDD_HH24MISS e lo salva
  in un file semaforo condiviso con pipeline_run_close.
  Deve essere il primo asset ad eseguire.
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
    now     = datetime.now(timezone.utc)
    run_id  = now.strftime("%Y%m%d_%H%M%S")

    # Salva run_id nel file semaforo per pipeline_run_close
    with open(SEMAPHORE_PATH, "w") as f:
        f.write(run_id)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stg.pipeline_runs (
                    run_id, pipeline_name, started_at, status
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
            """, (run_id, "mdg_migration_pipeline", now, "running"))
        conn.commit()
        print(f"[OK] Pipeline run aperto: {run_id}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
