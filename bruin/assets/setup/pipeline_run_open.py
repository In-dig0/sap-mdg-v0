""" @bruin
name: setup.pipeline_run_open
type: python
depends:
  - setup.init_db
  - setup.check_catalog
description: >
  Apre un nuovo record in stg.pipeline_runs usando la sequence
  stg.pipeline_run_seq come run_id progressivo (1, 2, 3...).
  Salva il run_id in /tmp/mdg_run_id.txt per pipeline_run_close.
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
    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT nextval('stg.pipeline_run_seq')")
            run_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO stg.pipeline_runs (
                    run_id, pipeline_name, started_at, status
                ) VALUES (%s, %s, %s, %s)
            """, (run_id, "mdg_migration_pipeline", now, "running"))

        conn.commit()

        with open(SEMAPHORE_PATH, "w") as f:
            f.write(str(run_id))

        print(f"[OK] Pipeline run #{run_id} aperto alle {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
