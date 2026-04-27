""" @bruin
name: stg.ck404_zbp_clienti
type: python
depends:
  - stg.clean_check_results
description: >
  CK404 — CROSS_TABLE: flusso 03-ZBP-Clienti.
  Verifica che ogni KUNNR nelle tabelle secondarie del flusso
  esista nella master S_CUST_GEN#ZBP-DatiGenerali.
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

CHECK_ID     = "CK404"
ZIP_PREFIX   = "03-ZBP-Clienti"
MASTER_TABLE = "S_CUST_GEN#ZBP-DatiGenerali"
MASTER_FK    = "KUNNR(k/*)"
SECONDARY_FK = "KUNNR(k/*)"

def q(name): return '"' + name.replace('"', '""') + '"'

def get_secondary_tables(cur):
    cur.execute("""
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND column_name = %s
          AND table_name != %s
        ORDER BY table_name
    """, (SECONDARY_FK, MASTER_TABLE))
    return [r[0] for r in cur.fetchall()]

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = %s", (CHECK_ID,))
            row = cur.fetchone()
            if not row or not row[0]:
                print(f"[SKIP] {CHECK_ID} disattivato nel catalogo")
                return

            cur.execute("SELECT run_id FROM stg.pipeline_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1")
            run_id = cur.fetchone()[0]
            now    = datetime.now(timezone.utc)

            secondary_tables = get_secondary_tables(cur)
            total_orphans = 0

            for sec_table in secondary_tables:
                # Errori: SECONDARY_FK non presente nella master
                cur.execute(f"""
                    SELECT DISTINCT sec.{q(SECONDARY_FK)}, sec."_source"
                    FROM raw.{q(sec_table)} sec
                    WHERE sec."_source" ILIKE %s
                      AND sec.{q(SECONDARY_FK)} IS NOT NULL
                      AND sec.{q(SECONDARY_FK)} <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM raw.{q(MASTER_TABLE)} mst
                          WHERE mst.{q(MASTER_FK)} = sec.{q(SECONDARY_FK)}
                      )
                """, (f"{ZIP_PREFIX}%",))

                rows_err = []
                for rec in cur.fetchall():
                    rows_err.append((
                        sec_table, 'BP', rec[0], CHECK_ID,
                        f"[{SECONDARY_FK.split('(')[0]}={rec[0]}] presente in {sec_table} "
                        f"ma assente nella master {MASTER_TABLE}",
                        'Error', run_id, rec[1], now,
                    ))

                if rows_err:
                    execute_values(cur, """
                        INSERT INTO stg.check_results (
                            source_table, category, object_key, check_id,
                            message, status, run_id, zip_source, created_at
                        ) VALUES %s
                    """, rows_err, page_size=500)
                    total_orphans += len(rows_err)
                    print(f"[WARN] {CHECK_ID} — {sec_table}: {len(rows_err)} orfani")

                # Ok: SECONDARY_FK presente nella master
                cur.execute(f"""
                    SELECT DISTINCT sec.{q(SECONDARY_FK)}, sec."_source"
                    FROM raw.{q(sec_table)} sec
                    WHERE sec."_source" ILIKE %s
                      AND sec.{q(SECONDARY_FK)} IS NOT NULL
                      AND sec.{q(SECONDARY_FK)} <> ''
                      AND EXISTS (
                          SELECT 1 FROM raw.{q(MASTER_TABLE)} mst
                          WHERE mst.{q(MASTER_FK)} = sec.{q(SECONDARY_FK)}
                      )
                """, (f"{ZIP_PREFIX}%",))

                rows_ok = []
                for rec in cur.fetchall():
                    rows_ok.append((
                        sec_table, 'BP', rec[0], CHECK_ID,
                        'Ok',
                        'Ok', run_id, rec[1], now,
                    ))

                if rows_ok:
                    execute_values(cur, """
                        INSERT INTO stg.check_results (
                            source_table, category, object_key, check_id,
                            message, status, run_id, zip_source, created_at
                        ) VALUES %s
                    """, rows_ok, page_size=500)
                    print(f"[OK] {CHECK_ID} — {sec_table}: {len(rows_ok)} record ok")

        conn.commit()
        if total_orphans == 0:
            print(f"[OK] {CHECK_ID} — nessun orfano nel flusso {ZIP_PREFIX}")
        else:
            print(f"[WARN] {CHECK_ID} — totale orfani: {total_orphans}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
