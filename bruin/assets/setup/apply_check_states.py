""" @bruin
name: setup.apply_check_states
type: python
depends:
  - setup.check_catalog
description: >
  Legge /project/bruin/config/check_states.json e applica a stg.check_catalog
  lo stato is_active ({"CK001": true, ...}) e il flag di esclusione dall'output
  ({"exclude_output": {"CK034": false, ...}}, solo per i check che la supportano).
  Questo asset garantisce che lo stato impostato dall'utente
  via Streamlit Check Catalog sopravviva ai riavvii della pipeline.
@bruin """

import os
import json
import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

STATES_PATH = "/project/bruin/config/check_states.json"


def main():
    if not os.path.exists(STATES_PATH):
        print(f"[WARN] File check_states.json non trovato in {STATES_PATH} — skip")
        return

    with open(STATES_PATH, "r") as f:
        states = json.load(f)

    excludes = states.get("exclude_output", {})
    # Rimuovi commenti (chiavi che iniziano con _) e la sezione del flag di esclusione
    states = {k: v for k, v in states.items() if not k.startswith("_") and k != "exclude_output"}

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            updated = 0
            for check_id, is_active in states.items():
                cur.execute("""
                    UPDATE stg.check_catalog
                    SET is_active  = %s,
                        updated_at = %s
                    WHERE check_id = %s
                """, (bool(is_active), datetime.now(timezone.utc), check_id))
                if cur.rowcount > 0:
                    status = "attivo" if is_active else "DISATTIVATO"
                    print(f"  [{status:>12}] {check_id}")
                    updated += 1

            for check_id, exclude in excludes.items():
                cur.execute("""
                    UPDATE stg.check_catalog
                    SET exclude_output = %s
                    WHERE check_id = %s AND exclude_supported
                """, (bool(exclude), check_id))
                if cur.rowcount > 0:
                    status = "esclude" if exclude else "NON esclude"
                    print(f"  [{status:>12}] {check_id} — record in errore nell'output")

        conn.commit()
        print(f"[OK] Stati applicati: {updated}/{len(states)} check, "
              f"flag di esclusione dall'output: {len(excludes)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
