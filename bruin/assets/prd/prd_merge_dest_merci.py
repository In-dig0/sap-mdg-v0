""" @bruin
name: prd.merge_dest_merci
type: python
description: >
  Merge raw + stg → prd."S_CUST_GEN#ZDM_DatiGenerali".
  - Colonne comuni: COALESCE(stg, raw) — preferenza a stg
  - _source e _loaded_at da raw
  - _status esclusa
  - Record stg con _status='DELETED' esclusi dal JOIN
  - Record senza nome (NAMORG1, NAMORG2, NAME_FIRST, NAME_LAST tutti vuoti, segnalati
    da CK217) esclusi: non verrebbero accettati in SAP. prd.export_dest_merci_zip
    esclude i DM assenti da prd anche dalle altre tabelle dello ZIP.
  Strategia: DROP + CREATE ad ogni run (full refresh).
depends:
  - stg.detect_new_records
@bruin """

import logging
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from prd_merge_lib import get_connection, ensure_prd_schema, merge_table, q

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PRD_TABLE  = "S_CUST_GEN#ZDM_DatiGenerali"
NAME_COLS  = ["NAMORG1", "NAMORG2", "NAME_FIRST", "NAME_LAST"]


def drop_without_name(cur):
    """Toglie da prd i DM con tutti i campi nome vuoti (CK217)."""
    all_empty = " AND ".join(f"COALESCE(TRIM({q(c)}), '') = ''" for c in NAME_COLS)
    cur.execute(f'DELETE FROM prd.{q(PRD_TABLE)} WHERE {all_empty} RETURNING "KUNNR(k/*)"')
    keys = sorted(r[0] for r in cur.fetchall())
    if keys:
        log.warning(f"  ⚠  {len(keys)} DM senza nome esclusi da prd (CK217): {keys}")
    else:
        log.info("  ✓  Nessun DM senza nome.")


def main():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_prd_schema(cur)
                merge_table(
                    cur,
                    raw_schema="raw", raw_table="S_CUST_GEN#ZDM-DatiGenerali",
                    stg_schema="stg", stg_table="S_CUST_GEN#ZDM_DatiGenerali_STG",
                    prd_table=PRD_TABLE,
                )
                drop_without_name(cur)
    finally:
        conn.close()
    log.info("=== prd.merge_dest_merci completato ===")


if __name__ == "__main__":
    main()
