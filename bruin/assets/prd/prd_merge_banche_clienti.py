""" @bruin
name: prd.merge_banche_clienti
type: python
description: >
  Merge raw → prd."S_CUST_BANK_DATA#ZBP-AppoggioBanca".
  - _source e _loaded_at da raw
  - _status esclusa
  - Filtro CK048: se BANKS(k)='IT' e BANKL(k) non è presente in
    ref."SAP_Banche" (campo "Numero ABI/CAB"), il record viene escluso
    dall'estrazione e tracciato nel log.
  Strategia: DROP + CREATE ad ogni run (full refresh).
depends:
  - stg.detect_new_records
@bruin """

import logging
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from prd_merge_lib import get_connection, ensure_prd_schema, merge_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_prd_schema(cur)
                merge_table(
                    cur,
                    raw_schema="raw", raw_table="S_CUST_BANK_DATA#ZBP-AppoggioBanca",
                    stg_schema="stg", stg_table="S_CUST_BANK_DATA#ZBP-AppoggioBanca_STG",
                    prd_table="S_CUST_BANK_DATA#ZBP-AppoggioBanca",
                    ck048_filter=True,
                    ck048_kunnr_col="KUNNR(k/*)",
                )
    finally:
        conn.close()
    log.info("=== prd.merge_banche_clienti completato ===")


if __name__ == "__main__":
    main()
