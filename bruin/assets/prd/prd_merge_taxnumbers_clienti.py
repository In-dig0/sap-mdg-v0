""" @bruin
name: prd.merge_taxnumbers_clienti
type: python
description: >
  Merge raw + stg → prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc".
  - Colonne comuni: COALESCE(stg, raw) — preferenza a stg
  - _source e _loaded_at da raw
  - _status esclusa
  - Record stg con _status='DELETED' esclusi dal JOIN
  - Filtro VIES: se un cliente ha is_eu=true e vies_valid=false
    in stg.check_vat_vies, i suoi record TAXTYPE LIKE '%0' vengono esclusi da prd
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
                    raw_schema="raw", raw_table="S_CUST_TAXNUMBERS#ZBP-CodiciFisc",
                    stg_schema="stg", stg_table="S_CUST_TAXNUMBERS#ZBP-CodiciFisc_STG",
                    prd_table="S_CUST_TAXNUMBERS#ZBP-CodiciFisc",
                    vies_filter=True,
                    vies_lifnr_col="KUNNR(k/*)",
                )
    finally:
        conn.close()
    log.info("=== prd.merge_taxnumbers_clienti completato ===")


if __name__ == "__main__":
    main()
