""" @bruin
name: stg.ck061_po_zterm_t052
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK061 — SAP_REF: Ordini di acquisto (S_EKKO): condizione di pagamento ZTERM obbligatoria e presente in
  SAP_EXPORT_T052 (ZTERM). Se SAP_EXPORT_T052 non è caricata è verificato solo il vuoto.
  Un esito per ordine.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_sap_ref_lib import run_ref_check

if __name__ == "__main__":
    run_ref_check(
        check_id="CK061",
        raw_table="S_EKKO",
        key_col="EBELN(k/*)",
        field="ZTERM",
        ref_table="SAP_EXPORT_T052",
        ref_col="ZTERM",
        category="PO",
    )
