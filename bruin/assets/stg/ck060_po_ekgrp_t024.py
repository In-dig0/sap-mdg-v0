""" @bruin
name: stg.ck060_po_ekgrp_t024
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK060 — SAP_REF: Ordini di acquisto (S_EKKO): gruppo acquisti EKGRP obbligatorio e presente in
  SAP_EXPORT_T024 (EKGRP). Se SAP_EXPORT_T024 non è caricata è verificato solo il vuoto.
  Un esito per ordine.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_sap_ref_lib import run_ref_check

if __name__ == "__main__":
    run_ref_check(
        check_id="CK060",
        raw_table="S_EKKO",
        key_col="EBELN(k/*)",
        field="EKGRP",
        ref_table="SAP_EXPORT_T024",
        ref_col="EKGRP",
        category="PO",
    )
