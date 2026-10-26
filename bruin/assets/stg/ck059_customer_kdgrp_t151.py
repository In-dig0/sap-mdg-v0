""" @bruin
name: stg.ck059_customer_kdgrp_t151
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK059 — SAP_REF: Clienti (DatiVendite) (S_CUST_SALES_DATA#ZBP-DatiVendite): KDGRP obbligatorio e presente in
  SAP_EXPORT_T151 (KDGRP). Errore se vuoto o non presente in SAP_EXPORT_T151.
  Nell'output (prd.merge_dati_vendite) KDGRP vuoto o non ammesso diventa '04'.
  Se SAP_EXPORT_T151 non è caricata sono verificati solo i casi precedenti. Un esito per riga.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_sap_ref_lib import run_ref_check

if __name__ == "__main__":
    run_ref_check(
        check_id="CK059",
        raw_table="S_CUST_SALES_DATA#ZBP-DatiVendite",
        key_col="KUNNR(k/*)",
        field="KDGRP",
        ref_table="SAP_EXPORT_T151",
        ref_col="KDGRP",
        placeholders=(),
        output_note={"vuoto": "(in output 04)", "non_ammesso": "(in output 04)"},
    )
