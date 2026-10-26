""" @bruin
name: stg.ck058_zdm_kvgr5_tvv5
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_xlsx_to_ref
description: >
  CK058 — SAP_REF: Destinatari merci (DatiVendite) (S_CUST_SALES_DATA#ZDM-DatiVendite): KVGR5 obbligatorio e presente in
  SAP_EXPORT_TVV5 (KVGR5). Errore se vuoto, '???' o non presente in SAP_EXPORT_TVV5.
  Nell'output (prd.merge_dati_vendite) '???' diventa vuoto, gli altri valori restano.
  Se SAP_EXPORT_TVV5 non è caricata sono verificati solo i casi precedenti. Un esito per riga.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_sap_ref_lib import run_ref_check

if __name__ == "__main__":
    run_ref_check(
        check_id="CK058",
        raw_table="S_CUST_SALES_DATA#ZDM-DatiVendite",
        key_col="KUNNR(k/*)",
        field="KVGR5",
        ref_table="SAP_EXPORT_TVV5",
        ref_col="KVGR5",
        placeholders=("???",),
        output_note={"segnaposto": "(in output vuoto)"},
    )
