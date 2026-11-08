""" @bruin
name: stg.ck516_inforecord_aplfz_plifz
type: python
depends:
  - stg.clean_check_results
description: >
  CK516 — CROSS_SOURCE: Info record acquisti (S_EINE#INFORDATIACQ): tempo di consegna
  assente sia nell'info record (APLFZ) sia nel materiale (PLIFZ), vuoto o zero in entrambi.
  PLIFZ è quello di ref.SAP_EXPORT_MARC (lo controlla SAP al caricamento) o, se la coppia
  materiale/divisione non è in SAP, quello di raw.S_MARC.
  Warning: l'info record non viene respinto perché prd.export_inforecord_zip scrive
  APLFZ = 999 nel file per SAP; l'elenco serve a correggere il dato in SAP.
  Info record senza testata o senza materiale/divisione in MARC esclusi (CK407, CK512).
  Un esito per riga di S_EINE. Saltato se S_EINE, S_EINA o S_MARC non sono caricate.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, lead_time_missing_sql, LEAD_TIME_PLACEHOLDER, LEAD_TIME_TABLES


def check_sql(cur) -> str:
    return f"""
SELECT 'S_EINE#INFORDATIACQ',
       infnr || '|' || COALESCE(ekorg, '') || '|' || COALESCE(esokz, '') || '|' || COALESCE(werks, ''),
       CASE WHEN forza THEN
            'Tempo di consegna assente nell''info record [' || infnr || '] e nel materiale ['
            || matnr || '] divisione [' || COALESCE(werks, '')
            || ']: nel file per SAP viene impostato APLFZ = {LEAD_TIME_PLACEHOLDER}, da correggere in SAP'
       END,
       src
FROM ({lead_time_missing_sql(cur)}) b"""


if __name__ == "__main__":
    run_query_check(
        check_id="CK516",
        sql=check_sql,
        required=[("raw", t) for t in LEAD_TIME_TABLES],
        category="PIR",
    )
