""" @bruin
name: stg.ck416_customer_vbund_akont
type: python
depends:
  - stg.clean_check_results
description: >
  CK416 — CROSS_TABLE: Clienti: coerenza tra società partner VBUND (S_CUST_GEN#ZBP-DatiGenerali)
  e conto di riconciliazione AKONT(*) (S_CUST_COMPANY#ZBP-DatiSocieta). Con VBUND valorizzato il
  conto deve essere intercompany (elenco IC_ACCOUNTS); con conto intercompany VBUND è obbligatorio.
  Un esito per cliente/società.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

# Conti di riconciliazione clienti ammessi per le società partner (crediti v/società del gruppo)
IC_ACCOUNTS = (
    "0011320001", "0012220001", "0012220011", "0012220021", "0012220031", "0012220051",
    "0012230001", "0012230011", "0012230021", "0012240001", "0012240021",
    "0012250001", "0012250021", "0012250041",
)
IC_SQL = ", ".join(f"'{a}'" for a in IC_ACCOUNTS)

SQL = f"""
SELECT 'S_CUST_COMPANY#ZBP-DatiSocieta', s."KUNNR(k/*)" || '/' || s."BUKRS(k/*)",
       CASE
           WHEN x.vbund <> '' AND x.akont NOT IN ({IC_SQL})
               THEN 'Società partner VBUND [' || x.vbund || '] con conto di riconciliazione AKONT(*) ['
                    || x.akont || '] non intercompany'
           WHEN x.vbund = '' AND x.akont IN ({IC_SQL})
               THEN 'Conto di riconciliazione intercompany AKONT(*) [' || x.akont
                    || '] senza società partner VBUND'
       END,
       s."_source"
FROM raw."S_CUST_COMPANY#ZBP-DatiSocieta" s
JOIN raw."S_CUST_GEN#ZBP-DatiGenerali" g ON TRIM(g."KUNNR(k/*)") = TRIM(s."KUNNR(k/*)")
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(g."VBUND"), '') AS vbund, COALESCE(TRIM(s."AKONT(*)"), '') AS akont
) x
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK416",
        sql=SQL,
        required=[("raw", "S_CUST_COMPANY#ZBP-DatiSocieta"), ("raw", "S_CUST_GEN#ZBP-DatiGenerali")],
        category="BP",
    )
