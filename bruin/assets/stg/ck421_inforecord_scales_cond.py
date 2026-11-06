""" @bruin
name: stg.ck421_inforecord_scales_cond
type: python
depends:
  - stg.clean_check_results
description: >
  CK421 — CROSS_TABLE: Info record: ogni scaglione di S_SCALES#INFORSCALES deve avere la
  condizione padre in S_COND#INFORCOND con la stessa chiave completa (INFNR, EKORG, ESOKZ,
  WERKS, KOPOS, COND_TYPE, VALID_FROM, VALID_TO), altrimenti SAP lo rifiuta (messaggio
  /LTB/MC 416: "does not exist at a higher level in the hierarchy").
  Il messaggio indica se la condizione esiste con un ESOKZ diverso. Solo segnalazione: la
  riga resta nell'output. Un esito per scaglione.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

KEY_NO_ESOKZ = ["INFNR(k/*)", "EKORG(k/*)", "WERKS(k)", "KOPOS(k/*)",
                "COND_TYPE(k/*)", "VALID_FROM(k/*)", "VALID_TO(k/*)"]
JOIN_NO_ESOKZ = " AND ".join(
    f"""COALESCE(TRIM(c."{col}"), '') = COALESCE(TRIM(s."{col}"), '')""" for col in KEY_NO_ESOKZ
)

SQL = f"""
SELECT 'S_SCALES#INFORSCALES',
       s."INFNR(k/*)" || '|' || COALESCE(s."EKORG(k/*)", '') || '|' || COALESCE(s."ESOKZ(k/*)", '')
           || '|' || COALESCE(s."WERKS(k)", '') || '|' || COALESCE(s."KOPOS(k/*)", '')
           || '|' || COALESCE(s."COND_TYPE(k/*)", '') || '|' || COALESCE(s."VALID_FROM(k/*)", '')
           || '|' || COALESCE(s."LINE_NO(k/*)", ''),
       CASE
           WHEN m.esokz IS NULL
               THEN 'Scaglione senza condizione padre in S_COND#INFORCOND (chiave completa)'
           WHEN NOT m.same_esokz
               THEN 'ESOKZ [' || COALESCE(TRIM(s."ESOKZ(k/*)"), '') || '] diverso dalla condizione padre in '
                    || 'S_COND#INFORCOND (ESOKZ [' || m.esokz || '])'
       END,
       s."_source"
FROM raw."S_SCALES#INFORSCALES" s
LEFT JOIN LATERAL (
    SELECT string_agg(DISTINCT TRIM(c."ESOKZ(k/*)"), ', ') AS esokz,
           COALESCE(bool_or(COALESCE(TRIM(c."ESOKZ(k/*)"), '') = COALESCE(TRIM(s."ESOKZ(k/*)"), '')), FALSE) AS same_esokz
    FROM raw."S_COND#INFORCOND" c
    WHERE {JOIN_NO_ESOKZ}
) m ON TRUE
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK421",
        sql=SQL,
        required=[("raw", "S_SCALES#INFORSCALES"), ("raw", "S_COND#INFORCOND")],
        category="PIR",
    )
