""" @bruin
name: stg.ck017_mara_pesi
type: python
depends:
  - stg.clean_check_results
description: >
  CK017 — EXISTENCE: Materiali (S_MARA): pesi e unità di peso.
  Materiali non imballo: peso netto NTGEW e peso lordo BRGEW obbligatori, numerici e maggiori
  di zero, con BRGEW >= NTGEW. Imballi (MTART(*) VERP o ZVER): solo il peso lordo BRGEW
  obbligatorio, il peso netto NTGEW deve essere vuoto (o zero, es. '000000,000').
  Con un peso valorizzato l'unità di peso GEWEI è obbligatoria. Il messaggio indica il tipo
  materiale. Un esito per materiale.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check

# Tipi materiale imballo: solo peso lordo
PACKAGING_TYPES = ("VERP", "ZVER")

# Numero da testo ('000001,331'); NULL se vuoto o non numerico
NUM = "CASE WHEN TRIM({c}) ~ '^[0-9]+([.,][0-9]+)?$' THEN REPLACE(TRIM({c}), ',', '.')::numeric END"

SQL = f"""
SELECT 'S_MARA', m."PRODUCT(k/*)",
       'Tipo materiale [' || p.mtart || ']: ' || NULLIF(CONCAT_WS('; ',
           CASE WHEN p.netto_txt <> '' AND p.netto IS NULL THEN 'peso netto NTGEW [' || p.netto_txt || '] non numerico' END,
           CASE WHEN p.lordo_txt <> '' AND p.lordo IS NULL THEN 'peso lordo BRGEW [' || p.lordo_txt || '] non numerico' END,
           CASE WHEN p.imballo AND COALESCE(p.netto, 0) <> 0
                THEN 'imballo, peso netto NTGEW [' || p.netto_txt || '] deve essere vuoto' END,
           CASE WHEN NOT p.imballo AND p.netto_txt = '' THEN 'peso netto NTGEW obbligatorio mancante'
                WHEN NOT p.imballo AND p.netto = 0 THEN 'peso netto NTGEW non può essere zero' END,
           CASE WHEN p.lordo_txt = '' THEN 'peso lordo BRGEW obbligatorio mancante'
                WHEN p.lordo = 0 THEN 'peso lordo BRGEW non può essere zero' END,
           CASE WHEN NOT p.imballo AND p.lordo > 0 AND p.netto > 0 AND p.lordo < p.netto
                THEN 'peso lordo BRGEW [' || p.lordo_txt || '] minore del peso netto NTGEW [' || p.netto_txt || ']' END,
           CASE WHEN p.gewei = '' AND (COALESCE(p.netto, 0) <> 0 OR COALESCE(p.lordo, 0) <> 0)
                THEN 'unità di peso GEWEI obbligatoria mancante' END
       ), ''),
       m."_source"
FROM raw."S_MARA" m
CROSS JOIN LATERAL (
    SELECT COALESCE(TRIM(m."NTGEW"), '') AS netto_txt,
           COALESCE(TRIM(m."BRGEW"), '') AS lordo_txt,
           {NUM.format(c='m."NTGEW"')} AS netto,
           {NUM.format(c='m."BRGEW"')} AS lordo,
           COALESCE(TRIM(m."GEWEI"), '') AS gewei,
           COALESCE(TRIM(m."MTART(*)"), '') AS mtart,
           COALESCE(TRIM(m."MTART(*)"), '') IN ({", ".join(f"'{t}'" for t in PACKAGING_TYPES)}) AS imballo
) p
"""

if __name__ == "__main__":
    run_query_check(
        check_id="CK017",
        sql=SQL,
        required=[("raw", "S_MARA")],
        category="MAT",
    )
