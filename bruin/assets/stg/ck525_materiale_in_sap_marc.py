""" @bruin
name: stg.ck525_materiale_in_sap_marc
type: python
depends:
  - stg.clean_check_results
description: >
  CK525 — CROSS_SOURCE: Flussi collegati all'anagrafica materiali: il materiale (con la
  divisione, se presente nel flusso) deve essere già migrato in SAP, cioè presente in
  ref.SAP_EXPORT_MARC (MATNR + WERKS). Il codice materiale SAP è uguale a quello del
  vecchio ERP (confronto senza spazi iniziali e finali). Tabelle controllate, se caricate:
    - S_MARITC (codici doganali): MATNR + PLANT;
    - S_BOM_HEADER / S_BOM_ITEM (distinte base): padre MATNR + WERKS, componente IDNRK + WERKS;
    - S_MAPL (cicli di lavoro): MATNR + WERKS_MAT;
    - S_EINA#INFORMATFOR (info record acquisto): MATNR, in qualunque divisione;
    - S_EKPO (ordini d'acquisto): MATNR + WERKS (posizioni senza materiale escluse);
    - S_CUSTMAT#INFORMATFOR (info cliente-materiale): MATERIAL + PLANT (solo MATERIAL se PLANT vuota).
  Assente: Warning, in SAP la migrazione andrebbe in errore. Nello ZIP di S_MARITC la riga
  non è esportata (prd.export_maritc_zip, S_MARITC_discarded.csv).
  Un esito per riga. Saltato se SAP_EXPORT_MARC non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

# tabella -> (object_key, materiale, divisione o None, filtro righe o None)
TABLES = {
    "S_MARITC": ('x."MATNR(k/*)" || \'|\' || x."PLANT(k/*)"',
                 'x."MATNR(k/*)"', 'x."PLANT(k/*)"', None),
    "S_BOM_HEADER": ('x."MATNR(k/*)" || \'|\' || x."WERKS(k)" || \'|\' || x."STLAN(k/*)" || \'|\' || x."STLAL(k/*)"',
                     'x."MATNR(k/*)"', 'x."WERKS(k)"', None),
    "S_BOM_ITEM": ('x."MATNR(k/*)" || \'|\' || x."WERKS(k)" || \'|\' || x."STLAN(k/*)" || \'|\' || x."STLAL(k/*)"'
                   ' || \'|\' || x."ITMID(k/*)"',
                   'x."IDNRK"', 'x."WERKS(k)"', 'COALESCE(TRIM(x."IDNRK"), \'\') <> \'\''),
    "S_MAPL": ('x."PLNNR(k/*)" || \'|\' || x."PLNAL(k/*)" || \'|\' || x."MATNR(k/*)" || \'|\' || x."WERKS_MAT(k/*)"',
               'x."MATNR(k/*)"', 'x."WERKS_MAT(k/*)"', None),
    "S_EINA#INFORMATFOR": ('x."INFNR(k/*)"', 'x."MATNR"', None, 'COALESCE(TRIM(x."MATNR"), \'\') <> \'\''),
    "S_EKPO": ('x."EBELN(k/*)" || \'|\' || x."EBELP(k/*)"',
               'x."MATNR"', 'x."WERKS"', 'COALESCE(TRIM(x."MATNR"), \'\') <> \'\''),
    "S_CUSTMAT#INFORMATFOR": ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
                              '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"',
                              'x."MATERIAL(k/*)"', 'NULLIF(TRIM(x."PLANT"), \'\')', None),
}


def select_for(table: str, key: str, mat: str, plant: str | None, where: str | None) -> str:
    if plant:
        match = f"m.matnr = TRIM({mat}) AND ({plant} IS NULL OR m.werks = TRIM({plant}))"
        label = (f"'Materiale [' || TRIM({mat}) || '] divisione [' || COALESCE(TRIM({plant}), '') || ']'")
    else:
        match = f"m.matnr = TRIM({mat})"
        label = f"'Materiale [' || TRIM({mat}) || ']'"
    return f"""
SELECT '{table}', {key},
       CASE WHEN NOT EXISTS (SELECT 1 FROM sap_marc m WHERE {match})
            THEN {label} || ' non presente in SAP (SAP_EXPORT_MARC): la migrazione darebbe errore'
       END,
       x."_source",
       {label} || ' presente in SAP'
FROM raw."{table}" x
{"WHERE " + where if where else ""}"""


def build_sql(cur):
    parts = [select_for(t, *spec) for t, spec in TABLES.items() if table_exists(cur, "raw", t)]
    if not parts:
        return None
    return ('WITH sap_marc AS (SELECT DISTINCT TRIM("MATNR") AS matnr, TRIM("WERKS") AS werks '
            'FROM ref."SAP_EXPORT_MARC")' + "\nUNION ALL".join(parts))


if __name__ == "__main__":
    run_query_check(
        check_id="CK525",
        sql=build_sql,
        required=[("ref", "SAP_EXPORT_MARC")],
        category="MAT",
    )
