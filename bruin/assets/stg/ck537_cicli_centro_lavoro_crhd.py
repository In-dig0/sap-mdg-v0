""" @bruin
name: stg.ck537_cicli_centro_lavoro_crhd
type: python
depends:
  - stg.clean_check_results
  - ingestion.ingest_others_to_stg
description: >
  CK537 — SAP_REF: Cicli di lavoro, operazioni: il centro di lavoro WORK_CNTR nella divisione
  WERKS_WORK_CNTR deve esistere nell'anagrafica dei centri di lavoro SAP (ref.SAP_EXPORT_CRHD,
  ARBPL + WERKS; confronto senza spazi iniziali e finali). Controllate le righe di raw.S_OPERATION
  e, se caricate, quelle del file di integrazione stg.S_OPERATION_STG (UPD/ADD) che vengono unite
  nel file per SAP (prd.merge_cicli). Per le righe di raw.S_OPERATION si controlla il centro di
  lavoro dopo la trascodifica di ref.SAP_TRASCO_CDLAV (WERKS + WORK_CNTR_OLD → WORK_CNTR_NEW, se
  caricata e univoca), come nel file per SAP; le righe STG non sono trascodificate.
  Le operazioni senza centro di lavoro non sono controllate (esito Ok). Un esito per operazione.
  Esclusione dallo ZIP dei cicli supportata (prd.export_cicli_lavoro_zip, default disattivata).
  Saltato se SAP_EXPORT_CRHD non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

CRHD = "SAP_EXPORT_CRHD"
WC_TRASCO = "SAP_TRASCO_CDLAV"
SOURCES = [("raw", "S_OPERATION", True), ("stg", "S_OPERATION_STG", False)]


def norm(expr):
    # come prd.merge_cicli: testo senza spazi e senza il ".0" delle celle numeriche di Excel
    return f"regexp_replace(TRIM({expr}), '\\.0+$', '')"


def part(schema, table, transcode):
    wc = 'TRIM(o."WORK_CNTR")'
    trasco_join = ""
    if transcode:
        wc = f'COALESCE(t.new, {wc})'
        trasco_join = f"""
LEFT JOIN (
    SELECT {norm('"WERKS"')} AS werks, {norm('"WORK_CNTR_OLD"')} AS old, MIN({norm('"WORK_CNTR_NEW"')}) AS new
    FROM ref."{WC_TRASCO}"
    WHERE COALESCE(TRIM("WORK_CNTR_OLD"), '') <> '' AND COALESCE(TRIM("WORK_CNTR_NEW"), '') <> ''
    GROUP BY 1, 2 HAVING COUNT(DISTINCT {norm('"WORK_CNTR_NEW"')}) = 1
) t ON t.werks = TRIM(o."WERKS_WORK_CNTR") AND t.old = TRIM(o."WORK_CNTR")"""
        label = ("CASE WHEN t.new IS NOT NULL THEN ' (trascodificato da [' || TRIM(o.\"WORK_CNTR\") || '])' "
                 "ELSE '' END")
    else:
        label = "''"
    return f"""
SELECT '{table}',
       o."PLNNR(k/*)" || '|' || o."PLNAL(k/*)" || '|' || COALESCE(o."PLNFL(k)", '') || '|' || o."VORNR(k/*)",
       CASE WHEN COALESCE({wc}, '') <> ''
             AND NOT EXISTS (SELECT 1 FROM ref."{CRHD}" c
                             WHERE TRIM(c."ARBPL") = {wc} AND TRIM(c."WERKS") = TRIM(o."WERKS_WORK_CNTR"))
            THEN 'Centro di lavoro [' || {wc} || ']' || {label} || ' nella divisione ['
                 || COALESCE(TRIM(o."WERKS_WORK_CNTR"), '') || '] non presente in SAP ({CRHD}: ARBPL + WERKS)'
       END,
       o."_source",
       CASE WHEN COALESCE({wc}, '') = '' THEN 'Centro di lavoro non indicato'
            ELSE 'Centro di lavoro [' || {wc} || '/' || TRIM(o."WERKS_WORK_CNTR") || ']' || {label} || ' presente in SAP'
       END
FROM {schema}."{table}" o{trasco_join}"""


def build_sql(cur):
    has_trasco = table_exists(cur, "ref", WC_TRASCO)
    parts = [part(s, t, tr and has_trasco) for s, t, tr in SOURCES if table_exists(cur, s, t)]
    return "\nUNION ALL\n".join(parts) if parts else None


if __name__ == "__main__":
    run_query_check(
        check_id="CK537",
        sql=build_sql,
        required=[("ref", CRHD)],
        category="ROUTING",
    )
