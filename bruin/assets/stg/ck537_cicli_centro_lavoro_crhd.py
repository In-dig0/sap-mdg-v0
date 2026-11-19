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
  nel file per SAP (prd.merge_cicli). Le operazioni senza centro di lavoro non sono controllate
  (esito Ok). Solo segnalazione. Un esito per operazione. Saltato se SAP_EXPORT_CRHD non è caricata.
@bruin """

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from ck_query_lib import run_query_check, table_exists

CRHD = "SAP_EXPORT_CRHD"
SOURCES = [("raw", "S_OPERATION"), ("stg", "S_OPERATION_STG")]


def part(schema, table):
    return f"""
SELECT '{table}',
       o."PLNNR(k/*)" || '|' || o."PLNAL(k/*)" || '|' || COALESCE(o."PLNFL(k)", '') || '|' || o."VORNR(k/*)",
       CASE WHEN c.ok IS NULL
            THEN 'Centro di lavoro [' || TRIM(o."WORK_CNTR") || '] nella divisione ['
                 || COALESCE(TRIM(o."WERKS_WORK_CNTR"), '') || '] non presente in SAP ({CRHD}: ARBPL + WERKS)'
       END,
       o."_source",
       CASE WHEN COALESCE(TRIM(o."WORK_CNTR"), '') = '' THEN 'Centro di lavoro non indicato'
            ELSE 'Centro di lavoro [' || TRIM(o."WORK_CNTR") || '/' || TRIM(o."WERKS_WORK_CNTR") || '] presente in SAP'
       END
FROM {schema}."{table}" o
LEFT JOIN LATERAL (
    SELECT 1 AS ok
    WHERE COALESCE(TRIM(o."WORK_CNTR"), '') = ''
       OR EXISTS (SELECT 1 FROM ref."{CRHD}" c
                  WHERE TRIM(c."ARBPL") = TRIM(o."WORK_CNTR")
                    AND TRIM(c."WERKS") = TRIM(o."WERKS_WORK_CNTR"))
) c ON TRUE"""


def build_sql(cur):
    parts = [part(s, t) for s, t in SOURCES if table_exists(cur, s, t)]
    return "\nUNION ALL\n".join(parts) if parts else None


if __name__ == "__main__":
    run_query_check(
        check_id="CK537",
        sql=build_sql,
        required=[("ref", CRHD)],
        category="ROUTING",
    )
