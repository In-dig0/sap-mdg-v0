""" @bruin
name: prd.merge_dati_vendite
type: python
description: >
  Copia raw → prd dei dati di vendita di clienti (ZBP) e destinatari merci (ZDM)
  con i valori di default degli Incoterm per i campi non valorizzati
  (segnalati come errore da CK218/CK219):
    - INCO1 vuoto → 'EXW'
    - INCO2 vuoto → '.'
    - KVGR5 = '???' → '' (segnalato da CK057/CK058)
    - solo clienti: KDGRP vuoto o non presente in ref.SAP_EXPORT_T151 → '04' (CK059)
  Strategia: DROP + CREATE ad ogni run (full refresh). Tabelle raw assenti: skip.
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
@bruin """

import logging
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from prd_merge_lib import get_connection, ensure_prd_schema, q

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TABLES = [
    "S_CUST_SALES_DATA#ZBP-DatiVendite",
    "S_CUST_SALES_DATA#ZDM-DatiVendite",
]

# Valori di default per i campi Incoterm non valorizzati (stringhe di soli spazi = vuote)
DEFAULTS = {
    "INCO1": "EXW",
    "INCO2": ".",
}

# Valori sostituiti nell'output: {colonna: {valore: nuovo_valore}}
REPLACE = {
    "KVGR5": {"???": ""},
}

# Gruppo clienti (solo clienti ZBP): vuoto o non ammesso in SAP -> valore di default
KDGRP_TABLE   = "S_CUST_SALES_DATA#ZBP-DatiVendite"
KDGRP_DEFAULT = "04"
KDGRP_REF     = "SAP_EXPORT_T151"


def merge_dati_vendite(cur, table: str):
    cur.execute("SELECT to_regclass(%s)", (f"raw.{q(table)}",))
    if cur.fetchone()[0] is None:
        log.warning(f"  SKIP raw.{table} — tabella non trovata")
        cur.execute(f"DROP TABLE IF EXISTS prd.{q(table)}")
        return

    cur.execute(f"DROP TABLE IF EXISTS prd.{q(table)}")
    cur.execute(f"CREATE TABLE prd.{q(table)} AS SELECT * FROM raw.{q(table)}")
    cur.execute("SELECT count(*) FROM prd." + q(table))
    log.info(f"  prd.{table}: {cur.fetchone()[0]} righe copiate da raw")

    for col, default in DEFAULTS.items():
        cur.execute(
            f"UPDATE prd.{q(table)} SET {q(col)} = %s WHERE COALESCE(TRIM({q(col)}), '') = ''",
            (default,),
        )
        if cur.rowcount:
            log.info(f"    {col} vuoto → '{default}': {cur.rowcount} righe")

    for col, mapping in REPLACE.items():
        for old, new in mapping.items():
            cur.execute(f"UPDATE prd.{q(table)} SET {q(col)} = %s WHERE TRIM({q(col)}) = %s", (new, old))
            if cur.rowcount:
                log.info(f"    {col} '{old}' → '{new}': {cur.rowcount} righe")

    if table == KDGRP_TABLE:
        cur.execute("SELECT to_regclass(%s)", (f"ref.{q(KDGRP_REF)}",))
        check_ref = cur.fetchone()[0] is not None
        not_allowed = (f' OR NOT EXISTS (SELECT 1 FROM ref.{q(KDGRP_REF)} r WHERE TRIM(r."KDGRP") = TRIM(p."KDGRP"))'
                       if check_ref else "")
        cur.execute(
            f'UPDATE prd.{q(table)} p SET "KDGRP" = %s '
            f'WHERE COALESCE(TRIM(p."KDGRP"), \'\') = \'\'{not_allowed}',
            (KDGRP_DEFAULT,),
        )
        log.info(f"    KDGRP vuoto{' o non in ' + KDGRP_REF if check_ref else ''} → '{KDGRP_DEFAULT}': {cur.rowcount} righe"
                 + ("" if check_ref else f" (ref.{KDGRP_REF} non caricata: verificati solo i vuoti)"))


def main():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_prd_schema(cur)
                for table in TABLES:
                    merge_dati_vendite(cur, table)
    finally:
        conn.close()
    log.info("=== prd.merge_dati_vendite completato ===")


if __name__ == "__main__":
    main()
