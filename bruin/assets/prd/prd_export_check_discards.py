""" @bruin
name: prd.export_check_discards
type: python
description: >
  Per ogni check attivo con l'esclusione dall'output attiva nel catalogo scrive
  out_source_mdg/discarded/<CHECK>_discarded.xlsx con i record segnalati dal check
  nell'ultimo run (esito diverso da Ok), quindi esclusi dai file per SAP, e il motivo:
  TABELLA;CHIAVE;MOTIVO;GRAVITA;ZIP. Se il check non ha segnalazioni il file non è scritto.
  Elenca i record segnalati dal check: le righe effettivamente tolte dagli ZIP sono nei file
  discarded/<tabella>_discarded.xlsx (possono essere di più, es. l'intera distinta base per un
  componente di CK527, o tutte le righe collegate a un codice orfano).
depends:
  - stg.ck034_maritc_comco
  - stg.ck506_maritc_in_marc
  - stg.ck511_maritc_mara
  - stg.ck525_materiale_in_sap_marc
  - stg.ck051_supplier_industry_sector
  - stg.ck052_supplier_add_industry_sector
  - stg.ck402_zbp_fornitori_orphans
  - stg.ck418_fornitori_cli_banca_cliente
  - stg.ck420_supplier_partner_lifn2_sap
  - stg.ck047_supplier_bankl_banche_it
  - stg.ck055_supplier_bankl_banche_ue
  - stg.ck214_supplier_bank_conto_iban
  - stg.ck804_supplier_vat_vies_summary
  - stg.ck048_customer_bankl_banche_it
  - stg.ck056_customer_bankl_banche_ue
  - stg.ck213_customer_bank_conto_iban
  - stg.ck802_customer_vat_vies_summary
  - stg.ck220_customer_taxnum_vuoto
  - stg.ck404_zbp_clienti_orphans
  - stg.ck403_zdm_clienti_orphans
  - stg.ck526_inforecord_lifnr_sap
  - stg.ck527_bom_materiale_in_sap_marc
  - stg.ck528_inforecord_materiale_in_sap_marc
  - stg.ck518_add_interlocutore_cli_customer
  - stg.ck519_gestione_credito_customer
  - stg.ck422_customer_partner_riferimento
  - stg.ck423_customer_partner_fornitore_sap
  - stg.ck424_fornitori_cli_cliente_sap
  - stg.ck425_customer_add_cliente_sap
  - stg.ck426_customer_add_solo_dm
@bruin """

import os
import re
import sys
import logging
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from prd_discard_lib import DISCARDED_DIR, write_xlsx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

CHECK_FILE    = re.compile(r"^CK\d{3}_discarded\.(csv|xlsx)$")   # anche i CSV delle versioni precedenti
HEADER        = ["TABELLA", "CHIAVE", "MOTIVO", "GRAVITA", "ZIP"]


def remove_previous_files():
    """File dei check del run precedente: rimossi (un check può non escludere più nulla)."""
    if not os.path.isdir(DISCARDED_DIR):
        return
    for name in os.listdir(DISCARDED_DIR):
        if CHECK_FILE.match(name):
            os.remove(os.path.join(DISCARDED_DIR, name))


def main():
    remove_previous_files()
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT check_id FROM stg.check_catalog
                WHERE is_active AND exclude_supported AND exclude_output
                ORDER BY check_id
            """)
            check_ids = [r[0] for r in cur.fetchall()]
            written = 0
            for check_id in check_ids:
                cur.execute("""
                    SELECT source_table, object_key, message, status, zip_source
                    FROM stg.check_results
                    WHERE check_id = %s AND status <> 'Ok'
                      AND run_id = (SELECT MAX(run_id) FROM stg.check_results WHERE check_id = %s)
                    ORDER BY source_table, object_key
                """, (check_id, check_id))
                rows = cur.fetchall()
                if not rows:
                    continue
                path = os.path.join(DISCARDED_DIR, f"{check_id}_discarded.xlsx")
                write_xlsx(path, [(check_id, HEADER, rows)])
                written += 1
                log.warning("  [%s] %d record esclusi dall'output: %s", check_id, len(rows), path)
        log.info("=== %d check con esclusione attiva, %d file di scarto scritti ===", len(check_ids), written)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
