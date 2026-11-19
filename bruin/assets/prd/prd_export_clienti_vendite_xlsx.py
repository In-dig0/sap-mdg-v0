""" @bruin
name: prd.export_clienti_vendite_xlsx
type: python
description: >
  Genera i file Excel con le informazioni che l'ERP passa in campi dei dati di vendita usati
  in "override" (S_CUST_SALES_DATA#ZBP-DatiVendite), da caricare in SAP a parte:
    - Clienti_fattura_email.xlsx: invio fattura via email, clienti con INCO3_L = 'E';
      colonne KUNNR, KSCHL (messaggio fattura = ZDMO);
    - Clienti_SpeseIncasso.xlsx: addebito spese incasso, clienti con PERRL valorizzato, una riga
      per cliente e area di vendita; colonne come le spese imballo più DIVISA (EUR) dopo
      l'importo, con KSCHL ZMCD, VALORE_MAGGIORAZIONE 2,00 e TIPO_MAGGIORAZIONE vuoto;
    - Clienti_SpeseImballo.xlsx: percentuale di maggiorazione per spese imballo, clienti con
      KABSS valorizzato, una riga per cliente e area di vendita (VKORG, VTWEG); colonne del
      modello SAP: progressivo (senza intestazione), KSCHL (condizione ZMPT "Magg. Imballi"),
      VKORG, VTWEG, KUNNR, NAME (descrizione del BP in BUT000), VALORE_MAGGIORAZIONE (KABSS),
      TIPO_MAGGIORAZIONE (%), DUMMY_2..DUMMY_5 (vuote), DATA_INIZIO (01.01.2026),
      DATA_FINE (31.12.9999).
  Fattura via email: una riga per cliente (in almeno un'area di vendita). KUNNR è il codice BP SAP (PARTNER di
  ref.SAP_EXPORT_BUT000 con BU_SORT2 = codice cliente ERP): i clienti non presenti in SAP, con
  più BP o con valori diversi tra le aree di vendita sono esclusi e scritti negli scarti
  (discarded/<file>_discarded.xlsx). Senza BUT000 i file non vengono creati.
  Nello ZIP clienti per SAP INCO3_L, PERRL e KABSS vengono svuotati (prd.export_clienti_zip).
  I file hanno data e ora di avvio del run nel nome, come gli ZIP (<file>_MDGOUT_YYYYMMDD_HHMI
  .xlsx): resta solo quello dell'ultimo run; non creati se nessun cliente ha il campo valorizzato.
  Output: /project/datalake/out_source_mdg/<file>_MDGOUT_<YYYYMMDD_HHMI>.xlsx
depends:
  - prd.merge_dati_vendite
  - ingestion.ingest_xlsx_to_ref
@bruin """

import os
import sys
import logging
import psycopg2
from openpyxl import Workbook

sys.path.insert(0, os.path.dirname(__file__))
from prd_output_lib import output_zip_path, remove_previous_zips
from prd_discard_lib import add_discards, flush_discards, reset_discards

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

OUTPUT_DIR   = "/project/datalake/out_source_mdg"
SALES_TABLE  = "S_CUST_SALES_DATA#ZBP-DatiVendite"
BUT000_TABLE = "SAP_EXPORT_BUT000"
LEGACY_NAMES = ["Clienti_ZDMO"]   # nomi dei file delle versioni precedenti, rimossi a ogni run

# Un file per campo in override: nome base, campo, valore richiesto (None = qualsiasi valore
# non vuoto), intestazione della seconda colonna, valore scritto (None = valore del campo), descrizione
OUTPUTS = [
    ("Clienti_fattura_email", "INCO3_L", "E", "KSCHL", "ZDMO", "invio fattura via email (messaggio ZDMO)"),
]
# Condizioni per cliente e area di vendita (spese incasso, spese imballo), nel formato delle
# condizioni SAP: nome file, campo, descrizione, KSCHL, valore (None = valore del campo),
# divisa (None = nessuna colonna DIVISA), tipo maggiorazione, layout del modello SAP (True =
# progressivo, NAME e colonne DUMMY_2..DUMMY_5)
CONDITIONS = [
    ("Clienti_SpeseIncasso", "PERRL", "addebito spese incasso",          "ZMCD", "2,00", "EUR", "",  False),
    ("Clienti_SpeseImballo", "KABSS", "maggiorazione per spese imballo", "ZMPT", None,   None,  "%", True),
]
DUMMY_COLUMNS = ["DUMMY_2", "DUMMY_3", "DUMMY_4", "DUMMY_5"]
CONDITION_VALID  = ("01.01.2026", "31.12.9999")

SQL_CONDITION = """
    WITH cli AS (
        SELECT TRIM("KUNNR(k/*)") AS kunnr, TRIM("VKORG(k/*)") AS vkorg, TRIM("VTWEG(k/*)") AS vtweg,
               ARRAY_AGG(DISTINCT TRIM({col}) ORDER BY TRIM({col})) AS vals
        FROM prd."{sales}"
        WHERE TRIM(COALESCE({col}, '')) <> '' AND TRIM(COALESCE("KUNNR(k/*)", '')) <> ''
        GROUP BY 1, 2, 3
    )
    SELECT c.kunnr, c.vkorg, c.vtweg, c.vals,
           ARRAY_REMOVE(ARRAY_AGG(DISTINCT TRIM(b."PARTNER") ORDER BY TRIM(b."PARTNER")), NULL),
           MIN(COALESCE(NULLIF(TRIM(b."NAME_ORG1"), ''),
                        NULLIF(TRIM(CONCAT_WS(' ', TRIM(b."NAME_FIRST"), TRIM(b."NAME_LAST"))), '')))
    FROM cli c
    LEFT JOIN ref."{but000}" b ON TRIM(b."BU_SORT2") = c.kunnr
                              AND TRIM(COALESCE(b."PARTNER", '')) <> ''
    GROUP BY c.kunnr, c.vkorg, c.vtweg, c.vals
    ORDER BY 1, 2, 3
"""

# Clienti con il campo valorizzato, valori distinti e BP SAP corrispondenti (BU_SORT2 = codice ERP)
SQL_ROWS = """
    WITH cli AS (
        SELECT TRIM("KUNNR(k/*)") AS kunnr,
               ARRAY_AGG(DISTINCT TRIM({col}) ORDER BY TRIM({col})) AS vals
        FROM prd."{sales}"
        WHERE TRIM(COALESCE({col}, '')) <> '' {cond}
          AND TRIM(COALESCE("KUNNR(k/*)", '')) <> ''
        GROUP BY 1
    )
    SELECT c.kunnr, c.vals,
           ARRAY_REMOVE(ARRAY_AGG(DISTINCT TRIM(b."PARTNER") ORDER BY TRIM(b."PARTNER")), NULL)
    FROM cli c
    LEFT JOIN ref."{but000}" b ON TRIM(b."BU_SORT2") = c.kunnr
                              AND TRIM(COALESCE(b."PARTNER", '')) <> ''
    GROUP BY c.kunnr, c.vals
    ORDER BY 1
"""


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table))
    return cur.fetchone() is not None


def column_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
                "AND column_name = %s", (schema, table, column))
    return cur.fetchone() is not None


def export_one(cur, name, field, required, header, fixed, label):
    output_name = f"{name}.xlsx"
    if not column_exists(cur, "prd", SALES_TABLE, field):
        log.warning("Campo %s assente in prd.%s — %s non generato.", field, SALES_TABLE, output_name)
        return
    cond = "AND UPPER(TRIM({0})) = %s".format(f'"{field}"') if required else ""
    cur.execute(SQL_ROWS.format(col=f'"{field}"', cond=cond, sales=SALES_TABLE, but000=BUT000_TABLE),
                (required,) if required else None)
    rows = cur.fetchall()
    if not rows:
        log.warning("Nessun cliente con %s valorizzato — %s non generato.", field, output_name)
        return

    cols = [f"KUNNR (codice ERP)", "BP SAP", header]
    ok, not_in_sap, ambiguous, mixed = [], [], [], []
    for kunnr, vals, partners in rows:
        value = fixed or vals[0]
        if len(vals) > 1 and not fixed:
            mixed.append((kunnr, ", ".join(partners), " | ".join(vals)))
        elif not partners:
            not_in_sap.append((kunnr, "", value))
        elif len(partners) > 1:
            ambiguous.append((kunnr, ", ".join(partners), value))
        else:
            ok.append((partners[0], value))
    add_discards(name, cols, not_in_sap, "",
                 f"Cliente non presente in SAP ({BUT000_TABLE}, BU_SORT2): {label} da assegnare dopo il "
                 "caricamento del cliente")
    add_discards(name, cols, ambiguous, "", f"Cliente associato a più BP SAP ({BUT000_TABLE}, BU_SORT2)")
    add_discards(name, cols, mixed, "", f"Valori di {field} diversi tra le aree di vendita del cliente")
    for what, items in (("non presenti in SAP", not_in_sap), ("con più BP SAP", ambiguous),
                        (f"con {field} diverso tra le aree di vendita", mixed)):
        if items:
            log.warning("  *** %s: %d clienti %s, esclusi: %s", name, len(items), what,
                        ", ".join(r[0] for r in items))
    if not ok:
        log.warning("Nessun cliente con %s presente in SAP — %s non generato.", field, output_name)
        return

    wb = Workbook(write_only=True)
    ws = wb.create_sheet(name)
    ws.append(["KUNNR", header])
    for partner, value in sorted(ok):
        ws.append([partner, value])
    output_file = output_zip_path(OUTPUT_DIR, output_name)
    wb.save(output_file)
    log.info("  ✓  %s  (%d clienti, %s; %d esclusi)", output_file, len(ok), label,
             len(not_in_sap) + len(ambiguous) + len(mixed))


def export_condition(cur, name, field, label, condition, fixed_value, currency, cond_type, sap_layout):
    output_name = f"{name}.xlsx"
    if not column_exists(cur, "prd", SALES_TABLE, field):
        log.warning("Campo %s assente in prd.%s — %s non generato.", field, SALES_TABLE, output_name)
        return
    cur.execute(SQL_CONDITION.format(col=f'"{field}"', sales=SALES_TABLE, but000=BUT000_TABLE))
    rows = cur.fetchall()
    if not rows:
        log.warning("Nessun cliente con %s valorizzato — %s non generato.", field, output_name)
        return
    cols = ["KUNNR (codice ERP)", "BP SAP", "VKORG", "VTWEG", field]
    ok, discards = [], {}
    for kunnr, vkorg, vtweg, vals, partners, bp_name in rows:
        if len(vals) > 1 and not fixed_value:
            reason = f"Valori di {field} diversi tra le divisioni (SPART) dell'area di vendita {vkorg}/{vtweg}"
        elif not partners:
            reason = (f"Cliente non presente in SAP ({BUT000_TABLE}, BU_SORT2): {label} da assegnare dopo il "
                      "caricamento del cliente")
        elif len(partners) > 1:
            reason = f"Cliente associato a più BP SAP ({BUT000_TABLE}, BU_SORT2)"
        else:
            amount = [fixed_value or vals[0]] + ([currency] if currency else [])
            if sap_layout:
                ok.append([condition, vkorg, vtweg, partners[0], bp_name or "", *amount, cond_type,
                           *[""] * len(DUMMY_COLUMNS), *CONDITION_VALID])
            else:
                ok.append([condition, vkorg, vtweg, partners[0], *amount, cond_type, *CONDITION_VALID])
            continue
        discards.setdefault(reason, []).append((kunnr, ", ".join(partners), vkorg, vtweg, " | ".join(vals)))
    for reason, items in discards.items():
        add_discards(name, cols, items, "", reason)
        log.warning("  *** %s: %d righe escluse (%s): %s", name, len(items), reason.split(":")[0],
                    ", ".join(sorted({r[0] for r in items})))
    if not ok:
        log.warning("Nessun cliente con %s presente in SAP — %s non generato.", field, output_name)
        return
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(name)
    if sap_layout:
        ws.append(["", "KSCHL", "VKORG", "VTWEG", "KUNNR", "NAME", "VALORE_MAGGIORAZIONE", "TIPO_MAGGIORAZIONE",
                   *DUMMY_COLUMNS, "DATA_INIZIO", "DATA_FINE"])
    else:
        ws.append(["KSCHL", "VKORG", "VTWEG", "KUNNR", "VALORE_MAGGIORAZIONE", *(["DIVISA"] if currency else []),
                   "TIPO_MAGGIORAZIONE", "DATA_INIZIO", "DATA_FINE"])
    for n, row in enumerate(sorted(ok, key=lambda r: (r[3], r[1], r[2])), 1):
        ws.append([n, *row] if sap_layout else row)
    output_file = output_zip_path(OUTPUT_DIR, output_name)
    wb.save(output_file)
    log.info("  ✓  %s  (%d righe, %d clienti, %s; %d righe escluse)", output_file, len(ok),
             len({r[3] for r in ok}), label, sum(len(v) for v in discards.values()))


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Rimossi a ogni run: se oggi mancano i dati non deve restare il file di un run precedente
    names = [o[0] for o in OUTPUTS] + [c[0] for c in CONDITIONS]
    for name in names + LEGACY_NAMES:
        remove_previous_zips(OUTPUT_DIR, f"{name}.xlsx")
    reset_discards(names + LEGACY_NAMES)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for schema, table in (("prd", SALES_TABLE), ("ref", BUT000_TABLE)):
                if not table_exists(cur, schema, table):
                    log.warning("%s.%s non presente — file dei dati di vendita non generati.", schema, table)
                    return
            for spec in OUTPUTS:
                export_one(cur, *spec)
            for spec in CONDITIONS:
                export_condition(cur, *spec)
    finally:
        conn.close()
        flush_discards()


if __name__ == "__main__":
    main()
