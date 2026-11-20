"""
Libreria comune per i controlli Python basati su una query (es. ordini di acquisto CK062-CK524,
società partner CK063, CK416, CK417).
Non è un asset Bruin — viene importata dagli asset ckNNN.

Il controllo esegue una query che restituisce una riga per oggetto controllato con le colonne
(source_table, object_key, message, zip_source): message NULL = Ok, altrimenti testo dell'errore,
scritto con la gravità del catalogo. Se una delle tabelle richieste non è caricata il controllo
viene saltato (il flusso può non essere presente in tutti gli ambienti).
"""

import os
import sys
import csv
import io
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from mdg_env import DB_CONFIG, IN_PPROD  # noqa: E402


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{q(table)}",))
    return cur.fetchone()[0] is not None


def material_in_sap_sql(cur, tables: dict) -> str | None:
    """
    Query dei controlli "materiale già migrato in SAP" (CK525, CK527-CK531): per ogni tabella
    caricata di tables = {tabella: (object_key, materiale, divisione o None, filtro o None)}
    il materiale (con la divisione, se indicata e valorizzata) deve essere in
    ref.SAP_EXPORT_MARC. Confronto senza spazi iniziali e finali; None se nessuna tabella
    è caricata. Join sugli insiemi distinti (hash join): una NOT EXISTS correlata per riga
    diventa lentissima con le distinte base (centinaia di migliaia di righe x tutto MARC).
    """
    parts = []
    for table, (key, mat, plant, where) in tables.items():
        if not table_exists(cur, "raw", table):
            continue
        if plant:
            found = f"CASE WHEN {plant} IS NULL THEN mm.matnr IS NOT NULL ELSE mp.matnr IS NOT NULL END"
            joins = (f"LEFT JOIN sap_mat mm ON mm.matnr = TRIM({mat})\n"
                     f"LEFT JOIN sap_marc mp ON mp.matnr = TRIM({mat}) AND mp.werks = TRIM({plant})")
            label = f"'Materiale [' || TRIM({mat}) || '] divisione [' || COALESCE(TRIM({plant}), '') || ']'"
        else:
            found = "mm.matnr IS NOT NULL"
            joins = f"LEFT JOIN sap_mat mm ON mm.matnr = TRIM({mat})"
            label = f"'Materiale [' || TRIM({mat}) || ']'"
        parts.append(f"""
SELECT '{table}', {key},
       CASE WHEN NOT ({found})
            THEN {label} || ' non presente in SAP (SAP_EXPORT_MARC): la migrazione darebbe errore'
       END,
       x."_source",
       {label} || ' presente in SAP'
FROM raw."{table}" x
{joins}
{"WHERE " + where if where else ""}""")
    if not parts:
        return None
    return ('WITH sap_marc AS (SELECT DISTINCT TRIM("MATNR") AS matnr, TRIM("WERKS") AS werks '
            'FROM ref."SAP_EXPORT_MARC"),\n'
            '     sap_mat AS (SELECT DISTINCT matnr FROM sap_marc)' + "\nUNION ALL".join(parts))


# Tempo di consegna assente nell'info record e nel materiale (CK516 e prd.export_inforecord_zip):
# valore riconoscibile scritto in APLFZ nel file per SAP, altrimenti l'info record è respinto
LEAD_TIME_PLACEHOLDER = "999"
LEAD_TIME_TABLES = ("S_EINE#INFORDATIACQ", "S_EINA#INFORMATFOR", "S_MARC")
_NO_LEAD_TIME = r"'^0*([.,]0*)?$'"   # vuoto o zero (000, 0, 0.0)


def lead_time_missing_sql(cur) -> str:
    """
    Una riga per ogni riga di S_EINE con materiale e divisione noti, colonne
    (infnr, ekorg, esokz, werks, matnr, src, forza). forza = APLFZ e PLIFZ vuoti o zero; PLIFZ
    è quello di ref.SAP_EXPORT_MARC o, se la coppia materiale/divisione non c'è, di raw.S_MARC.
    """
    if table_exists(cur, "ref", "SAP_EXPORT_MARC"):
        sap_cte = """SELECT DISTINCT ON (TRIM("MATNR"), TRIM("WERKS"))
                            TRIM("MATNR") AS matnr, TRIM("WERKS") AS werks, TRIM("PLIFZ") AS plifz
                     FROM ref."SAP_EXPORT_MARC" ORDER BY 1, 2"""
    else:
        sap_cte = "SELECT NULL::text AS matnr, NULL::text AS werks, NULL::text AS plifz WHERE FALSE"
    return f"""
WITH sap AS ({sap_cte}),
     erp AS (SELECT DISTINCT ON (TRIM("PRODUCT(k/*)"), TRIM("WERKS(k/*)"))
                    TRIM("PRODUCT(k/*)") AS matnr, TRIM("WERKS(k/*)") AS werks, TRIM("PLIFZ") AS plifz
             FROM raw."S_MARC" ORDER BY 1, 2)
SELECT e."INFNR(k/*)" AS infnr, e."EKORG(k/*)" AS ekorg, e."ESOKZ(k/*)" AS esokz, e."WERKS(k)" AS werks,
       TRIM(a."MATNR") AS matnr, e."_source" AS src,
       COALESCE(TRIM(e."APLFZ"), '') ~ {_NO_LEAD_TIME}
       AND CASE WHEN s.matnr IS NOT NULL THEN COALESCE(s.plifz, '') ELSE COALESCE(m.plifz, '') END
           ~ {_NO_LEAD_TIME} AS forza
FROM raw."S_EINE#INFORDATIACQ" e
JOIN raw."S_EINA#INFORMATFOR" a ON a."INFNR(k/*)" = e."INFNR(k/*)"
LEFT JOIN sap s ON s.matnr = TRIM(a."MATNR") AND s.werks = TRIM(e."WERKS(k)")
LEFT JOIN erp m ON m.matnr = TRIM(a."MATNR") AND m.werks = TRIM(e."WERKS(k)")
WHERE s.matnr IS NOT NULL OR m.matnr IS NOT NULL"""


# Info record cliente-materiale: cliente in SAP (CK532, CK534, prd.export_custmat_zip)
CUSTMAT_TABLE = "S_CUSTMAT#INFORMATFOR"
CUSTMAT_VIA = {1: "codice BP", 2: "chiave di ricerca BU_SORT2", 3: "chiave di ricerca BU_SORT1"}


def custmat_customer_sql() -> str:
    """
    Una riga per ogni CUSTOMER distinto di S_CUSTMAT#INFORMATFOR (senza spazi), colonne
    (cust, partners, n, via): BP di ref.SAP_EXPORT_BUT000 trovati con il criterio più forte,
    nell'ordine codice BP (PARTNER, via 1), BU_SORT2 (vecchio codice cliente/DM, via 2),
    BU_SORT1 (via 3). n = numero di BP trovati (0 = non in SAP, >1 = ambiguo).
    """
    return f"""
WITH b AS (SELECT DISTINCT TRIM("PARTNER") AS partner, NULLIF(TRIM("BU_SORT2"), '') AS s2,
                  NULLIF(TRIM("BU_SORT1"), '') AS s1
           FROM ref."SAP_EXPORT_BUT000"),
     c AS (SELECT DISTINCT TRIM("CUSTOMER(k/*)") AS cust FROM raw."{CUSTMAT_TABLE}"),
     m AS (SELECT c.cust, b.partner, 1 AS via FROM c JOIN b ON b.partner = c.cust
           UNION ALL SELECT c.cust, b.partner, 2 FROM c JOIN b ON b.s2 = c.cust
           UNION ALL SELECT c.cust, b.partner, 3 FROM c JOIN b ON b.s1 = c.cust),
     best AS (SELECT cust, MIN(via) AS via FROM m GROUP BY cust)
SELECT c.cust, string_agg(DISTINCT m.partner, ', ' ORDER BY m.partner) AS partners,
       COUNT(DISTINCT m.partner) AS n, best.via
FROM c
LEFT JOIN best ON best.cust = c.cust
LEFT JOIN m ON m.cust = c.cust AND m.via = best.via
GROUP BY c.cust, best.via"""


CUSTMAT_KEY_COLS = ["SALESORGANIZATION(k/*)", "DISTRIBUTIONCHANNEL(k/*)", "CUSTOMER(k/*)", "MATERIAL(k/*)"]
CUSTMAT_KDMAT = "MATERIALBYCUSTOMER"           # codice materiale del cliente (KDMAT in SAP)
CUSTMAT_KNMT = "SAP_EXPORT_KNMT"               # info record cliente-materiale già in SAP
CUSTMAT_DUP_TABLE = "custmat_dup"              # tabella temporanea di custmat_duplicates()
INBOUND_PATH = IN_PPROD
# object_key degli info record cliente-materiale (valori come in raw): VKORG|VTWEG|CUSTOMER|MATERIAL
CUSTMAT_OBJECT_KEY = ('x."SALESORGANIZATION(k/*)" || \'|\' || x."DISTRIBUTIONCHANNEL(k/*)" || \'|\' '
                      '|| x."CUSTOMER(k/*)" || \'|\' || x."MATERIAL(k/*)"')


def _read_zip_csv(zip_path: str, csv_name: str) -> list[list[str]]:
    """Righe (intestazione compresa) del CSV csv_name dentro lo ZIP, anche in una sottocartella."""
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if os.path.basename(n).lower() == csv_name.lower()]
        if not names:
            return []
        raw = zf.read(names[0])
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")          # come l'ingestione: file non UTF-8 letti come Windows-1252
    return list(csv.reader(io.StringIO(text), delimiter=";"))


def custmat_duplicates(cur) -> int:
    """
    Chiavi duplicate degli info record cliente-materiale nei file in ingresso: l'ingestione carica
    in raw solo la prima riga di ogni chiave, le altre si leggono dallo ZIP (_source) in in_source_pprod.
    Crea la tabella temporanea custmat_dup (src, vkorg, vtweg, cust, matnr, n, variants): n = righe
    con la chiave, variants = valori distinti di MATERIALBYCUSTOMER nell'ordine del file.
    Chiavi senza spazi iniziali e finali. Restituisce il numero di chiavi duplicate.
    """
    cur.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS {CUSTMAT_DUP_TABLE}
            (src text, vkorg text, vtweg text, cust text, matnr text, n int, variants text)""")
    cur.execute(f"TRUNCATE {CUSTMAT_DUP_TABLE}")
    cur.execute(f'SELECT DISTINCT "_source" FROM raw."{CUSTMAT_TABLE}" WHERE "_source" IS NOT NULL')
    out = []
    for (src,) in cur.fetchall():
        path = os.path.join(INBOUND_PATH, src)
        if not os.path.isfile(path):
            print(f"[WARN] {path} non trovato: duplicati di {src} non verificabili")
            continue
        rows = _read_zip_csv(path, f"{CUSTMAT_TABLE}.csv")
        if not rows:
            continue
        header = [c.strip() for c in rows[0]]
        if any(c not in header for c in CUSTMAT_KEY_COLS + [CUSTMAT_KDMAT]):
            print(f"[WARN] {src}: colonne chiave o {CUSTMAT_KDMAT} assenti — duplicati non verificati")
            continue
        idx = [header.index(c) for c in CUSTMAT_KEY_COLS]
        kd = header.index(CUSTMAT_KDMAT)
        groups = defaultdict(list)
        for row in rows[1:]:
            if len(row) != len(header):      # righe malformate: scartate anche dall'ingestione
                continue
            groups[tuple(row[i].strip() for i in idx)].append(row[kd].strip())
        for key, kdmats in groups.items():
            if len(kdmats) > 1:
                variants = list(dict.fromkeys(kdmats))
                out.append((src, *key, len(kdmats), ", ".join(f"[{v}]" for v in variants)))
    if out:
        execute_values(cur, f"INSERT INTO {CUSTMAT_DUP_TABLE} VALUES %s", out)
    return len(out)


def custmat_in_sap_sql() -> str:
    """
    Info record cliente-materiale già presenti in SAP (ref.SAP_EXPORT_KNMT) sulla chiave del
    Migration Cockpit: stessi VKORG, VTWEG, KUNNR (CUSTOMER trascodificato con il BP SAP come in
    CK534, un solo BP) e MATNR. SAP rifiuta l'inserimento anche se KDMAT è diverso ("instance
    already exists"), quindi MATERIALBYCUSTOMER non è confrontato: kdmat è quello di SAP.
    Valori senza spazi iniziali e finali. Colonne: src (_source), object_key, cust, partner, kdmat.
    """
    return f"""
SELECT DISTINCT ON (x."_source", {CUSTMAT_OBJECT_KEY})
       x."_source" AS src, {CUSTMAT_OBJECT_KEY} AS object_key, c.cust, c.partners AS partner,
       TRIM(COALESCE(k."KDMAT", '')) AS kdmat
FROM raw."{CUSTMAT_TABLE}" x
JOIN ({custmat_customer_sql()}) c ON c.cust = TRIM(x."CUSTOMER(k/*)") AND c.n = 1
JOIN ref."{CUSTMAT_KNMT}" k
  ON TRIM(k."VKORG") = TRIM(x."SALESORGANIZATION(k/*)")
 AND TRIM(k."VTWEG") = TRIM(x."DISTRIBUTIONCHANNEL(k/*)")
 AND TRIM(k."KUNNR") = c.partners
 AND TRIM(k."MATNR") = TRIM(x."MATERIAL(k/*)")
ORDER BY x."_source", {CUSTMAT_OBJECT_KEY}, kdmat"""

# Ordini di acquisto (DM27): trascodifica del centro di costo DBCOGE in S_EKKN.KOSTL
PO_ACCOUNT_TABLE = "S_EKKN"
PO_ITEM_TABLE    = "S_EKPO"
CDC_TRASCO       = "SAP_TRASCO_CDC"     # COD_DBCOGE + DIV -> COD_SAP_DEF
PO_ACCOUNT_KEY   = ('x."EBELN(k/*)" || \'|\' || x."EBELP(k/*)" || \'|\' || x."ZEKKN(k/*)"')


def po_cdc_sql() -> str:
    """
    Righe di S_EKKN con KOSTL valorizzato e la trascodifica del centro di costo DBCOGE nel codice SAP:
    ref.SAP_TRASCO_CDC con COD_DBCOGE = KOSTL e DIV = WERKS della posizione (S_EKPO, stessi EBELN e
    EBELP), valori senza spazi iniziali e finali. COD_SAP_DEF vuoto o con '?' (es. '??') non è una
    trascodifica valida. Colonne: src (_source), object_key (EBELN|EBELP|ZEKKN), kostl, werks
    (NULL se la posizione non esiste), n_rec (record di trascodifica trovati), n_sap (codici SAP
    validi distinti), sap (il codice SAP se è uno solo), sap_list.
    """
    return f"""
SELECT x."_source" AS src, {PO_ACCOUNT_KEY} AS object_key, TRIM(x."KOSTL") AS kostl, p.werks,
       COUNT(t.cod) AS n_rec,
       COUNT(DISTINCT t.cod) FILTER (WHERE t.valid) AS n_sap,
       CASE WHEN COUNT(DISTINCT t.cod) FILTER (WHERE t.valid) = 1
            THEN MIN(t.cod) FILTER (WHERE t.valid) END AS sap,
       STRING_AGG(DISTINCT t.cod, ', ') FILTER (WHERE t.valid) AS sap_list
FROM raw."{PO_ACCOUNT_TABLE}" x
LEFT JOIN (SELECT DISTINCT "_source" AS src, TRIM("EBELN(k/*)") AS ebeln, TRIM("EBELP(k/*)") AS ebelp,
                  TRIM("WERKS") AS werks
           FROM raw."{PO_ITEM_TABLE}") p
       ON p.src = x."_source" AND p.ebeln = TRIM(x."EBELN(k/*)") AND p.ebelp = TRIM(x."EBELP(k/*)")
LEFT JOIN (SELECT TRIM("COD_DBCOGE") AS kostl, TRIM("DIV") AS werks,
                  TRIM(COALESCE("COD_SAP_DEF", '')) AS cod,
                  TRIM(COALESCE("COD_SAP_DEF", '')) <> '' AND POSITION('?' IN "COD_SAP_DEF") = 0 AS valid
           FROM ref."{CDC_TRASCO}") t
       ON t.kostl = TRIM(x."KOSTL") AND t.werks = p.werks
WHERE TRIM(COALESCE(x."KOSTL", '')) <> ''
GROUP BY x."_source", {PO_ACCOUNT_KEY}, TRIM(x."KOSTL"), p.werks"""


def run_query_check(check_id: str, sql, required: list, category: str = "PO"):
    """
    required: tabelle necessarie come (schema, tabella); se ne manca una il controllo è saltato.
    sql: query con le colonne (source_table, object_key, message, zip_source), oppure funzione
         che riceve il cursore e restituisce la query (None = niente da controllare).
         Una quinta colonna facoltativa è il messaggio degli esiti Ok (vuota = "Ok").
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(is_active, FALSE), severity FROM stg.check_catalog WHERE check_id = %s",
                        (check_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                print(f"[SKIP] {check_id} disattivato nel catalogo")
                return
            severity = row[1] or "Error"
            missing = [f"{s}.{t}" for s, t in required if not table_exists(cur, s, t)]
            if missing:
                print(f"[SKIP] {check_id} — tabelle non caricate: {', '.join(missing)}")
                return

            cur.execute("SELECT run_id FROM stg.pipeline_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1")
            run_id = cur.fetchone()[0]
            now = datetime.now(timezone.utc)

            if callable(sql):
                sql = sql(cur)
                if not sql:
                    print(f"[SKIP] {check_id} — nessuna tabella da controllare caricata")
                    return
            cur.execute(sql)
            results, n_err = [], 0
            for source_table, key, msg, source, *ok_msg in cur.fetchall():
                if msg:
                    n_err += 1
                    results.append((source_table, category, key, check_id, msg, severity, run_id, source, now))
                else:
                    results.append((source_table, category, key, check_id, (ok_msg or [None])[0] or "Ok", "Ok",
                                    run_id, source, now))

            if results:
                execute_values(cur, """
                    INSERT INTO stg.check_results (
                        source_table, category, object_key, check_id,
                        message, status, run_id, zip_source, created_at
                    ) VALUES %s
                """, results, page_size=500)
        conn.commit()
        print(f"[{'WARN' if n_err else 'OK'}] {check_id}: {len(results)} righe, {n_err} segnalazioni")
    finally:
        conn.close()
