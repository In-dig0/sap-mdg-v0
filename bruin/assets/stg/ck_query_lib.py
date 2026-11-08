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
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


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
