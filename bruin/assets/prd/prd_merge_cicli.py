""" @bruin
name: prd.merge_cicli
type: python
description: >
  Merge dei cicli di lavoro (DM15_CICLI) raw + stg."S_OPERATION_STG" → prd."S_OPERATION",
  prd."S_PLKO", prd."S_MAPL" (full refresh a ogni run; senza la tabella STG le tre tabelle prd
  sono la copia di raw).
  Centri di lavoro: PRIMA del merge con la tabella STG, nelle operazioni copiate da raw
  WORK_CNTR viene trascodificato con ref.SAP_TRASCO_CDLAV (file SAP_TRASCO_CDLAV.xlsx in
  in_source_sap): WERKS = WERKS_WORK_CNTR e WORK_CNTR_OLD = WORK_CNTR → WORK_CNTR_NEW. Le
  operazioni da S_OPERATION_STG restano con il loro centro di lavoro. Centri non in tabella:
  invariati; coppie WERKS + WORK_CNTR_OLD con più WORK_CNTR_NEW diversi: non trascodificate.
  Riepilogo in report/S_OPERATION_trascodifica_centri_lavoro.xlsx.
  La tabella STG (file S_OPERATION_STG.xlsx in in_source_others) ha le colonne di S_OPERATION
  più LEGENDA (UPD/ADD) e PN_2 (codice materiale); PLNNR è vuoto e si ricava da raw.S_MAPL
  (MATNR = PN_2, WERKS_MAT = WERKS_WORK_CNTR, PLNAL):
    - UPD: le operazioni con la stessa chiave (PLNNR, PLNAL, PLNFL, VORNR) sostituiscono quelle
      di raw (riga intera); quelle con un VORNR non presente vengono aggiunte al ciclo; le altre
      operazioni del ciclo restano invariate. Ciclo non trovato in S_MAPL: riga scartata.
    - ADD: ciclo alternativo nuovo (PLNAL) del gruppo di cicli del materiale (PLNNR del ciclo già
      presente in S_MAPL): testata (S_PLKO) e attribuzione al materiale (S_MAPL) copiate dal
      ciclo 01 del materiale (o dal primo ciclo presente) con il nuovo PLNAL; operazioni aggiunte.
      Se il ciclo esiste già le operazioni sono trattate come UPD. Materiale senza cicli in
      S_MAPL (o in più gruppi): riga scartata.
  Valori normalizzati nel formato del file SAP: PLNAL e PLNFL a 2 cifre, VORNR a 4 cifre,
  ZZSTANDARD_TEXT_KEY a 3 cifre (come nel file ERP; Excel toglie gli zeri iniziali), numeri con
  la virgola decimale. Le righe STG con chiave duplicata non sono applicate (resta la riga di
  raw, se c'è). Righe scartate e duplicati: discarded/S_OPERATION_STG_discarded.xlsx (con il
  motivo); elenco completo dei duplicati in report/S_OPERATION_STG_chiavi_duplicate.xlsx.
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_others_to_stg
  - ingestion.ingest_xlsx_to_ref
@bruin """

import os
import re
import sys
import logging
from decimal import Decimal, InvalidOperation
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
from prd_merge_lib import get_connection, ensure_prd_schema
from prd_discard_lib import add_discards, flush_discards, reset_discards, REPORT_DIR, write_xlsx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

STG_TABLE   = "S_OPERATION_STG"
OPS, PLKO, MAPL = "S_OPERATION", "S_PLKO", "S_MAPL"
AUDIT_COLS  = {"_source", "_loaded_at", "_status", "_zip_source", "_xlsx_source"}
LEGEND_COL, MATERIAL_COL, PLANT_COL = "LEGENDA", "PN_2", "WERKS_WORK_CNTR"
PLNNR, PLNAL, PLNFL, VORNR = "PLNNR(k/*)", "PLNAL(k/*)", "PLNFL(k)", "VORNR(k/*)"
DUP_REPORT  = os.path.join(REPORT_DIR, "S_OPERATION_STG_chiavi_duplicate.xlsx")
WC_TRASCO   = "SAP_TRASCO_CDLAV"   # WERKS, WORK_CNTR_OLD, WORK_CNTR_NEW
WC_REPORT   = os.path.join(REPORT_DIR, "S_OPERATION_trascodifica_centri_lavoro.xlsx")
WC_COL      = "WORK_CNTR"
# Campi numerici del modello S_OPERATION (guida SAP): virgola decimale nel file per SAP
NUMERIC_COLS = {"BMSCH(*)", "UMREN", "UMREZ", "VGW01", "VGW02", "VGW03", "VGW04", "VGW05", "VGW06",
                "ZLMAX", "ZLPRO", "ZWNOR", "ZWMIN", "ZTNOR", "ZTMIN", "SPLIM", "ZMINB", "ZMINU",
                "MINWE", "ZZPO_ITEM", "PLIFZ", "PEINH", "PREIS", "QRASTZFAK"}
PAD = {PLNAL: 2, PLNFL: 2, VORNR: 4,
       "ZZSTANDARD_TEXT_KEY": 3}   # chiave testo standard: 3 cifre come nel file ERP (Excel toglie gli zeri)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def table_exists(cur, schema, table):
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{q(table)}",))
    return cur.fetchone()[0] is not None


def columns(cur, schema, table):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position", (schema, table))
    return [r[0] for r in cur.fetchall()]


def normalize(col, value):
    """Valore STG (testo letto da Excel) nel formato del file SAP."""
    v = "" if value is None else str(value)
    s = v.strip()
    if col in PAD and re.fullmatch(r"\d+(\.0+)?", s):
        return str(int(float(s))).zfill(PAD[col])
    if col in NUMERIC_COLS and s and re.fullmatch(r"-?[\d.]+([eE][-+]?\d+)?", s):
        try:
            d = Decimal(s)
        except InvalidOperation:
            return v
        text = format(d.normalize(), "f") if d != d.to_integral() else str(int(d))
        return text.replace(".", ",")
    return v


def copy_raw(cur, table):
    cur.execute(f"DROP TABLE IF EXISTS prd.{q(table)}")
    cur.execute(f"CREATE TABLE prd.{q(table)} AS SELECT * FROM raw.{q(table)}")


def main():
    reset_discards([STG_TABLE])
    for path in (DUP_REPORT, WC_REPORT):
        if os.path.exists(path):
            os.remove(path)
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            ensure_prd_schema(cur)
            for table in (OPS, PLKO, MAPL):
                if not table_exists(cur, "raw", table):
                    log.warning("raw.%s non presente — cicli non elaborati", table)
                    return
                copy_raw(cur, table)
            log.info("prd.S_OPERATION, prd.S_PLKO, prd.S_MAPL copiate da raw")
            transcode_work_centers(cur)   # prima del merge STG: solo le operazioni del file ERP
            if not table_exists(cur, "stg", STG_TABLE):
                log.info("stg.%s non presente — nessuna integrazione delle operazioni", STG_TABLE)
                return
            apply_stg(cur)
    finally:
        flush_discards()
        conn.close()
    log.info("=== merge_cicli completato ===")


def wc_norm(expr):
    """Codice come testo senza spazi e senza il ".0" che Excel aggiunge alle celle numeriche."""
    return f"regexp_replace(TRIM({expr}), '\\.0+$', '')"


def transcode_work_centers(cur):
    """WORK_CNTR di prd.S_OPERATION (copia di raw) → WORK_CNTR_NEW di ref.SAP_TRASCO_CDLAV."""
    if not table_exists(cur, "ref", WC_TRASCO):
        log.info("ref.%s non presente — centri di lavoro non trascodificati", WC_TRASCO)
        return
    needed = {"WERKS", "WORK_CNTR_OLD", "WORK_CNTR_NEW"}
    missing = needed - set(columns(cur, "ref", WC_TRASCO))
    if missing:
        log.warning("ref.%s senza le colonne %s — centri di lavoro non trascodificati",
                    WC_TRASCO, ", ".join(sorted(missing)))
        return
    cur.execute(f"""
        CREATE TEMP TABLE wc_map ON COMMIT DROP AS
        SELECT {wc_norm('"WERKS"')} AS werks, {wc_norm('"WORK_CNTR_OLD"')} AS old,
               ARRAY_AGG(DISTINCT {wc_norm('"WORK_CNTR_NEW"')}) AS news
        FROM ref.{q(WC_TRASCO)}
        WHERE COALESCE(TRIM("WORK_CNTR_OLD"), '') <> '' AND COALESCE(TRIM("WORK_CNTR_NEW"), '') <> ''
        GROUP BY 1, 2""")
    cur.execute(f"""
        SELECT m.werks, m.old, ARRAY_TO_STRING(m.news, ' | '), CARDINALITY(m.news) > 1, COUNT(o.*)
        FROM wc_map m
        LEFT JOIN prd.{q(OPS)} o ON TRIM(o.{q(PLANT_COL)}) = m.werks AND TRIM(o.{q(WC_COL)}) = m.old
        GROUP BY m.werks, m.old, m.news ORDER BY 1, 2""")
    rows = cur.fetchall()
    cur.execute(f"""
        UPDATE prd.{q(OPS)} o SET {q(WC_COL)} = m.news[1]
        FROM wc_map m
        WHERE CARDINALITY(m.news) = 1 AND TRIM(o.{q(PLANT_COL)}) = m.werks AND TRIM(o.{q(WC_COL)}) = m.old""")
    n_upd = cur.rowcount
    ambiguous = [r for r in rows if r[3]]
    for werks, old, news, _, n in ambiguous:
        log.warning("  *** Centro di lavoro %s/%s con più trascodifiche (%s): non trascodificato (%d operazioni)",
                    werks, old, news, n)
    report = [[werks, old, news, n, "NON trascodificato: più WORK_CNTR_NEW" if amb
               else ("Trascodificato" if n else "Nessuna operazione nel file")]
              for werks, old, news, amb, n in rows]
    write_xlsx(WC_REPORT, [("trascodifica", ["WERKS", "WORK_CNTR_OLD", "WORK_CNTR_NEW", "Operazioni", "Esito"],
                            report)])
    log.info("Centri di lavoro trascodificati (%s): %d operazioni, %d coppie in tabella (%d con operazioni, "
             "%d con più trascodifiche) — %s", WC_TRASCO, n_upd, len(rows), sum(1 for r in rows if r[4] and not r[3]),
             len(ambiguous), WC_REPORT)


def apply_stg(cur):
    stg_cols = [c for c in columns(cur, "stg", STG_TABLE) if c not in AUDIT_COLS]
    ops_cols = [c for c in columns(cur, "raw", OPS) if c not in AUDIT_COLS]
    missing = [c for c in ops_cols if c != PLNNR and c not in stg_cols]
    if missing:
        log.warning("  Colonne di S_OPERATION assenti nel file STG (vuote nelle righe STG): %s", ", ".join(missing))
    cur.execute(f"SELECT {', '.join(q(c) for c in stg_cols)} FROM stg.{q(STG_TABLE)}")
    stg_rows = cur.fetchall()
    idx = {c: i for i, c in enumerate(stg_cols)}

    # Cicli dei materiali: (MATNR, divisione, PLNAL) -> PLNNR, e gruppi di cicli per materiale
    cur.execute(f'SELECT TRIM({q("MATNR(k/*)")}), TRIM({q("WERKS_MAT(k/*)")}), TRIM({q(PLNAL)}), TRIM({q(PLNNR)}), '
                f'"_source" FROM raw.{q(MAPL)}')
    cycle, groups, source_of = {}, {}, {}
    for mat, plant, plnal, plnnr, src in cur.fetchall():
        cycle[(mat, plant, plnal)] = plnnr
        groups.setdefault((mat, plant), set()).add(plnnr)
        source_of[plnnr] = src
    cur.execute(f"SELECT TRIM({q(PLNNR)}), TRIM({q(PLNAL)}), TRIM({q(PLNFL)}), TRIM({q(VORNR)}) FROM raw.{q(OPS)}")
    raw_keys = set(cur.fetchall())

    resolved, discarded = {}, []          # chiave -> lista di (riga STG, riga S_OPERATION)
    new_cycles = {}                       # (PLNNR, PLNAL) -> (materiale, divisione)
    for row in stg_rows:
        get = lambda c: normalize(c, row[idx[c]]) if c in idx else ""
        legend, mat, plant = get(LEGEND_COL).strip().upper(), get(MATERIAL_COL).strip(), get(PLANT_COL).strip()
        plnal, plnfl, vornr = get(PLNAL).strip(), get(PLNFL).strip(), get(VORNR).strip()
        plnnr = cycle.get((mat, plant, plnal))
        if not plnnr:
            if legend == "ADD" and len(groups.get((mat, plant), ())) == 1:
                plnnr = next(iter(groups[(mat, plant)]))
                new_cycles[(plnnr, plnal)] = (mat, plant)
            else:
                reason = (f"Ciclo non trovato: materiale [{mat}] divisione [{plant}] ciclo [{plnal}] non presente in S_MAPL"
                          if legend != "ADD" else
                          f"Materiale [{mat}] divisione [{plant}] senza un gruppo di cicli in S_MAPL "
                          f"({len(groups.get((mat, plant), ()))} gruppi): ciclo alternativo [{plnal}] non creabile")
                discarded.append((row, reason))
                continue
        out = [plnnr if c == PLNNR else get(c) for c in ops_cols]
        resolved.setdefault((plnnr, plnal, plnfl, vornr), []).append((row, out))

    dups = {k: v for k, v in resolved.items() if len(v) > 1}
    for key, items in dups.items():
        for row, _ in items:
            discarded.append((row, f"Chiave duplicata nel file STG: {len(items)} righe per ciclo {key[0]}/{key[1]}, "
                                   f"sequenza {key[2]}, operazione {key[3]} — non applicata"))
    apply = {k: v[0][1] for k, v in resolved.items() if len(v) == 1}
    new_cycles = {k: v for k, v in new_cycles.items() if any(a[0] == k[0] and a[1] == k[1] for a in apply)}

    # Operazioni: sostituite (stessa chiave) o aggiunte
    replaced = sum(1 for k in apply if k in raw_keys)
    if apply:
        cur.execute("CREATE TEMP TABLE stg_ops (plnnr text, plnal text, plnfl text, vornr text)")
        execute_values(cur, "INSERT INTO stg_ops VALUES %s", list(apply))
        cur.execute(f"""DELETE FROM prd.{q(OPS)} o USING stg_ops t
                        WHERE TRIM(o.{q(PLNNR)}) = t.plnnr AND TRIM(o.{q(PLNAL)}) = t.plnal
                          AND TRIM(o.{q(PLNFL)}) = t.plnfl AND TRIM(o.{q(VORNR)}) = t.vornr""")
        col_list = ", ".join(q(c) for c in ops_cols + ["_source", "_loaded_at"])
        execute_values(cur, f"INSERT INTO prd.{q(OPS)} ({col_list}) VALUES %s",
                       [out + [source_of.get(k[0]), None] for k, out in apply.items()])
    cur.execute(f"UPDATE prd.{q(OPS)} SET _loaded_at = NOW() WHERE _loaded_at IS NULL")

    # Cicli alternativi nuovi: testata e attribuzione materiale copiate dal ciclo 01 (o dal primo)
    for table in (PLKO, MAPL):
        cols = [c for c in columns(cur, "raw", table)]
        for (plnnr, plnal), (mat, plant) in sorted(new_cycles.items()):
            extra = f' AND TRIM({q("MATNR(k/*)")}) = %s AND TRIM({q("WERKS_MAT(k/*)")}) = %s' if table == MAPL else ""
            params = [plnnr] + ([mat, plant] if table == MAPL else [])
            cur.execute(f"""INSERT INTO prd.{q(table)} ({', '.join(q(c) for c in cols)})
                            SELECT {', '.join('%s' if c == PLNAL else q(c) for c in cols)}
                            FROM raw.{q(table)} WHERE TRIM({q(PLNNR)}) = %s{extra}
                            ORDER BY TRIM({q(PLNAL)}) <> '01', TRIM({q(PLNAL)}) LIMIT 1""",
                        [plnal] + params)

    reasons = {id(r): reason for r, reason in discarded}
    if discarded:
        add_discards(STG_TABLE, stg_cols, [r for r, _ in discarded], "", lambda r: reasons[id(r)])
    if dups:
        rows = [[k[0], k[1], k[2], k[3], len(v)] + [row[idx[c]] for c in stg_cols]
                for k, v in sorted(dups.items()) for row, _ in v]
        write_xlsx(DUP_REPORT, [("chiavi_duplicate", ["PLNNR", "PLNAL", "PLNFL", "VORNR", "RIGHE"] + stg_cols, rows)])
    log.info("  STG %s: %d righe — %d operazioni sostituite, %d aggiunte, %d cicli alternativi nuovi "
             "(testata e materiale dal ciclo 01), %d righe scartate (di cui %d con chiave duplicata: %d chiavi)",
             STG_TABLE, len(stg_rows), replaced, len(apply) - replaced, len(new_cycles), len(discarded),
             sum(len(v) for v in dups.values()), len(dups))
    if dups:
        log.warning("  [STG] %d chiavi duplicate nel file, elenco in %s", len(dups), DUP_REPORT)


if __name__ == "__main__":
    main()
