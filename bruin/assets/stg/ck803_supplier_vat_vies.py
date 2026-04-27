""" @bruin
name: stg.ck803_supplier_vat_vies
type: python
depends:
  - stg.clean_check_results
description: >
  CK803 — EXT_REF: Fornitori: verifica validità Partita IVA EU/UK
  tramite servizi esterni (VIES SOAP + HMRC API v2).
  Legge da raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc" i record con
  TAXTYPE che termina in '0' (= P.IVA CEE).
  Routing per paese:
    - EU (AT,BE,DE,IT,...,XI): WS VIES (SOAP, Commissione Europea)
    - GB (UK mainland):        HMRC API v2 (OAuth2 client_credentials)
      Se HMRC_CLIENT_ID/SECRET mancano: status GB_NO_CREDENTIALS
    - Altri (IL, US, ...):     NOT_EU, nessuna chiamata esterna
  Risultati scritti in stg.check_vat_vies (tabella di dettaglio).
  Il check CK804 legge questa tabella e scrive la sintesi in
  stg.check_results come tutti gli altri check della pipeline.
@bruin """

import os
import re
import time
import logging
from datetime import datetime, timezone, date
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from zeep import Client
from zeep.exceptions import Fault, TransportError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ck803")

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------
CHECK_ID     = "CK803"
SOURCE_TABLE = "S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
CATEGORY     = "BP"

VIES_WSDL = "https://ec.europa.eu/taxation_customs/vies/checkVatService.wsdl"

EU_COUNTRIES = {
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES",
    "FI", "FR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
    "NL", "PL", "PT", "RO", "SE", "SI", "SK", "XI",
}
GB_COUNTRIES = {"GB"}

INTER_CALL_DELAY = 0.5

HMRC_TOKEN_URL  = "https://api.service.hmrc.gov.uk/oauth/token"
HMRC_LOOKUP_URL = "https://api.service.hmrc.gov.uk/organisations/vat/check-vat-number/lookup/{vat_number}"
HMRC_SCOPE      = "read:vat"

# ---------------------------------------------------------------------------
# DB Config
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ.get("POSTGRES_DB", "mdg"),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

# ---------------------------------------------------------------------------
# DDL tabella di dettaglio
# ---------------------------------------------------------------------------
DDL_CHECK_VAT_VIES = """
CREATE TABLE IF NOT EXISTS stg.check_vat_vies (
    id                SERIAL PRIMARY KEY,
    run_id            INTEGER      NOT NULL,
    check_id          VARCHAR(10)  NOT NULL DEFAULT 'CK801',
    entity_type       VARCHAR(20)  NOT NULL,   -- 'vendor' | 'customer'
    entity_id         VARCHAR(100) NOT NULL,   -- KUNNR o LIFNR
    vat_number        VARCHAR(50)  NOT NULL,   -- VAT normalizzato (CC+numero)
    country_code      CHAR(2),                 -- es. 'IT', 'DE', 'GB', 'XI'
    vat_local         VARCHAR(50),             -- parte numerica senza prefisso paese
    is_eu             BOOLEAN,                 -- True = verificato su VIES
    is_gb             BOOLEAN,                 -- True = verificato su HMRC
    vies_valid        BOOLEAN,                 -- NULL = non verificato
    vies_name         TEXT,                    -- ragione sociale restituita dal servizio
    vies_address      TEXT,                    -- indirizzo restituito dal servizio
    vies_request_date DATE,                    -- data della verifica restituita dal servizio
    check_status      VARCHAR(25)  NOT NULL,
    -- VALID              : P.IVA valida (VIES o HMRC)
    -- INVALID            : P.IVA non valida
    -- ERROR              : Errore di rete o del servizio esterno
    -- NOT_EU             : Paese non EU e non GB, nessuna verifica esterna
    -- GB_NO_CREDENTIALS  : VAT GB ma credenziali HMRC non configurate nel .env
    error_message     TEXT,
    zip_source        TEXT,                    -- campo _source dalla tabella raw
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE stg.check_vat_vies IS
    'Dettaglio verifiche P.IVA EU (VIES) e UK (HMRC API v2). '
    'La sintesi viene scritta in stg.check_results da CK802.';

CREATE INDEX IF NOT EXISTS idx_check_vat_vies_run_id
    ON stg.check_vat_vies (run_id);
CREATE INDEX IF NOT EXISTS idx_check_vat_vies_entity
    ON stg.check_vat_vies (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_check_vat_vies_status
    ON stg.check_vat_vies (check_status);
"""

# ---------------------------------------------------------------------------
# Normalizzazione VAT
# ---------------------------------------------------------------------------
def normalize_taxnum(taxtype: str, taxnum: str) -> str:
    """
    Restituisce il VAT nel formato CC+numero (es. "IT12345678901").
    TAXTYPE ha formato "{CC}0" (es. "IT0").
    TAXNUM può già contenere il prefisso (es. "DE129205077")
    oppure solo la parte locale (es. "502534702" per PT0).
    """
    cc  = taxtype[:2].upper()
    num = re.sub(r"[\s.\-]", "", taxnum.upper())
    return num if num.startswith(cc) else cc + num


def parse_vat(vat: str) -> tuple[str, str, bool, bool]:
    """
    Restituisce (country_code, vat_local, is_eu, is_gb).
    """
    normalized = re.sub(r"[\s.\-]", "", vat.upper())
    m = re.match(r"^([A-Z]{2})(.+)$", normalized)
    if not m:
        return ("", normalized, False, False)
    cc    = m.group(1)
    local = m.group(2)
    return (cc, local, cc in EU_COUNTRIES, cc in GB_COUNTRIES)


# ---------------------------------------------------------------------------
# Client VIES
# ---------------------------------------------------------------------------
class ViesClient:
    def __init__(self):
        log.info("Inizializzazione client VIES...")
        self._client = Client(VIES_WSDL)
        log.info("Client VIES pronto.")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((TransportError, Exception)),
        reraise=True,
    )
    def check_vat(self, country_code: str, vat_local: str) -> dict:
        result = self._client.service.checkVat(
            countryCode=country_code,
            vatNumber=vat_local,
        )
        return {
            "valid":        result.valid,
            "name":         result.name or None,
            "address":      result.address or None,
            "request_date": result.requestDate,
        }


# ---------------------------------------------------------------------------
# Client HMRC API v2
# ---------------------------------------------------------------------------
class HmrcClient:
    def __init__(self):
        self._client_id     = os.environ.get("HMRC_CLIENT_ID")
        self._client_secret = os.environ.get("HMRC_CLIENT_SECRET")
        self._token:        Optional[str] = None
        self._token_expiry: float = 0.0

        if self.is_available():
            log.info("Client HMRC API v2 pronto (credenziali trovate).")
        else:
            log.warning(
                "HMRC_CLIENT_ID/SECRET non trovati nel .env. "
                "VAT GB → GB_NO_CREDENTIALS."
            )

    def is_available(self) -> bool:
        return bool(self._client_id and self._client_secret)

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        log.debug("Rinnovo token OAuth2 HMRC...")
        resp = requests.post(
            HMRC_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
                "scope":         HMRC_SCOPE,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token        = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 14400)
        log.debug("Token HMRC rinnovato.")
        return self._token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def check_vat(self, vat_local: str) -> dict:
        """
        GET /organisations/vat/check-vat-number/lookup/{vatNumber}
        HTTP 404 = VAT non registrata / invalida.
        """
        token = self._get_token()
        url   = HMRC_LOOKUP_URL.format(vat_number=vat_local)
        resp  = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept":        "application/vnd.hmrc.2.0+json",
            },
            timeout=15,
        )
        if resp.status_code == 404:
            return {"valid": False, "name": None, "address": None, "request_date": None}
        resp.raise_for_status()
        data   = resp.json()
        target = data.get("target", {})
        addr   = target.get("address", {})
        addr_str = ", ".join(
            v for v in [
                addr.get("line1"), addr.get("line2"), addr.get("line3"),
                addr.get("line4"), addr.get("postcode"), addr.get("countryCode"),
            ] if v
        ) or None
        return {
            "valid":        target.get("isValid", False),
            "name":         target.get("name") or None,
            "address":      addr_str,
            "request_date": data.get("processingDate"),
        }


# ---------------------------------------------------------------------------
# Check singolo record
# ---------------------------------------------------------------------------
def check_single(
    vies: ViesClient,
    hmrc: HmrcClient,
    run_id: int,
    entity_id: str,
    vat_raw: str,
    zip_source: str,
) -> dict:
    """
    Restituisce un dict pronto per l'INSERT in stg.check_vat_vies.
    """
    cc, local, is_eu, is_gb = parse_vat(vat_raw)
    now = datetime.now(timezone.utc)

    base = {
        "run_id":           run_id,
        "check_id":         CHECK_ID,
        "entity_type":      "vendor",
        "entity_id":        entity_id,
        "vat_number":       vat_raw,
        "country_code":     cc or None,
        "vat_local":        local or None,
        "is_eu":            is_eu,
        "is_gb":            is_gb,
        "vies_valid":       None,
        "vies_name":        None,
        "vies_address":     None,
        "vies_request_date": None,
        "check_status":     "PENDING",
        "error_message":    None,
        "zip_source":       zip_source,
        "created_at":       now,
    }

    # Paese non verificabile
    if not is_eu and not is_gb:
        base["check_status"] = "NOT_EU"
        log.debug(f"  [supplier:{entity_id}] {vat_raw} → NOT_EU")
        return base

    # Branch EU → VIES
    if is_eu:
        try:
            resp = vies.check_vat(cc, local)
            base.update({
                "vies_valid":        resp["valid"],
                "vies_name":         resp["name"],
                "vies_address":      resp["address"],
                "vies_request_date": resp["request_date"],
                "check_status":      "VALID" if resp["valid"] else "INVALID",
            })
            log.info(
                f"  [supplier:{entity_id}] {vat_raw} → {base['check_status']} (VIES)"
                + (f" | {resp['name']}" if resp["name"] else "")
            )
        except Fault as e:
            base["check_status"]  = "ERROR"
            base["error_message"] = f"SOAP Fault: {e.message}"
            log.warning(f"  [supplier:{entity_id}] {vat_raw} → SOAP Fault: {e.message}")
        except Exception as e:
            base["check_status"]  = "ERROR"
            base["error_message"] = str(e)
            log.warning(f"  [supplier:{entity_id}] {vat_raw} → ERROR VIES: {e}")
        return base

    # Branch GB → HMRC
    if is_gb:
        if not hmrc.is_available():
            base["check_status"]  = "GB_NO_CREDENTIALS"
            base["error_message"] = (
                "HMRC_CLIENT_ID/SECRET non configurati. "
                "Registrarsi su https://developer.service.hmrc.gov.uk"
            )
            log.warning(f"  [supplier:{entity_id}] {vat_raw} → GB_NO_CREDENTIALS")
            return base
        try:
            resp = hmrc.check_vat(local)
            base.update({
                "vies_valid":        resp["valid"],
                "vies_name":         resp["name"],
                "vies_address":      resp["address"],
                "vies_request_date": resp["request_date"],
                "check_status":      "VALID" if resp["valid"] else "INVALID",
            })
            log.info(
                f"  [supplier:{entity_id}] {vat_raw} → {base['check_status']} (HMRC)"
                + (f" | {resp['name']}" if resp["name"] else "")
            )
        except requests.exceptions.HTTPError as e:
            base["check_status"]  = "ERROR"
            base["error_message"] = f"HMRC HTTP {e.response.status_code}: {e.response.text[:200]}"
            log.warning(f"  [supplier:{entity_id}] {vat_raw} → HMRC ERROR: {e}")
        except Exception as e:
            base["check_status"]  = "ERROR"
            base["error_message"] = str(e)
            log.warning(f"  [supplier:{entity_id}] {vat_raw} → ERROR HMRC: {e}")
        return base

    return base


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:

            # 0. Verifica check attivo nel catalogo
            cur.execute(
                "SELECT COALESCE(is_active, FALSE) FROM stg.check_catalog WHERE check_id = %s",
                (CHECK_ID,)
            )
            row = cur.fetchone()
            if not row or not row[0]:
                print(f"[SKIP] {CHECK_ID} disattivato nel catalogo")
                return

            # 1. Recupera run_id corrente
            cur.execute("SELECT MAX(run_id) FROM stg.pipeline_runs")
            run_id = cur.fetchone()[0]

            # 2. Crea tabella di dettaglio se non esiste
            cur.execute(DDL_CHECK_VAT_VIES)
            conn.commit()

            # 2b. Pulizia selettiva: rimuove solo i record vendor
            #     per il run corrente, lasciando intatti i record customer (CK801).
            cur.execute("""
                DELETE FROM stg.check_vat_vies
                WHERE entity_type = 'vendor'
                  AND run_id = %s
            """, (run_id,))
            deleted = cur.rowcount
            conn.commit()
            if deleted:
                log.info(f"Rimossi {deleted} record vendor precedenti per run_id={run_id}.")

            # 3. Leggi VAT supplier da raw
            cur.execute("""
                SELECT
                    "LIFNR(k/*)"     AS entity_id,
                    "TAXTYPE(k/*)" AS taxtype,
                    "TAXNUM(*)"    AS taxnum,
                    "_source"      AS zip_source
                FROM raw."S_SUPPL_TAXNUMBERS#ZBP_CodiciFisc"
                WHERE "TAXTYPE(k/*)" LIKE '%0'
                  AND "TAXNUM(*)" IS NOT NULL
                  AND TRIM("TAXNUM(*)") <> ''
            """)
            raw_rows = cur.fetchall()
            log.info(f"Record VAT supplier da elaborare: {len(raw_rows)}")

            # 4. Carica cache: per ogni (entity_id, vat_number) cerca l'ultimo
            #    risultato valido in stg.check_vat_vies (qualsiasi run precedente).
            #    Sono esclusi gli ERROR perché potrebbero essere transitori
            #    (es. SOAP Fault: MS_MAX_CONCURRENT_REQ) e vanno riverificati.
            #    Sono esclusi anche PENDING (non dovrebbero esserci ma per sicurezza).
            cur.execute("""
                SELECT DISTINCT ON (entity_id, vat_number)
                    entity_id,
                    vat_number,
                    check_status,
                    vies_valid,
                    vies_name,
                    vies_address,
                    vies_request_date,
                    country_code,
                    vat_local,
                    is_eu,
                    is_gb,
                    zip_source
                FROM stg.check_vat_vies
                WHERE entity_type = 'vendor'
                  AND check_status NOT IN ('ERROR', 'PENDING')
                ORDER BY entity_id, vat_number, created_at DESC
            """)
            cache = {
                (r[0], r[1]): r
                for r in cur.fetchall()
            }
            log.info(f"Cache VIES vendor: {len(cache)} risultati riusabili.")

        # 4b. Inizializza client esterni
        vies = ViesClient()
        hmrc = HmrcClient()

        # 5. Esegui check su ogni record
        results = []
        cached_count = 0
        for i, (entity_id, taxtype, taxnum, zip_source) in enumerate(raw_rows, 1):
            vat_normalized = normalize_taxnum(taxtype, taxnum)
            log.debug(f"[{i}/{len(raw_rows)}] supplier:{entity_id} — {vat_normalized}")

            # Controlla cache: (entity_id, vat_number) già verificato con successo?
            cache_key = (entity_id, vat_normalized)
            if cache_key in cache:
                cached = cache[cache_key]
                results.append({
                    "run_id":            run_id,
                    "check_id":          CHECK_ID,
                    "entity_type":       "vendor",
                    "entity_id":         entity_id,
                    "vat_number":        vat_normalized,
                    "country_code":      cached[7],
                    "vat_local":         cached[8],
                    "is_eu":             cached[9],
                    "is_gb":             cached[10],
                    "vies_valid":        cached[3],
                    "vies_name":         cached[4],
                    "vies_address":      cached[5],
                    "vies_request_date": cached[6],
                    "check_status":      "CACHED",
                    "error_message":     f"Risultato riusato da run precedente: {cached[2]}",
                    "zip_source":        zip_source,
                    "created_at":        datetime.now(timezone.utc),
                })
                cached_count += 1
                log.debug(f"  [supplier:{entity_id}] {vat_normalized} → CACHED ({cached[2]})")
                continue

            result = check_single(
                vies=vies,
                hmrc=hmrc,
                run_id=run_id,
                entity_id=entity_id,
                vat_raw=vat_normalized,
                zip_source=zip_source,
            )
            results.append(result)

            # Rate limiting: pausa dopo ogni chiamata reale
            if result["check_status"] not in ("NOT_EU", "GB_NO_CREDENTIALS", "CACHED"):
                time.sleep(INTER_CALL_DELAY)

        log.info(f"CACHED: {cached_count} | Da verificare: {len(raw_rows) - cached_count}")

        # 6. Scrivi risultati in stg.check_vat_vies
        if results:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO stg.check_vat_vies (
                        run_id, check_id, entity_type, entity_id, vat_number,
                        country_code, vat_local, is_eu, is_gb,
                        vies_valid, vies_name, vies_address, vies_request_date,
                        check_status, error_message, zip_source, created_at
                    ) VALUES %s
                    """,
                    [
                        (
                            r["run_id"], r["check_id"], r["entity_type"], r["entity_id"],
                            r["vat_number"], r["country_code"], r["vat_local"],
                            r["is_eu"], r["is_gb"], r["vies_valid"], r["vies_name"],
                            r["vies_address"], r["vies_request_date"], r["check_status"],
                            r["error_message"], r["zip_source"], r["created_at"],
                        )
                        for r in results
                    ],
                    page_size=200,
                )
            conn.commit()
            log.info(f"Scritti {len(results)} record in stg.check_vat_vies.")

        # 7. Riepilogo
        from collections import Counter
        counts = Counter(r["check_status"] for r in results)
        print(f"[OK] {CHECK_ID} completato — " + " | ".join(f"{s}:{n}" for s, n in sorted(counts.items())))

        invalids = [r for r in results if r["check_status"] == "INVALID"]
        if invalids:
            print(f"[WARN] {CHECK_ID} — {len(invalids)} P.IVA non valide:")
            for r in invalids:
                print(f"       supplier:{r['entity_id']} → {r['vat_number']}")

        gb_pending = [r for r in results if r["check_status"] == "GB_NO_CREDENTIALS"]
        if gb_pending:
            print(f"[WARN] {CHECK_ID} — {len(gb_pending)} VAT GB non verificate (credenziali HMRC mancanti)")

        cached = [r for r in results if r["check_status"] == "CACHED"]
        if cached:
            print(f"[INFO] {CHECK_ID} — {len(cached)} record riusati dalla cache (nessuna chiamata VIES/HMRC)")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
