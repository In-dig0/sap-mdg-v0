/* @bruin
name: setup.fn_fornitori_cli
type: pg.sql
depends:
  - setup.init_db
description: >
  Funzioni per i clienti già in SAP estesi al ruolo fornitore (flusso DM06-05a,
  raw."S_SUPPL_GEN#AddDatiGenerali", cliente di riferimento in REF_KUNNR).
  Unica fonte delle regole, usate dai check CK207/CK418/CK419 e da prd.export_suppliers_zip (CK418):
    - stg.fn_fornitori_da_clienti(): coppie fornitore (LIFNR) -> cliente (REF_KUNNR);
    - stg.fn_norm_taxnum(taxtype, taxnum): identificativo fiscale normalizzato come in
      CK207/CK801/CK803 (maiuscolo, solo lettere e cifre, prefisso paese del TAXTYPE);
    - stg.fn_iban_uguale_cliente(lifnr, iban): l'IBAN del fornitore è tra quelli del
      cliente di riferimento in prd."S_CUST_BANK_DATA#ZBP-AppoggioBanca";
    - stg.fn_taxnum_uguale_cliente(lifnr, taxtype, taxnum): il cliente di riferimento ha
      lo stesso TAXTYPE con lo stesso identificativo in prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc".
  Le tabelle sono lette con SQL dinamico: se non sono caricate le funzioni non restituiscono
  righe (o FALSE), senza errori.
connection: mdg_postgres
@bruin */

CREATE OR REPLACE FUNCTION stg.fn_fornitori_da_clienti()
RETURNS TABLE (lifnr text, kunnr text)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    IF to_regclass('raw."S_SUPPL_GEN#AddDatiGenerali"') IS NULL THEN
        RETURN;
    END IF;
    RETURN QUERY EXECUTE
        'SELECT DISTINCT TRIM("LIFNR(k/*)")::text, TRIM("REF_KUNNR")::text
         FROM raw."S_SUPPL_GEN#AddDatiGenerali"
         WHERE length(TRIM("REF_KUNNR")) > 0';
END;
$$;

CREATE OR REPLACE FUNCTION stg.fn_norm_taxnum(p_taxtype text, p_taxnum text)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT CASE
               WHEN n = '' THEN NULL
               WHEN n LIKE UPPER(LEFT(COALESCE(p_taxtype, ''), 2)) || '%' THEN n
               ELSE UPPER(LEFT(COALESCE(p_taxtype, ''), 2)) || n
           END
    FROM (SELECT regexp_replace(UPPER(COALESCE(p_taxnum, '')), '[^A-Z0-9]', '', 'g') AS n) x
$$;

CREATE OR REPLACE FUNCTION stg.fn_iban_uguale_cliente(p_lifnr text, p_iban text)
RETURNS boolean
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_iban  text := regexp_replace(UPPER(COALESCE(p_iban, '')), '[^A-Z0-9]', '', 'g');
    v_found boolean;
BEGIN
    IF v_iban = '' OR to_regclass('prd."S_CUST_BANK_DATA#ZBP-AppoggioBanca"') IS NULL THEN
        RETURN FALSE;
    END IF;
    EXECUTE
        'SELECT EXISTS (
             SELECT 1
             FROM stg.fn_fornitori_da_clienti() fc
             JOIN prd."S_CUST_BANK_DATA#ZBP-AppoggioBanca" c ON TRIM(c."KUNNR(k/*)") = fc.kunnr
             WHERE fc.lifnr = TRIM($1)
               AND regexp_replace(UPPER(c."IBAN(k)"), ''[^A-Z0-9]'', '''', ''g'') = $2)'
        INTO v_found
        USING p_lifnr, v_iban;
    RETURN v_found;
END;
$$;

CREATE OR REPLACE FUNCTION stg.fn_taxnum_uguale_cliente(p_lifnr text, p_taxtype text, p_taxnum text)
RETURNS boolean
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_num   text := stg.fn_norm_taxnum(p_taxtype, p_taxnum);
    v_found boolean;
BEGIN
    IF v_num IS NULL OR COALESCE(TRIM(p_taxtype), '') = ''
       OR to_regclass('prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc"') IS NULL THEN
        RETURN FALSE;
    END IF;
    EXECUTE
        'SELECT EXISTS (
             SELECT 1
             FROM stg.fn_fornitori_da_clienti() fc
             JOIN prd."S_CUST_TAXNUMBERS#ZBP-CodiciFisc" c ON TRIM(c."KUNNR(k/*)") = fc.kunnr
             WHERE fc.lifnr = TRIM($1)
               AND c."TAXTYPE(k/*)" = TRIM($2)
               AND stg.fn_norm_taxnum(c."TAXTYPE(k/*)", c."TAXNUM(*)") = $3)'
        INTO v_found
        USING p_lifnr, p_taxtype, v_num;
    RETURN v_found;
END;
$$;
