/* @bruin
name: setup.fn_banca_estera
type: pg.sql
depends:
  - setup.init_db
description: >
  Crea la funzione stg.fn_banca_estera(banks, bankl, iban, swift) che determina
  la chiave banca SAP (BANKL) di una coordinata bancaria ESTERA (BANKS <> 'IT')
  sull'anagrafica ref."SAP_EXPORT_BNKA_UE" (banche non cancellate, LOEVM <> 'X').
  Unica fonte della regola, usata dai check CK055/CK056 e dal merge delle
  coordinate bancarie in prd:
    1. BANKL valorizzato → confronto esatto (zeri iniziali significativi);
    2. BANKL vuoto → codice banca dall'IBAN: BANKL di BNKA (stesso paese) uguale
       all'inizio del BBAN (IBAN dalla 5a posizione), confronto esatto;
       con più corrispondenze vince la più lunga;
    3. altrimenti SWIFT (dall'ERP nel campo BKREF), stesso paese: prima SWIFT
       identico; se non trovato, 8 e 11 caratteri equivalenti
       (XXXXXXXX = XXXXXXXXXXX). Valido solo se individua una sola chiave banca,
       altrimenti SWIFT ambiguo.
  Esiti: OK, NON_TROVATA, CANCELLATA, SWIFT_AMBIGUO, DATI_MANCANTI.
connection: mdg_postgres
@bruin */

CREATE OR REPLACE FUNCTION stg.fn_banca_estera(
    p_banks text, p_bankl text, p_iban text, p_swift text
)
RETURNS TABLE (bankl text, metodo text, esito text, dettaglio text)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_bankl  text := COALESCE(TRIM(p_bankl), '');
    v_iban   text := UPPER(REPLACE(COALESCE(p_iban, ''), ' ', ''));
    v_swift  text := UPPER(REPLACE(COALESCE(p_swift, ''), ' ', ''));
    v_key    text;
    v_n      integer;
    v_chiavi text;
BEGIN
    -- 1. BANKL indicato dall'ERP: confronto esatto
    IF v_bankl <> '' THEN
        IF EXISTS (SELECT 1 FROM ref."SAP_EXPORT_BNKA_UE" b
                   WHERE b."BANKS" = p_banks AND b."BANKL" = v_bankl
                     AND COALESCE(b."LOEVM", '') <> 'X') THEN
            RETURN QUERY SELECT v_bankl, 'BANKL'::text, 'OK'::text, NULL::text;
        ELSIF EXISTS (SELECT 1 FROM ref."SAP_EXPORT_BNKA_UE" b
                      WHERE b."BANKS" = p_banks AND b."BANKL" = v_bankl) THEN
            RETURN QUERY SELECT NULL::text, 'BANKL'::text, 'CANCELLATA'::text,
                                ('chiave ' || v_bankl)::text;
        ELSE
            RETURN QUERY SELECT NULL::text, 'BANKL'::text, 'NON_TROVATA'::text,
                                ('chiave ' || v_bankl)::text;
        END IF;
        RETURN;
    END IF;

    -- 2. Codice banca all'inizio del BBAN (IBAN dalla 5a posizione)
    IF v_iban <> '' THEN
        SELECT b."BANKL" INTO v_key
        FROM ref."SAP_EXPORT_BNKA_UE" b
        WHERE b."BANKS" = p_banks
          AND COALESCE(b."BANKL", '') <> ''
          AND COALESCE(b."LOEVM", '') <> 'X'
          AND SUBSTRING(v_iban FROM 5 FOR LENGTH(b."BANKL")) = UPPER(b."BANKL")
        ORDER BY LENGTH(b."BANKL") DESC
        LIMIT 1;
        IF v_key IS NOT NULL THEN
            RETURN QUERY SELECT v_key, 'IBAN'::text, 'OK'::text, ('IBAN ' || v_iban)::text;
            RETURN;
        END IF;
    END IF;

    -- 3. SWIFT, solo se individua una chiave sola: prima identico, poi con
    --    8 e 11 caratteri equivalenti (in BNKA la stessa banca può comparire
    --    sia come XXXXXXXX sia come XXXXXXXXXXX)
    IF v_swift <> '' THEN
        SELECT COUNT(DISTINCT b."BANKL"), MIN(b."BANKL"),
               STRING_AGG(DISTINCT b."BANKL", ', ')
          INTO v_n, v_key, v_chiavi
        FROM ref."SAP_EXPORT_BNKA_UE" b
        WHERE b."BANKS" = p_banks
          AND COALESCE(b."BANKL", '') <> ''
          AND COALESCE(b."LOEVM", '') <> 'X'
          AND UPPER(TRIM(b."SWIFT")) = v_swift;
        IF v_n = 0 THEN
            SELECT COUNT(DISTINCT b."BANKL"), MIN(b."BANKL"),
                   STRING_AGG(DISTINCT b."BANKL", ', ')
              INTO v_n, v_key, v_chiavi
            FROM ref."SAP_EXPORT_BNKA_UE" b
            WHERE b."BANKS" = p_banks
              AND COALESCE(b."BANKL", '') <> ''
              AND COALESCE(b."LOEVM", '') <> 'X'
              AND (CASE WHEN LENGTH(UPPER(TRIM(b."SWIFT"))) = 8 THEN UPPER(TRIM(b."SWIFT")) || 'XXX'
                        ELSE UPPER(TRIM(b."SWIFT")) END)
                = (CASE WHEN LENGTH(v_swift) = 8 THEN v_swift || 'XXX' ELSE v_swift END);
        END IF;
        IF v_n = 1 THEN
            RETURN QUERY SELECT v_key, 'SWIFT'::text, 'OK'::text, ('SWIFT ' || v_swift)::text;
            RETURN;
        ELSIF v_n > 1 THEN
            RETURN QUERY SELECT NULL::text, 'SWIFT'::text, 'SWIFT_AMBIGUO'::text,
                                ('SWIFT ' || v_swift || ' → ' || v_n || ' chiavi: ' || v_chiavi)::text;
            RETURN;
        END IF;
    END IF;

    IF v_iban = '' AND v_swift = '' THEN
        RETURN QUERY SELECT NULL::text, NULL::text, 'DATI_MANCANTI'::text, NULL::text;
    ELSE
        RETURN QUERY SELECT NULL::text, NULL::text, 'NON_TROVATA'::text,
                            TRIM(BOTH ' ' FROM
                                 CASE WHEN v_iban  <> '' THEN 'IBAN '  || v_iban  ELSE '' END || ' ' ||
                                 CASE WHEN v_swift <> '' THEN 'SWIFT ' || v_swift ELSE '' END)::text;
    END IF;
END;
$$;

COMMENT ON FUNCTION stg.fn_banca_estera(text, text, text, text) IS
    'Chiave banca SAP per coordinate bancarie estere (BANKL, IBAN, SWIFT su ref.SAP_EXPORT_BNKA_UE). Usata da CK055/CK056 e merge banche prd.';
