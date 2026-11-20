"""
Ambiente SAP del run (DS4-200, QS4-200, PS4-100, ...): database PostgreSQL e cartelle del datalake.

L'ambiente arriva nella variabile MDG_ENV. Senza MDG_ENV (run da terminale o prima del
multi-ambiente) vale l'ambiente di default, con il database e le cartelle di sempre:
    database  POSTGRES_DB (mdg)
    cartelle  /project/datalake/<cartella>
Con un ambiente, ad esempio PS4-100:
    database  <POSTGRES_DB>_ps4_100 (mdg_ps4_100)
    cartelle  /project/datalake/<cartella>/PS4-100

Gli asset Python leggono connessione e percorsi da qui invece di scriverli nel codice:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from mdg_env import DB_CONFIG, OUT_MDG
Non è un asset Bruin.
"""
import os
import re

ENV_VAR      = "MDG_ENV"
DEFAULT_ENV  = "DEFAULT"
DATALAKE     = "/project/datalake"
CONFIG_DIR   = "/project/bruin/config"
# Identificativo di un ambiente: sistema SAP + mandante, es. DS4-200
ENV_RE       = re.compile(r"^[A-Z0-9]{2,10}-\d{1,4}$")


def current_env() -> str:
    """Ambiente del run (MDG_ENV in maiuscolo); DEFAULT se non indicato."""
    env = (os.environ.get(ENV_VAR) or "").strip().upper()
    if not env or env == DEFAULT_ENV:
        return DEFAULT_ENV
    if not ENV_RE.match(env):
        raise ValueError(f"{ENV_VAR}='{env}' non valido: atteso sistema-mandante, es. DS4-200")
    return env


MDG_ENV    = current_env()
IS_DEFAULT = MDG_ENV == DEFAULT_ENV


def db_name(env: str = MDG_ENV) -> str:
    """Database PostgreSQL dell'ambiente: POSTGRES_DB per il default, <POSTGRES_DB>_<ambiente> per gli altri."""
    base = os.environ.get("POSTGRES_DB", "mdg")
    return base if env == DEFAULT_ENV else f"{base}_{env.lower().replace('-', '_')}"


DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   db_name(),
    "user":     os.environ.get("POSTGRES_USER", "mdg_user"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


def env_folder(name: str, env: str = MDG_ENV) -> str:
    """Cartella del datalake per l'ambiente: /project/datalake/<name>[/<ambiente>]."""
    root = os.path.join(DATALAKE, name)
    return root if env == DEFAULT_ENV else os.path.join(root, env)


IN_PPROD  = env_folder("in_source_pprod")    # ZIP/CSV dell'ERP
IN_SAP    = env_folder("in_source_sap")      # tabelle di controllo SAP (XLSX)
IN_OTHERS = env_folder("in_source_others")   # altri XLSX (tabelle STG da file)
OUT_MDG   = env_folder("out_source_mdg")     # output di MDG

# Stato dei controlli (attivo, "Escludi dall'output") scritto dalla pagina Check Catalog
CHECK_STATES_FILE = os.path.join(
    CONFIG_DIR, "check_states.json" if IS_DEFAULT else f"check_states_{MDG_ENV}.json")
