#!/bin/sh
# Avvia la pipeline MDG per un ambiente SAP (es. DS4-200), dentro il container mdg_bruin:
#   docker exec mdg_bruin sh /project/bruin/run_env.sh DS4-200
# Senza ambiente (o DEFAULT): run di sempre, database POSTGRES_DB e cartelle principali.
# Con un ambiente: genera un file di connessione temporaneo (leggibile solo da root, cancellato
# a fine run) con la connessione mdg_postgres verso il database dell'ambiente
# (<POSTGRES_DB>_<ambiente>, es. mdg_ds4_200) e passa MDG_ENV agli asset Python (mdg_env.py).
# Le credenziali sono quelle già presenti nell'ambiente del container: .bruin.yml non è toccato.
set -e

PIPELINE=/project/bruin
ENV_ID=$(printf '%s' "${1:-DEFAULT}" | tr '[:lower:]' '[:upper:]')

if [ "$ENV_ID" = "DEFAULT" ]; then
    echo "[run_env] Ambiente DEFAULT — database ${POSTGRES_DB:-mdg}"
    exec bruin run "$PIPELINE"
fi

if ! printf '%s' "$ENV_ID" | grep -Eq '^[A-Z0-9]{2,10}-[0-9]{1,4}$'; then
    echo "[run_env] Ambiente non valido: '$ENV_ID' (atteso sistema-mandante, es. DS4-200)" >&2
    exit 2
fi

DB_NAME="${POSTGRES_DB:-mdg}_$(printf '%s' "$ENV_ID" | tr 'A-Z-' 'a-z_')"
CFG=$(mktemp /tmp/bruin_env_XXXXXX.yml)   # permessi 0600
trap 'rm -f "$CFG"' EXIT INT TERM

# JSON (YAML valido): i valori, password compresa, sono scritti con l'escape corretto
DB_NAME="$DB_NAME" python3 - "$CFG" <<'EOF'
import json, os, sys
conn = {
    "name": "mdg_postgres",
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "username": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "database": os.environ["DB_NAME"],
    "ssl_mode": "disable",
}
cfg = {"default_environment": "default",
       "environments": {"default": {"connections": {"postgres": [conn]}}}}
with open(sys.argv[1], "w") as f:
    json.dump(cfg, f)
EOF

echo "[run_env] Ambiente $ENV_ID — database $DB_NAME"
MDG_ENV="$ENV_ID" bruin run --config-file "$CFG" "$PIPELINE"
