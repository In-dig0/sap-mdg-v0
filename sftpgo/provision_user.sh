#!/bin/bash
# =============================================================================
# sftpgo/provision_user.sh
# =============================================================================
# Crea l'utente mdg_sftp su SFTPGo via REST API al primo avvio.
# Eseguire DOPO che il container SFTPGo è up e l'admin è stato creato.
#
# Uso:
#   chmod +x sftpgo/provision_user.sh
#   ./sftpgo/provision_user.sh
#
# Prerequisiti nel .env:
#   SFTPGO_ADMIN_USER, SFTPGO_ADMIN_PASSWORD
#   SFTPGO_MDG_USER, SFTPGO_MDG_PASSWORD
#   SFTPGO_MDG_PUBKEY  (contenuto della chiave pubblica SSH, opzionale)
#   SFTPGO_ADMIN_PORT
# =============================================================================

set -e

# Verifica che lo script venga eseguito dalla root del progetto
if [ ! -f ".env" ]; then
    echo "ERRORE: file .env non trovato."
    echo "Eseguire lo script dalla root del progetto:"
    echo "  bash sftpgo/provision_user.sh"
    exit 1
fi

# Carica .env se presente
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
fi

ADMIN_USER="${SFTPGO_ADMIN_USER:-admin}"
ADMIN_PASS="${SFTPGO_ADMIN_PASSWORD:-changeme}"
MDG_USER="${SFTPGO_MDG_USER:-mdg_sftp}"
MDG_PASS="${SFTPGO_MDG_PASSWORD}"
MDG_PUBKEY="${SFTPGO_MDG_PUBKEY:-}"
ADMIN_PORT="${SFTPGO_ADMIN_PORT:-8080}"
BASE_URL="http://localhost:${ADMIN_PORT}/api/v2"

echo "=================================================="
echo "SFTPGo — Provisioning utente ${MDG_USER}"
echo "=================================================="

# Attende che SFTPGo sia raggiungibile (max 30s)
echo "Attesa avvio SFTPGo..."
for i in $(seq 1 30); do
  if curl -sf "${BASE_URL}/token" \
       -u "${ADMIN_USER}:${ADMIN_PASS}" \
       -o /dev/null 2>/dev/null; then
    echo "SFTPGo raggiungibile."
    break
  fi
  if [ $i -eq 30 ]; then
    echo "ERRORE: SFTPGo non risponde dopo 30 secondi. Verificare il container."
    exit 1
  fi
  sleep 1
done

# Ottiene token JWT
echo "Autenticazione admin..."
TOKEN=$(curl -sf "${BASE_URL}/token" \
  -u "${ADMIN_USER}:${ADMIN_PASS}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
  echo "ERRORE: impossibile ottenere token JWT. Verificare credenziali admin."
  exit 1
fi

echo "Token ottenuto."

# Costruisce il payload JSON per l'utente mdg_sftp
# Permessi:
#   /                → list, download (solo navigazione root)
#   /in_source_pprod → tutto (upload ZIP dall'ERP)
#   /in_source_sap   → tutto (upload XLSX tabelle SAP)
#   /in_source_others→ tutto (upload XLSX altre sorgenti)
#   /out_source_mdg  → list, download (output pipeline MDG, sola lettura)

PUBLIC_KEYS_JSON="[]"
if [ -n "$MDG_PUBKEY" ]; then
  PUBLIC_KEYS_JSON="[\"${MDG_PUBKEY}\"]"
fi

PAYLOAD=$(cat <<EOF
{
  "status": 1,
  "username": "${MDG_USER}",
  "password": "${MDG_PASS}",
  "public_keys": ${PUBLIC_KEYS_JSON},
  "home_dir": "/datalake",
  "uid": 1000,
  "gid": 1000,
  "max_sessions": 5,
  "quota_size": 0,
  "quota_files": 0,
  "permissions": {
    "/":                ["list", "download"],
    "/in_source_pprod": ["*"],
    "/in_source_sap":   ["*"],
    "/in_source_others":["*"],
    "/out_source_mdg":  ["list", "download"]
  },
  "upload_bandwidth": 0,
  "download_bandwidth": 0,
  "filters": {
    "denied_login_methods": [],
    "file_patterns": []
  },
  "filesystem": { "provider": 0 }
}
EOF
)

# Verifica se l'utente esiste già
HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
  "${BASE_URL}/users/${MDG_USER}" \
  -H "Authorization: Bearer ${TOKEN}" 2>/dev/null || echo "404")

if [ "$HTTP_CODE" = "200" ]; then
  echo "Utente ${MDG_USER} già esistente — aggiornamento..."
  RESULT=$(curl -sf -X PUT "${BASE_URL}/users/${MDG_USER}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${PAYLOAD}")
  echo "Utente aggiornato."
else
  echo "Creazione utente ${MDG_USER}..."
  RESULT=$(curl -sf -X POST "${BASE_URL}/users" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${PAYLOAD}")
  echo "Utente creato."
fi

echo ""
echo "=================================================="
echo "Provisioning completato!"
echo "  SFTP user : ${MDG_USER}"
echo "  Home dir  : /datalake"
echo "  Cartelle  : in_source_pprod, in_source_sap,"
echo "              in_source_others, out_source_mdg"
echo "  WebAdmin  : http://localhost:${ADMIN_PORT}"
echo "=================================================="
