#!/usr/bin/env bash
# =============================================================================
# MDG v0 — setup.sh
# Prepara l'ambiente locale prima del primo docker compose up.
# Eseguire una sola volta: bash setup.sh
# =============================================================================
set -euo pipefail

echo "🔧 MDG v0 — Setup iniziale"
echo "================================="

# 1. Copia .env se non esiste
if [ ! -f .env ]; then
    cp .env .env.backup 2>/dev/null || true
    cp .env .env
    echo "✅ File .env creato — personalizza le password prima di procedere!"
else
    echo "ℹ️  File .env già presente, non sovrascritto."
fi

# 2. Genera chiavi SSH host per il container SFTP
mkdir -p sftp
if [ ! -f sftp/ssh_host_ed25519_key ]; then
    ssh-keygen -t ed25519 -f sftp/ssh_host_ed25519_key -N "" -q
    chmod 600 sftp/ssh_host_ed25519_key
    echo "✅ Chiave SSH ed25519 generata in sftp/"
else
    echo "ℹ️  Chiave SSH ed25519 già presente."
fi

if [ ! -f sftp/ssh_host_rsa_key ]; then
    ssh-keygen -t rsa -b 4096 -f sftp/ssh_host_rsa_key -N "" -q
    chmod 600 sftp/ssh_host_rsa_key
    echo "✅ Chiave SSH RSA generata in sftp/"
else
    echo "ℹ️  Chiave SSH RSA già presente."
fi

# 3. Crea le directory necessarie
mkdir -p db/init pgadmin pipelines streamlit/app
echo "✅ Directory struttura progetto verificate."

echo ""
echo "================================="
echo "✅ Setup completato!"
echo ""
echo "Prossimi passi:"
echo "  1. Modifica le password in .env"
echo "  2. docker compose up -d"
echo "  3. Verifica i servizi:"
echo "     • PgAdmin:   http://localhost:8080"
echo "     • Streamlit: http://localhost:8501"
echo "     • SFTP:      sftp -P 22 mdg_erp@localhost"
echo ""
