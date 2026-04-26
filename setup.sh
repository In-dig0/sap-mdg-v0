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
    cp .env.example .env 2>/dev/null || true
    echo "✅ File .env creato — personalizza le password prima di procedere!"
else
    echo "ℹ️  File .env già presente, non sovrascritto."
fi

# 2. Genera chiavi SSH host per il container SFTP (SFTPGo)
mkdir -p sftpgo/keys
if [ ! -f sftpgo/keys/ssh_host_ed25519_key ]; then
    ssh-keygen -t ed25519 -f sftpgo/keys/ssh_host_ed25519_key -N "" -q
    chmod 600 sftpgo/keys/ssh_host_ed25519_key
    echo "✅ Chiave SSH ed25519 generata in sftpgo/keys/"
else
    echo "ℹ️  Chiave SSH ed25519 già presente."
fi

if [ ! -f sftpgo/keys/ssh_host_rsa_key ]; then
    ssh-keygen -t rsa -b 4096 -f sftpgo/keys/ssh_host_rsa_key -N "" -q
    chmod 600 sftpgo/keys/ssh_host_rsa_key
    echo "✅ Chiave SSH RSA generata in sftpgo/keys/"
else
    echo "ℹ️  Chiave SSH RSA già presente."
fi

# 3. Rende eseguibile lo script di provisioning utente SFTPGo
chmod +x sftpgo/provision_user.sh
echo "✅ sftpgo/provision_user.sh pronto."

# 4. Crea le directory necessarie
mkdir -p pgadmin streamlit/app logs
echo "✅ Directory struttura progetto verificate."

echo ""
echo "================================="
echo "✅ Setup completato!"
echo ""
echo "Prossimi passi:"
echo "  1. Modifica le password in .env"
echo "  2. docker compose up -d"
echo "  3. Crea l'utente SFTP (solo al primo avvio):"
echo "     ./sftpgo/provision_user.sh"
echo "  4. Verifica i servizi:"
echo "     • PgAdmin:        http://localhost:8080"
echo "     • Streamlit:      http://localhost:8501"
echo "     • SFTPGo WebAdmin:http://localhost:8082"
echo "     • SFTP client:    sftp -P 22 mdg_sftp@localhost"
echo ""
