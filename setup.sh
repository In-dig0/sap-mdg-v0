#!/usr/bin/env bash
# =============================================================================
# MDG v0 — setup.sh
# Prepara l'ambiente locale prima del primo docker compose up.
# Eseguire una sola volta: bash setup.sh
# =============================================================================
set -euo pipefail

echo "🔧 MDG v0 — Setup iniziale"
echo "================================="

# 1. Configura Git per non convertire i fine riga (critico su Windows)
#    Evita che i file .sh vengano salvati con CRLF e non funzionino su Linux
git config core.autocrlf false
echo "✅ Git configurato per LF (core.autocrlf=false)"

# 2. Normalizza i fine riga di tutti gli script .sh già presenti nel repo
#    (necessario se il clone è avvenuto prima di questa impostazione)
if command -v sed &>/dev/null; then
    find . -name "*.sh" -not -path "./.git/*" -exec sed -i 's/\r//' {} \;
    echo "✅ Fine riga normalizzati (CRLF → LF) su tutti i file .sh"
fi

# 3. Copia .env se non esiste
if [ ! -f .env ]; then
    cp .env.example .env 2>/dev/null || true
    echo "✅ File .env creato — personalizza le password prima di procedere!"
else
    echo "ℹ️  File .env già presente, non sovrascritto."
fi

# 4. Genera chiavi SSH host per il container SFTP (SFTPGo)
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

# 5. Rende eseguibile lo script di provisioning utente SFTPGo
chmod +x sftpgo/provision_user.sh
echo "✅ sftpgo/provision_user.sh pronto."

# 6. Crea le directory necessarie
mkdir -p pgadmin streamlit/app logs
echo "✅ Directory struttura progetto verificate."

# 7. Genera .bruin.yml dal .env
#    Bruin non espande variabili d'ambiente — questo step sostituisce i
#    placeholder con i valori reali letti dal .env.
if [ -f ".env" ]; then
    # Carica le variabili del .env (ignora righe vuote e commenti)
    set +u  # disabilita temporaneamente il controllo variabili non definite
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
    set -u

    cat > .bruin.yml << EOF
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: "mdg_postgres"
          username: "${POSTGRES_USER}"
          password: "${POSTGRES_PASSWORD}"
          host: "${POSTGRES_HOST}"
          port: ${POSTGRES_PORT}
          database: "${POSTGRES_DB}"
          ssl_mode: "disable"
          schema: "raw"
EOF
    echo "✅ .bruin.yml generato dal .env"
else
    echo "⚠️  .env non trovato — .bruin.yml non generato. Esegui setup.sh dopo aver creato .env."
fi

echo ""
echo "================================="
echo "✅ Setup completato!"
echo ""
echo "Prossimi passi:"
echo "  1. Modifica le password in .env"
echo "     (vedi .env.example per la struttura)"
echo "  2. Avvia lo stack:"
echo "     docker compose up -d"
echo "  3. Crea l'utente SFTP (solo al primo avvio):"
echo "     bash sftpgo/provision_user.sh"
echo "  4. Verifica i servizi (porte da .env):"
echo "     • PgAdmin:         http://localhost:<PGADMIN_PORT>"
echo "     • Streamlit:       http://localhost:<STREAMLIT_PORT>"
echo "     • SFTPGo WebAdmin: http://localhost:<SFTPGO_ADMIN_PORT>"
echo "     • SFTP client:     sftp -P <SFTP_PORT> mdg_sftp@localhost"
echo ""
