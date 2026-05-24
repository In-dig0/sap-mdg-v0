#!/bin/bash
# =============================================================================
# MDG - deploy.sh | Script di deploy per produzione OCI
#
# Uso:
#   chmod +x deploy.sh
#   ./deploy.sh          # pull + up
#   ./deploy.sh --build  # pull + rebuild immagini + up
# =============================================================================

set -e

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
LOG_FILE="deploy.log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

log() {
  echo "$1"
  echo "[$TIMESTAMP] $1" >> "$LOG_FILE"
}

on_error() {
  local exit_code=$?
  local line=$1
  log "❌ Errore (exit code $exit_code) alla linea $line — deploy interrotto."
  echo "---" >> "$LOG_FILE"
}

trap 'on_error $LINENO' ERR

log "---"
log "🚀 MDG Deploy — $TIMESTAMP"

# Pull ultima versione dal repo
log "📥 Git pull..."
git pull origin main

# Pull immagini pre-built (postgres, pgadmin, sftp, ecc.)
log "🐳 Pull immagini Docker..."
$COMPOSE pull

# Build opzionale
case "$1" in
  --build)
    log "🔨 Build immagini custom..."
    $COMPOSE build --pull
    ;;
esac

# Up
log "🔄 Restart stack..."
$COMPOSE up -d

log "✅ Deploy completato con successo."
echo "---" >> "$LOG_FILE"
$COMPOSE ps