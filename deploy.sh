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

echo "🚀 MDG Deploy — $(date)"

# Pull ultima versione dal repo
echo "📥 Git pull..."
git pull origin main

# Pull immagini pre-built (postgres, pgadmin, sftp, ecc.)
echo "🐳 Pull immagini Docker..."
$COMPOSE pull

# Build opzionale
case "$1" in
  --build)
    echo "🔨 Build immagini custom..."
    $COMPOSE build --pull
    ;;
esac

# Up
echo "🔄 Restart stack..."
$COMPOSE up -d

echo "✅ Deploy completato."
$COMPOSE ps