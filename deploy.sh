#!/bin/bash
# =============================================================================
# MDG - deploy.sh | Script di deploy per produzione OCI
#
# Uso:
#   chmod +x deploy.sh
#   ./deploy.sh          # pull + up
#   ./deploy.sh --build  # pull + rebuild immagini + up
# =============================================================================

set -e  # Interrompi in caso di errore

echo "🚀 MDG Deploy — $(date)"

# Pull ultima versione dal repo
echo "📥 Git pull..."
git pull origin main

# Build opzionale
if [ "$1" == "--build" ]; then
  echo "🔨 Build immagini..."
  docker compose -f docker-compose.yml -f docker-compose.prod.yml build
fi

# Down + Up
echo "🔄 Restart stack..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

echo "✅ Deploy completato."
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
