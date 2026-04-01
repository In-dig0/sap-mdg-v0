#!/bin/sh
# entrypoint.sh MDG
# In v0: mantiene il container attivo per run manuali via docker exec
# In produzione: riceve il nome dell'asset come argomento

set -e

LOG_FILE="/pipelines/logs/bruin_pipeline.log"
mkdir -p /pipelines/logs

TIMESTAMP=$(date '+%Y-%m-%dT%H:%M:%S')
echo "[$TIMESTAMP] Container MDG Bruin avviato." >> "$LOG_FILE"

if [ $# -eq 0 ]; then
    echo "[$TIMESTAMP] Modalità attesa — run manuale con:" >> "$LOG_FILE"
    echo "[$TIMESTAMP]   docker exec -it mdg_bruin bruin run /pipelines/assets/ingestion" >> "$LOG_FILE"
    tail -f /dev/null
else
    echo "[$TIMESTAMP] Avvio: bruin $@" >> "$LOG_FILE"
    bruin "$@" >> "$LOG_FILE" 2>&1
    EXIT_CODE=$?
    TIMESTAMP=$(date '+%Y-%m-%dT%H:%M:%S')
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$TIMESTAMP] Completato con successo." >> "$LOG_FILE"
    else
        echo "[$TIMESTAMP] Fallito (exit code: $EXIT_CODE)." >> "$LOG_FILE"
    fi
    exit $EXIT_CODE
fi
