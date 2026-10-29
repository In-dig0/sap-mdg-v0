# Immagine Bruin custom con Python e dipendenze della pipeline MDG
# Compatibile con linux/amd64 e linux/arm64 (OCI Ampere A1)
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates tar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Versione Bruin fissata (quella su cui è testata la pipeline): con "latest" locale e VPS
# finivano con versioni diverse. Per aggiornare: cambiare BRUIN_VERSION e ricostruire l'immagine.
ARG BRUIN_VERSION=v0.11.782

# Installa il binario Bruin — rileva automaticamente l'architettura
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then BRUIN_ARCH="arm64"; else BRUIN_ARCH="x86_64"; fi && \
    curl -L "https://github.com/bruin-data/bruin/releases/download/${BRUIN_VERSION}/bruin_Linux_${BRUIN_ARCH}.tar.gz" \
    -o /tmp/bruin.tar.gz && \
    tar -xzf /tmp/bruin.tar.gz -C /usr/local/bin bruin && \
    chmod +x /usr/local/bin/bruin && \
    rm /tmp/bruin.tar.gz

COPY requirements_bruin.txt /tmp/requirements_bruin.txt
RUN pip install --no-cache-dir -r /tmp/requirements_bruin.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /pipelines
ENTRYPOINT ["/entrypoint.sh"]
