# Immagine Bruin custom con Python e dipendenze della pipeline MDG
# Compatibile con linux/amd64 e linux/arm64 (OCI Ampere A1)
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates tar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installa il binario Bruin — rileva automaticamente l'architettura
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then BRUIN_ARCH="arm64"; else BRUIN_ARCH="x86_64"; fi && \
    curl -L "https://github.com/bruin-data/bruin/releases/latest/download/bruin_Linux_${BRUIN_ARCH}.tar.gz" \
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
