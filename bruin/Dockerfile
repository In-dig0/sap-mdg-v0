# Immagine Bruin custom con Python e dipendenze della pipeline MDG
FROM ghcr.io/bruin-data/bruin:latest

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements_bruin.txt /tmp/requirements_bruin.txt
RUN pip3 install --break-system-packages -r /tmp/requirements_bruin.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /pipelines
ENTRYPOINT ["/entrypoint.sh"]
