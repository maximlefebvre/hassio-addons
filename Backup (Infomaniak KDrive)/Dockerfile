# Utilisation de l'image de base Home Assistant (choisissez l'architecture adaptée)
# Pour Raspberry Pi 4/5, utilisez : ghcr.io/home-assistant/aarch64-base:latest
# Pour PC/Intel/NUC, utilisez : ghcr.io/home-assistant/amd64-base:latest
ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.19
FROM $BUILD_FROM

# Installation des dépendances selon l'image de base HA (Alpine)
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-requests

# Copie du script Python
COPY sync_kdrive.py /sync_kdrive.py

# Copie du script de lancement
COPY run.sh /
RUN chmod a+x /run.sh

# Dans les images HA, on utilise CMD pour lancer le script via s6
CMD [ "/run.sh" ]