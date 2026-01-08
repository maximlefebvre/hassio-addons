# Utilise l'image Python officielle de Home Assistant (plus légère et adaptée)
ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.19
FROM $BUILD_FROM

# Installation des dépendances Python (requests)
RUN pip3 install requests

# Copie des fichiers
COPY sync_kdrive.py /sync_kdrive.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

# Commande de lancement
CMD [ "/run.sh" ]