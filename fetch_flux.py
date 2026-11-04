"""JOB 1 — Télécharge une seule fois les flux Lengow partagés par les autres jobs.

- flux canal Google (offer_id, GTIN) → stocks + conversationnel
- catalogue Lengow (champs produit)  → descriptions + conversationnel
"""

import logging

import config
from common.flux import telecharger

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

if __name__ == '__main__':
    telecharger(config.URL_FLUX_GMC, config.FICHIER_FLUX_GMC)
    telecharger(config.URL_CATALOGUE, config.FICHIER_CATALOGUE)
