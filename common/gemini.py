"""Client minimal de l'API Gemini, prévu pour le niveau gratuit :
temporisation entre appels, plafond par exécution, arrêt propre si quota atteint."""

import json
import logging
import os
import time

import requests

import config

log = logging.getLogger(__name__)
API = 'https://generativelanguage.googleapis.com/v1beta/models/{modele}:generateContent'


class QuotaAtteint(Exception):
    pass


class Gemini:
    def __init__(self, plafond):
        self.cle = os.environ.get('GEMINI_API_KEY', '').strip()
        self.plafond = plafond
        self.appels = 0
        self.dernier = 0.0
        self.quota_atteint = False
        if not self.cle:
            log.info("Pas de GEMINI_API_KEY : génération IA désactivée (héritage + règles uniquement).")

    @property
    def disponible(self):
        return bool(self.cle) and not self.quota_atteint and self.appels < self.plafond

    def json(self, prompt):
        """Envoie le prompt, renvoie l'objet JSON de la réponse (ou None si échec)."""
        if not self.disponible:
            raise QuotaAtteint()
        attente = config.GEMINI_PAUSE_SECONDES - (time.time() - self.dernier)
        if attente > 0:
            time.sleep(attente)
        body = {
            'contents': [{'parts': [{'text': prompt}]}],
            'generationConfig': {'responseMimeType': 'application/json', 'temperature': 0.4},
        }
        url = API.format(modele=config.GEMINI_MODELE)
        for essai in range(3):
            self.dernier = time.time()
            try:
                resp = requests.post(url, headers={'x-goog-api-key': self.cle}, json=body, timeout=240)
            except requests.RequestException as e:
                log.warning(f"Gemini indisponible ({e}), essai {essai + 1}/3")
                time.sleep(20)
                continue
            if resp.status_code == 429:
                # Quota par minute : on attend une fois ; quota journalier : on s'arrête pour cette nuit
                if essai == 0 and 'PerDay' not in resp.text:
                    time.sleep(60)
                    continue
                self.quota_atteint = True
                log.warning(f"Quota Gemini atteint : reprise à la prochaine exécution. Détail : {resp.text[:400]}")
                raise QuotaAtteint()
            if resp.status_code in (400, 401, 403, 404):
                # Erreur de configuration (modèle retiré, clé invalide) : inutile d'insister cette nuit
                self.quota_atteint = True
                log.error(f"Gemini {resp.status_code} — génération IA arrêtée pour cette exécution : {resp.text[:300]}")
                raise QuotaAtteint()
            if resp.status_code != 200:
                log.warning(f"Gemini {resp.status_code} : {resp.text[:300]}")
                time.sleep(10)
                continue
            self.appels += 1
            try:
                texte = resp.json()['candidates'][0]['content']['parts'][0]['text']
                return json.loads(texte)
            except (KeyError, IndexError, ValueError) as e:
                log.warning(f"Réponse Gemini illisible ({e})")
                return None
        return None
