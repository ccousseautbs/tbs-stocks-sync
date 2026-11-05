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
        self.appels = 0        # réponses réussies
        self.requetes = 0      # toutes les requêtes envoyées (Google les décompte toutes du quota)
        self.echecs_consecutifs = 0
        self.dernier = 0.0
        self.raison_arret = '' if self.cle_presente() else 'pas de clé GEMINI_API_KEY'
        self.quota_atteint = False
        if not self.cle:
            log.info("Pas de GEMINI_API_KEY : génération IA désactivée (héritage + règles uniquement).")

    def cle_presente(self):
        return bool(os.environ.get('GEMINI_API_KEY', '').strip())

    def arreter(self, raison):
        self.quota_atteint = True
        if not self.raison_arret:
            self.raison_arret = raison

    def resume(self):
        """Ligne lisible pour le résumé GitHub Actions."""
        if not self.cle:
            return "IA désactivée (pas de clé GEMINI_API_KEY)"
        raison = self.raison_arret or ('plafond de la nuit atteint' if self.requetes >= self.plafond else 'aucun')
        return (f"requêtes Gemini : {self.requetes} / {self.plafond} ({self.appels} réussies) "
                f"· arrêt : {raison}")

    @property
    def disponible(self):
        return bool(self.cle) and not self.quota_atteint and self.requetes < self.plafond

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
        # Google surchargé (503) : un seul réessai, après 2 minutes. Chaque requête compte dans le quota du jour,
        # un lot en échec coûte donc 2 requêtes au lieu de 3.
        attentes_5xx = [120]
        essais = len(attentes_5xx) + 1
        for essai in range(essais):
            if self.requetes >= self.plafond:
                break
            self.dernier = time.time()
            self.requetes += 1
            try:
                resp = requests.post(url, headers={'x-goog-api-key': self.cle}, json=body, timeout=240)
            except requests.RequestException as e:
                self.derniere_erreur = 'Google injoignable (réseau)'
                log.warning(f"Gemini injoignable ({e}), essai {essai + 1}/{essais}")
                if essai < essais - 1:
                    time.sleep(attentes_5xx[essai])
                continue
            if resp.status_code == 429:
                # Quota par minute : on attend une fois ; quota journalier : on s'arrête pour cette nuit
                if essai == 0 and 'PerDay' not in resp.text:
                    time.sleep(60)
                    continue
                self.arreter('quota Google atteint (429)')
                log.warning(f"Quota Gemini atteint : reprise à la prochaine exécution. Détail : {resp.text[:400]}")
                raise QuotaAtteint()
            if resp.status_code in (400, 401, 403, 404):
                # Erreur de configuration (modèle retiré, clé invalide) : inutile d'insister cette nuit
                self.arreter(f'erreur de configuration ({resp.status_code} : modèle ou clé)')
                log.error(f"Gemini {resp.status_code} — génération IA arrêtée pour cette exécution : {resp.text[:300]}")
                raise QuotaAtteint()
            if resp.status_code != 200:
                # 500/502/503/504 : surcharge temporaire côté Google
                self.derniere_erreur = f'Google surchargé ({resp.status_code})'
                log.warning(f"Gemini {resp.status_code}, essai {essai + 1}/{essais} : {resp.text[:200]}")
                if essai < essais - 1:
                    time.sleep(attentes_5xx[essai])
                continue
            self.appels += 1
            self.echecs_consecutifs = 0
            try:
                texte = resp.json()['candidates'][0]['content']['parts'][0]['text']
                return json.loads(texte)
            except (KeyError, IndexError, ValueError) as e:
                log.warning(f"Réponse Gemini illisible ({e})")
                return None
        # Lot en échec après tous les essais : au 2e lot consécutif, on arrête pour préserver le quota du jour
        self.echecs_consecutifs += 1
        if self.echecs_consecutifs >= 2:
            self.arreter(getattr(self, 'derniere_erreur', 'échecs répétés') + ' sur 2 lots consécutifs')
            log.warning("Gemini indisponible sur 2 lots consécutifs : génération IA arrêtée, reprise au prochain passage.")
        return None
