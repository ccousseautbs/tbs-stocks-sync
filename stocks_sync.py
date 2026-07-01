"""
STOCKS SYNC TBS — Script Python
1. Lit le flux Lengow pour construire un index GTIN → offer_id
2. Lit le flux stocks en streaming, filtre in_stock + store_code valide
3. Remplace le GTIN par l'offer_id dans la colonne id
4. Écrit dans Google Sheets
"""

import os
import csv
import json
import logging
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
STOCKS_URL = 'https://tbs.fr/Storage/Lengow/stocks.csv'
LENGOW_URL = 'https://tbs.fr/Storage/Lengow/Lengow.csv'
SHEET_ID   = '1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU'
TAB_NAME   = 'Stocks'
SCOPES     = ['https://www.googleapis.com/auth/spreadsheets']
# ────────────────────────────────────────────────────────────


def get_sheets_service():
    key_json = os.environ.get('GCP_KEY')
    if not key_json:
        raise ValueError("Variable d'environnement GCP_KEY manquante.")
    key_data = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(
        key_data, scopes=SCOPES
    )
    return build('sheets', 'v4', credentials=creds)


def build_gtin_index():
    """
    Lit le flux Lengow et construit un index gtin → offer_id.
    Le flux Lengow est séparé par | d'après les analyses précédentes.
    """
    log.info(f"Lecture flux Lengow : {LENGOW_URL}")
    response = requests.get(LENGOW_URL, stream=True, timeout=300)
    response.raise_for_status()

    gtin_index = {}
    headers    = None
    idx_offer  = None
    idx_gtin   = None
    count      = 0

    buffer = ''
    for chunk in response.iter_content(chunk_size=1024 * 1024, decode_unicode=True):
        buffer += chunk
        lines   = buffer.split('\n')
        buffer  = lines[-1]

        for line in lines[:-1]:
            line = line.strip()
            if not line:
                continue

            # Détecter le séparateur : | pour Lengow
            row = next(csv.reader([line], delimiter='|'))

            if headers is None:
                headers = row
                try:
                    idx_offer = headers.index('IDENTIFIER')
                except ValueError:
                    idx_offer = 0
                try:
                    idx_gtin = headers.index('EAN')
                except ValueError:
                    idx_gtin = None
                log.info(f"Lengow — offer_id col: {idx_offer} (IDENTIFIER), gtin col: {idx_gtin} (EAN)")
                continue

            if idx_gtin is None:
                continue

            offer_id = row[idx_offer].strip() if idx_offer < len(row) else ''
            gtin     = row[idx_gtin].strip().strip('"') if idx_gtin < len(row) else ''

            if gtin and offer_id:
                gtin_index[gtin] = offer_id
                count += 1

    # Traiter le dernier fragment
    if buffer.strip() and headers:
        row = next(csv.reader([buffer.strip()], delimiter='|'))
        if len(row) > max(idx_offer, idx_gtin or 0):
            offer_id = row[idx_offer].strip()
            gtin     = row[idx_gtin].strip().strip('"') if idx_gtin else ''
            if gtin and offer_id:
                gtin_index[gtin] = offer_id

    log.info(f"Index GTIN → offer_id : {len(gtin_index)} entrées")
    return gtin_index


def stream_and_filter(gtin_index):
    """Lit le CSV stocks en streaming et filtre les lignes utiles."""
    log.info(f"Téléchargement stocks en streaming : {STOCKS_URL}")
    response = requests.get(STOCKS_URL, stream=True, timeout=300)
    response.raise_for_status()

    filtered  = []
    headers   = None
    idx_avail = None
    idx_store = None
    idx_id    = None
    total     = 0
    kept      = 0
    no_match  = 0

    buffer = ''
    for chunk in response.iter_content(chunk_size=1024 * 1024, decode_unicode=True):
        buffer += chunk
        lines   = buffer.split('\n')
        buffer  = lines[-1]

        for line in lines[:-1]:
            line = line.strip()
            if not line:
                continue

            row = next(csv.reader([line], delimiter=';'))

            if headers is None:
                headers   = row
                idx_avail = headers.index('availability')
                idx_store = headers.index('store_code')
                idx_id    = headers.index('id')
                # Remplacer 'id' par 'itemid' + ajouter 'gtin'
                new_headers = ['itemid' if h == 'id' else h for h in headers]
                new_headers.append('gtin')
                filtered.append(new_headers)
                log.info(f"Colonnes stocks : {new_headers}")
                continue

            total += 1
            avail = row[idx_avail].strip().strip('"') if idx_avail < len(row) else ''
            store = row[idx_store].strip().strip('"') if idx_store < len(row) else ''
            gtin  = row[idx_id].strip().strip('"')   if idx_id    < len(row) else ''

            if avail == 'in_stock' and store and store != 'NONE':
                # Résoudre le GTIN en offer_id
                offer_id = gtin_index.get(gtin, '')
                if not offer_id:
                    no_match += 1
                    continue
                # Construire la ligne avec itemid = offer_id + gtin en dernière colonne
                new_row      = list(row)
                new_row[idx_id] = offer_id  # remplacer GTIN par offer_id
                new_row.append(gtin)         # ajouter GTIN en dernière colonne
                filtered.append(new_row)
                kept += 1

    log.info(f"Lignes lues : {total} | Gardées : {kept} | Sans match GTIN : {no_match}")
    return filtered


def write_to_sheet(service, rows):
    log.info(f"Écriture dans Sheet — onglet {TAB_NAME}...")

    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=TAB_NAME,
    ).execute()

    batch_size = 10000
    for i in range(0, len(rows), batch_size):
        batch      = rows[i:i + batch_size]
        range_name = f"{TAB_NAME}!A{i + 1}"
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            body={'values': batch},
        ).execute()
        log.info(f"  Batch {i // batch_size + 1} : {len(batch)} lignes")

    log.info(f"✅ Sheet mis à jour : {len(rows) - 1} produits")


def main():
    service     = get_sheets_service()
    gtin_index  = build_gtin_index()
    filtered    = stream_and_filter(gtin_index)

    if len(filtered) <= 1:
        log.warning("Aucun produit valide trouvé.")
        return

    write_to_sheet(service, filtered)
    log.info("Sync terminée.")


if __name__ == '__main__':
    main()
