"""
STOCKS SYNC TBS — Script Python
Lit le flux stocks en streaming, filtre in_stock + store_code valide,
écrit dans Google Sheets.
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
SHEET_ID   = '1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU'
TAB_NAME   = 'Stocks'
SCOPES     = ['https://www.googleapis.com/auth/spreadsheets']
# ────────────────────────────────────────────────────────────


def get_sheets_service():
    """Auth via la clé de service stockée dans la variable d'env GCP_KEY."""
    key_json = os.environ.get('GCP_KEY')
    if not key_json:
        raise ValueError("Variable d'environnement GCP_KEY manquante.")
    key_data = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(
        key_data, scopes=SCOPES
    )
    return build('sheets', 'v4', credentials=creds)


def stream_and_filter():
    """Lit le CSV en streaming et filtre les lignes utiles."""
    log.info(f"Téléchargement en streaming : {STOCKS_URL}")
    response = requests.get(STOCKS_URL, stream=True, timeout=300)
    response.raise_for_status()

    filtered = []
    headers  = None
    idx_avail = None
    idx_store = None
    total = 0
    kept  = 0

    # Décoder ligne par ligne sans tout charger en mémoire
    buffer = ''
    for chunk in response.iter_content(chunk_size=1024 * 1024, decode_unicode=True):
        buffer += chunk
        lines   = buffer.split('\n')
        buffer  = lines[-1]  # garder le fragment incomplet pour le prochain chunk

        for line in lines[:-1]:
            line = line.strip()
            if not line:
                continue

            row = next(csv.reader([line], delimiter=';'))

            if headers is None:
                headers   = row
                # Renommer 'id' en 'gtin' pour GMC Local Inventory
                headers = ['gtin' if h == 'id' else h for h in headers]
                idx_avail = headers.index('availability')
                idx_store = headers.index('store_code')
                filtered.append(headers)
                log.info(f"Colonnes : {headers}")
                continue

            total += 1

            avail = row[idx_avail].strip().strip('"') if idx_avail < len(row) else ''
            store = row[idx_store].strip().strip('"') if idx_store < len(row) else ''

            if avail == 'in_stock' and store and store != 'NONE':
                filtered.append(row)
                kept += 1

    # Traiter le dernier fragment
    if buffer.strip():
        row = next(csv.reader([buffer.strip()], delimiter=';'))
        if headers and len(row) == len(headers):
            avail = row[idx_avail].strip().strip('"')
            store = row[idx_store].strip().strip('"')
            if avail == 'in_stock' and store and store != 'NONE':
                filtered.append(row)
                kept += 1

    log.info(f"Lignes lues : {total} | Gardées : {kept}")
    return filtered


def write_to_sheet(service, rows):
    """Écrit les données dans le Sheet par batch."""
    log.info(f"Écriture dans Sheet {SHEET_ID} — onglet {TAB_NAME}...")

    # Vider l'onglet
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=TAB_NAME,
    ).execute()

    # Écrire par batch de 10 000 lignes
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
        log.info(f"  Batch {i // batch_size + 1} : {len(batch)} lignes écrites")

    log.info(f"✅ Sheet mis à jour : {len(rows) - 1} produits")


def main():
    service  = get_sheets_service()
    filtered = stream_and_filter()

    if len(filtered) <= 1:
        log.warning("Aucun produit in_stock avec store_code valide trouvé.")
        return

    write_to_sheet(service, filtered)
    log.info("Sync terminée.")


if __name__ == '__main__':
    main()
