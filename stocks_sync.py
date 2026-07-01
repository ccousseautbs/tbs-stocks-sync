"""
STOCKS SYNC TBS — Script Python
1. Lit le flux Lengow pour construire un index GTIN → offer_id
2. Lit le flux stocks en streaming, filtre in_stock + store_code valide
3. Pousse l'inventaire local via Merchant API (Content API v2.1)
"""

import os
import csv
import json
import logging
import requests
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
STOCKS_URL  = 'https://tbs.fr/Storage/Lengow/stocks.csv'
LENGOW_URL  = 'https://tbs.fr/Storage/Lengow/Lengow.csv'
MERCHANT_ID = '110798793'
SHEET_ID    = '1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU'
TAB_NAME    = 'Stocks'
SCOPES      = [
    'https://www.googleapis.com/auth/content',
    'https://www.googleapis.com/auth/spreadsheets',
]
# ────────────────────────────────────────────────────────────


def get_services():
    """Auth via la clé de service stockée dans GCP_KEY."""
    key_json = os.environ.get('GCP_KEY')
    if not key_json:
        raise ValueError("Variable d'environnement GCP_KEY manquante.")
    key_data = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(
        key_data, scopes=SCOPES
    )
    merchant = build('content', 'v2.1', credentials=creds)
    sheets   = build('sheets', 'v4',   credentials=creds)
    return merchant, sheets


def build_gtin_index():
    """Lit le flux Lengow (séparateur virgule) et construit index GTIN → offer_id."""
    log.info(f"Lecture flux Lengow : {LENGOW_URL}")
    response = requests.get(LENGOW_URL, stream=True, timeout=300)
    response.raise_for_status()

    gtin_index = {}
    headers    = None
    idx_offer  = None
    idx_gtin   = None

    buffer = ''
    for chunk in response.iter_content(chunk_size=1024 * 1024, decode_unicode=True):
        buffer += chunk
        lines   = buffer.split('\n')
        buffer  = lines[-1]

        for line in lines[:-1]:
            line = line.strip()
            if not line:
                continue

            row = next(csv.reader([line], delimiter=','))

            if headers is None:
                headers = [h.strip().strip("'\"") for h in row]
                try:
                    idx_offer = headers.index('identifier')
                except ValueError:
                    idx_offer = 0
                try:
                    idx_gtin = headers.index('EAN')
                except ValueError:
                    idx_gtin = None
                log.info(f"Lengow — colonnes : {len(headers)} | offer_id: {idx_offer} | gtin: {idx_gtin}")
                continue

            if idx_gtin is None:
                continue

            offer_id = row[idx_offer].strip().strip("'\"") if idx_offer < len(row) else ''
            gtin     = row[idx_gtin].strip().strip("'\"")  if idx_gtin  < len(row) else ''

            if gtin and offer_id:
                gtin_index[gtin] = offer_id.lower()

    # Dernier fragment
    if buffer.strip() and headers and idx_gtin is not None:
        row = next(csv.reader([buffer.strip()], delimiter=','))
        if len(row) > max(idx_offer, idx_gtin):
            offer_id = row[idx_offer].strip().strip("'\"")
            gtin     = row[idx_gtin].strip().strip("'\"")
            if gtin and offer_id:
                gtin_index[gtin] = offer_id.lower()

    log.info(f"Index GTIN → offer_id : {len(gtin_index)} entrées")
    return gtin_index


def stream_and_filter(gtin_index):
    """Lit le CSV stocks en streaming et filtre les lignes utiles."""
    log.info(f"Téléchargement stocks en streaming : {STOCKS_URL}")
    response = requests.get(STOCKS_URL, stream=True, timeout=300)
    response.raise_for_status()

    products  = []
    headers   = None
    idx_avail = None
    idx_store = None
    idx_id    = None
    idx_price = None
    idx_sale  = None
    idx_sale_date = None
    idx_qty   = None
    total     = 0
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
                headers       = [h.strip().strip("'\"") for h in row]
                idx_avail     = headers.index('availability')
                idx_store     = headers.index('store_code')
                idx_id        = headers.index('id')
                idx_price     = headers.index('price')
                idx_sale      = headers.index('sale_price')       if 'sale_price' in headers else None
                idx_sale_date = headers.index('sale_price_effective_date') if 'sale_price_effective_date' in headers else None
                idx_qty       = headers.index('quantity')         if 'quantity' in headers else None
                log.info(f"Colonnes stocks : {headers}")
                continue

            total += 1
            avail = row[idx_avail].strip().strip('"') if idx_avail < len(row) else ''
            store = row[idx_store].strip().strip('"') if idx_store < len(row) else ''
            gtin  = row[idx_id].strip().strip('"')   if idx_id    < len(row) else ''

            if avail == 'in_stock' and store and store != 'NONE':
                offer_id = gtin_index.get(gtin, '')
                if not offer_id:
                    no_match += 1
                    continue

                # Parser le prix : "37.5 EUR" → amount + currency
                price_raw  = row[idx_price].strip().strip('"') if idx_price < len(row) else ''
                price_parts = price_raw.split(' ')
                price_amount   = price_parts[0] if price_parts else ''
                price_currency = price_parts[1] if len(price_parts) > 1 else 'EUR'

                # Sale price
                sale_amount   = ''
                sale_currency = 'EUR'
                sale_date     = ''
                if idx_sale and idx_sale < len(row):
                    sale_raw = row[idx_sale].strip().strip('"')
                    if sale_raw:
                        sale_parts    = sale_raw.split(' ')
                        sale_amount   = sale_parts[0]
                        sale_currency = sale_parts[1] if len(sale_parts) > 1 else 'EUR'
                if idx_sale_date and idx_sale_date < len(row):
                    sale_date = row[idx_sale_date].strip().strip('"')

                qty = ''
                if idx_qty and idx_qty < len(row):
                    qty = row[idx_qty].strip().strip('"')

                products.append({
                    'offer_id':     offer_id,
                    'store_code':   store,
                    'availability': avail,
                    'price_amount':   price_amount,
                    'price_currency': price_currency,
                    'sale_amount':    sale_amount,
                    'sale_currency':  sale_currency,
                    'sale_date':      sale_date,
                    'quantity':       qty,
                })

    log.info(f"Lignes lues : {total} | Produits valides : {len(products)} | Sans match GTIN : {no_match}")
    return products


def build_product_id(offer_id):
    """
    Construit le product_id GMC depuis l'offer_id.
    Format GMC : online:fr:FR:{offer_id}
    """
    return f"online:fr:FR:{offer_id}"


def push_local_inventory(merchant, products):
    """Pousse l'inventaire local via Content API v2.1 — batch de 100."""
    log.info(f"Push inventaire local pour {len(products)} produits...")

    success  = 0
    errors   = []
    batch_size = 100

    for i in range(0, len(products), batch_size):
        batch   = products[i:i + batch_size]
        entries = []

        for j, p in enumerate(batch):
            product_id = build_product_id(p['offer_id'])

            inventory = {
                'storeCode':    p['store_code'],
                'availability': p['availability'],
            }

            if p['price_amount']:
                inventory['price'] = {
                    'value':    p['price_amount'],
                    'currency': p['price_currency'],
                }

            if p['sale_amount']:
                inventory['salePrice'] = {
                    'value':    p['sale_amount'],
                    'currency': p['sale_currency'],
                }

            if p['sale_date']:
                inventory['salePriceEffectiveDate'] = p['sale_date']

            if p['quantity']:
                try:
                    inventory['quantity'] = int(p['quantity'])
                except ValueError:
                    pass

            entries.append({
                'batchId':   j,
                'merchantId': MERCHANT_ID,
                'method':    'insert',
                'productId': product_id,
                'localInventory': inventory,
            })

        try:
            response = merchant.localinventory().custombatch(
                body={'entries': entries}
            ).execute()

            for entry in response.get('entries', []):
                if entry.get('errors'):
                    for err in entry['errors'].get('errors', []):
                        errors.append({
                            'product': entries[entry['batchId']]['productId'],
                            'error':   err.get('message', ''),
                        })
                else:
                    success += 1

        except Exception as e:
            log.error(f"Batch {i // batch_size + 1} erreur : {e}")
            errors.append({'product': 'batch', 'error': str(e)})

        log.info(f"  Batch {i // batch_size + 1}/{(len(products) + batch_size - 1) // batch_size} — {success} OK / {len(errors)} erreurs")
        time.sleep(0.5)  # éviter rate limiting

    return success, errors


def write_to_sheet(sheets, products):
    """Écrit le résultat dans le Sheet pour visibilité."""
    log.info(f"Écriture dans Sheet — onglet {TAB_NAME}...")

    headers = ['id', 'store_code', 'availability', 'price', 'sale_price', 'quantity']
    rows    = [headers]
    for p in products:
        rows.append([
            p['offer_id'],
            p['store_code'],
            p['availability'],
            f"{p['price_amount']} {p['price_currency']}",
            f"{p['sale_amount']} {p['sale_currency']}" if p['sale_amount'] else '',
            p['quantity'],
        ])

    sheets.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=TAB_NAME,
    ).execute()

    sheets.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A1",
        valueInputOption='RAW',
        body={'values': rows},
    ).execute()

    log.info(f"✅ Sheet mis à jour : {len(products)} produits")


def main():
    merchant, sheets = get_services()
    gtin_index       = build_gtin_index()
    products         = stream_and_filter(gtin_index)

    if not products:
        log.warning("Aucun produit valide trouvé.")
        return

    # Écrire dans le Sheet pour visibilité
    write_to_sheet(sheets, products)

    # Pousser via Merchant API
    success, errors = push_local_inventory(merchant, products)

    log.info(f"\n=== RÉSULTAT ===")
    log.info(f"✅ Succès  : {success}")
    log.info(f"❌ Erreurs : {len(errors)}")
    if errors:
        for e in errors[:10]:
            log.error(f"  {e['product']} : {e['error']}")


if __name__ == '__main__':
    main()
