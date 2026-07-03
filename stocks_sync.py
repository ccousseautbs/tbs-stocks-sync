"""
STOCKS SYNC TBS — Script Python
1. Enregistre le projet GCP auprès de la Merchant API
2. Lit le flux Lengow → index GTIN → offer_id
3. Génère le flux local GMC (CSV plat) → publié via GitHub Pages
4. Lit le flux stocks en streaming → filtre in_stock + store_code valide
5. Pousse l'inventaire local via Merchant Inventories API v1
6. Écrit le résultat dans Google Sheets
"""

import os
import csv
import json
import logging
import requests
import time
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
STOCKS_URL        = 'https://tbs.fr/Storage/Lengow/stocks.csv'
LENGOW_URL        = 'https://tbs.fr/Storage/Lengow/Lengow.csv'
MERCHANT_ID       = '110798793'
DEVELOPER_EMAIL   = 'ton@email.com'  # ← ton email GMC/Google
SHEET_ID          = '1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU'
TAB_NAME          = 'Stocks'
LOCAL_FLUX_PATH   = 'docs/flux_local_gmc.csv'  # publié via GitHub Pages
SCOPES            = [
    'https://www.googleapis.com/auth/content',
    'https://www.googleapis.com/auth/spreadsheets',
]
MERCHANT_API_BASE      = 'https://merchantapi.googleapis.com/inventories/v1'
MERCHANT_ACCOUNTS_BASE = 'https://merchantapi.googleapis.com/accounts/v1'
# ────────────────────────────────────────────────────────────

# Colonnes du flux local plat pour GMC
LOCAL_FLUX_HEADERS = [
    'id', 'title', 'price', 'availability', 'image_link', 'link',
    'gtin', 'brand', 'condition', 'mpn', 'description', 'color',
    'size', 'gender', 'item_group_id',
]

# Mapping colonnes Lengow → colonnes flat GMC
LENGOW_COL_MAP = {
    'offer_id':                               'id',
    'product_attributes.title':               'title',
    'product_attributes.price.amount_micros': 'price',
    'product_attributes.price.currency_code': 'price_currency',
    'product_attributes.availability':        'availability',
    'product_attributes.image_link':          'image_link',
    'product_attributes.link':                'link',
    'product_attributes.gtins_01':            'gtin',
    'product_attributes.brand':               'brand',
    'product_attributes.condition':           'condition',
    'product_attributes.mpn':                 'mpn',
    'product_attributes.description':         'description',
    'product_attributes.color':               'color',
    'product_attributes.size':                'size',
    'product_attributes.gender':              'gender',
    'product_attributes.item_group_id':       'item_group_id',
}
# ────────────────────────────────────────────────────────────


def get_credentials():
    key_json = os.environ.get('GCP_KEY')
    if not key_json:
        raise ValueError("Variable d'environnement GCP_KEY manquante.")
    key_data = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(
        key_data, scopes=SCOPES
    )
    creds.refresh(Request())
    return creds


def get_sheets_service(creds):
    return build('sheets', 'v4', credentials=creds)


def register_gcp_project(creds):
    token = creds.token
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type':  'application/json',
    }
    url  = f"{MERCHANT_ACCOUNTS_BASE}/accounts/{MERCHANT_ID}/developerRegistration:registerGcp"
    body = {'developerEmail': DEVELOPER_EMAIL}
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    if resp.status_code == 200:
        log.info("✅ Projet GCP enregistré avec succès.")
    elif resp.status_code == 409:
        log.info("✅ Projet GCP déjà enregistré.")
    else:
        log.warning(f"⚠️ Enregistrement GCP : {resp.status_code} — {resp.text[:300]}")


def build_gtin_index():
    """Lit le flux Lengow (séparateur virgule) et construit index GTIN → offer_id."""
    log.info(f"Lecture flux Lengow : {LENGOW_URL}")
    response = requests.get(LENGOW_URL, stream=True, timeout=300)
    response.raise_for_status()

    gtin_index  = {}
    lengow_rows = []  # pour la génération du flux local
    headers     = None
    idx_offer   = None
    idx_gtin    = None
    col_indices = {}

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
                # Indices pour le flux local
                for lengow_col in LENGOW_COL_MAP:
                    if lengow_col in headers:
                        col_indices[lengow_col] = headers.index(lengow_col)
                log.info(f"Lengow — colonnes : {len(headers)} | offer_id: {idx_offer} | gtin: {idx_gtin}")
                continue

            if idx_gtin is None:
                continue

            offer_id = row[idx_offer].strip().strip("'\"") if idx_offer < len(row) else ''
            gtin     = row[idx_gtin].strip().strip("'\"")  if idx_gtin  < len(row) else ''

            if gtin and offer_id:
                gtin_index[gtin] = offer_id.lower()
                lengow_rows.append(row)

    # Dernier fragment
    if buffer.strip() and headers and idx_gtin is not None:
        row = next(csv.reader([buffer.strip()], delimiter=','))
        if len(row) > max(idx_offer, idx_gtin):
            offer_id = row[idx_offer].strip().strip("'\"")
            gtin     = row[idx_gtin].strip().strip("'\"")
            if gtin and offer_id:
                gtin_index[gtin] = offer_id.lower()
                lengow_rows.append(row)

    log.info(f"Index GTIN → offer_id : {len(gtin_index)} entrées")
    return gtin_index, lengow_rows, col_indices


def generate_local_flux(lengow_rows, col_indices):
    """Génère le CSV plat pour la source locale GMC."""
    log.info(f"Génération flux local GMC : {len(lengow_rows)} produits...")

    os.makedirs(os.path.dirname(LOCAL_FLUX_PATH), exist_ok=True)

    with open(LOCAL_FLUX_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(LOCAL_FLUX_HEADERS)

        for row in lengow_rows:
            price_raw = ''
            currency  = 'EUR'
            if 'product_attributes.price.amount_micros' in col_indices:
                price_raw = row[col_indices['product_attributes.price.amount_micros']].strip()
            if 'product_attributes.price.currency_code' in col_indices:
                currency = row[col_indices['product_attributes.price.currency_code']].strip() or 'EUR'
            price_str = f"{price_raw} {currency}" if price_raw else ''

            writer.writerow([
                row[col_indices.get('offer_id', 0)].strip().lower(),
                row[col_indices['product_attributes.title']].strip() if 'product_attributes.title' in col_indices else '',
                price_str,
                row[col_indices['product_attributes.availability']].strip() if 'product_attributes.availability' in col_indices else '',
                row[col_indices['product_attributes.image_link']].strip() if 'product_attributes.image_link' in col_indices else '',
                row[col_indices['product_attributes.link']].strip() if 'product_attributes.link' in col_indices else '',
                row[col_indices['product_attributes.gtins_01']].strip() if 'product_attributes.gtins_01' in col_indices else '',
                row[col_indices['product_attributes.brand']].strip() if 'product_attributes.brand' in col_indices else '',
                row[col_indices['product_attributes.condition']].strip() or 'new' if 'product_attributes.condition' in col_indices else 'new',
                row[col_indices['product_attributes.mpn']].strip() if 'product_attributes.mpn' in col_indices else '',
                row[col_indices['product_attributes.description']].strip() if 'product_attributes.description' in col_indices else '',
                row[col_indices['product_attributes.color']].strip() if 'product_attributes.color' in col_indices else '',
                row[col_indices['product_attributes.size']].strip() if 'product_attributes.size' in col_indices else '',
                row[col_indices['product_attributes.gender']].strip() if 'product_attributes.gender' in col_indices else '',
                row[col_indices['product_attributes.item_group_id']].strip() if 'product_attributes.item_group_id' in col_indices else '',
            ])

    log.info(f"✅ Flux local généré : {LOCAL_FLUX_PATH}")


def stream_and_filter(gtin_index):
    """Lit le CSV stocks en streaming et filtre les lignes utiles."""
    log.info(f"Téléchargement stocks en streaming : {STOCKS_URL}")
    response = requests.get(STOCKS_URL, stream=True, timeout=300)
    response.raise_for_status()

    products      = []
    headers       = None
    idx_avail     = None
    idx_store     = None
    idx_id        = None
    idx_price     = None
    idx_sale      = None
    idx_sale_date = None
    idx_qty       = None
    total         = 0
    no_match      = 0

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
                idx_sale      = headers.index('sale_price') if 'sale_price' in headers else None
                idx_sale_date = headers.index('sale_price_effective_date') if 'sale_price_effective_date' in headers else None
                idx_qty       = headers.index('quantity') if 'quantity' in headers else None
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

                price_raw      = row[idx_price].strip().strip('"') if idx_price < len(row) else ''
                price_parts    = price_raw.split(' ')
                price_amount   = price_parts[0] if price_parts else ''
                price_currency = price_parts[1] if len(price_parts) > 1 else 'EUR'

                sale_amount   = ''
                sale_currency = 'EUR'
                sale_date     = ''
                if idx_sale is not None and idx_sale < len(row):
                    sale_raw = row[idx_sale].strip().strip('"')
                    if sale_raw:
                        sale_parts    = sale_raw.split(' ')
                        sale_amount   = sale_parts[0]
                        sale_currency = sale_parts[1] if len(sale_parts) > 1 else 'EUR'
                if idx_sale_date is not None and idx_sale_date < len(row):
                    # Supprimer espaces autour du / pour format RFC 3339
                    sale_date = row[idx_sale_date].strip().strip('"').replace(' ', '')

                qty = 0
                if idx_qty is not None and idx_qty < len(row):
                    try:
                        qty = int(row[idx_qty].strip().strip('"'))
                    except ValueError:
                        qty = 0

                products.append({
                    'offer_id':       offer_id,
                    'store_code':     store,
                    'availability':   avail,
                    'price_amount':   price_amount,
                    'price_currency': price_currency,
                    'sale_amount':    sale_amount,
                    'sale_currency':  sale_currency,
                    'sale_date':      sale_date,
                    'quantity':       qty,
                })

    log.info(f"Lignes lues : {total} | Produits valides : {len(products)} | Sans match GTIN : {no_match}")
    return products


def build_product_name(offer_id):
    return f"accounts/{MERCHANT_ID}/products/local~fr~FR~{offer_id}"


def push_local_inventory(creds, products):
    log.info(f"Push inventaire local pour {len(products)} produits...")

    token   = creds.token
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type':  'application/json',
    }

    success = 0
    errors  = []

    for i, p in enumerate(products):
        product_name = build_product_name(p['offer_id'])
        url = f"{MERCHANT_API_BASE}/{product_name}/localInventories:insert"

        local_attrs = {
            'availability': p['availability'].upper().replace(' ', '_'),
            'quantity':     p['quantity'],
        }

        if p['price_amount']:
            try:
                local_attrs['price'] = {
                    'amountMicros': str(int(float(p['price_amount']) * 1_000_000)),
                    'currencyCode': p['price_currency'],
                }
            except ValueError:
                pass

        if p['sale_amount']:
            try:
                local_attrs['salePrice'] = {
                    'amountMicros': str(int(float(p['sale_amount']) * 1_000_000)),
                    'currencyCode': p['sale_currency'],
                }
            except ValueError:
                pass

        if p['sale_date']:
            date_parts = p['sale_date'].split('/')
            if len(date_parts) == 2:
                local_attrs['salePriceEffectiveDate'] = {
                    'startTime': date_parts[0],
                    'endTime':   date_parts[1],
                }

        body = {
            'storeCode':                p['store_code'],
            'localInventoryAttributes': local_attrs,
        }

        try:
            resp = requests.post(url, headers=headers, json=body, timeout=30)
            if resp.status_code == 200:
                success += 1
            else:
                errors.append({
                    'product': p['offer_id'],
                    'status':  resp.status_code,
                    'error':   resp.text[:300],
                })
        except Exception as e:
            errors.append({'product': p['offer_id'], 'error': str(e)})

        if (i + 1) % 50 == 0:
            log.info(f"  Progression : {i + 1}/{len(products)} — {success} OK / {len(errors)} erreurs")

        time.sleep(0.02)

    log.info(f"Push terminé — {success} OK / {len(errors)} erreurs")
    return success, errors


def write_to_sheet(sheets, products):
    log.info(f"Écriture dans Sheet — onglet {TAB_NAME}...")

    headers_row = ['id', 'store_code', 'availability', 'price', 'sale_price', 'quantity']
    rows = [headers_row]
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
    creds  = get_credentials()
    sheets = get_sheets_service(creds)

    register_gcp_project(creds)

    gtin_index, lengow_rows, col_indices = build_gtin_index()

    # Générer le flux local CSV pour GitHub Pages
    generate_local_flux(lengow_rows, col_indices)

    products = stream_and_filter(gtin_index)

    if not products:
        log.warning("Aucun produit valide trouvé.")
        return

    write_to_sheet(sheets, products)

    success, errors = push_local_inventory(creds, products)

    log.info(f"\n=== RÉSULTAT FINAL ===")
    log.info(f"✅ Succès  : {success}")
    log.info(f"❌ Erreurs : {len(errors)}")
    if errors:
        for e in errors[:10]:
            log.error(f"  {e.get('product')} [{e.get('status', '')}] : {e.get('error', '')[:200]}")


if __name__ == '__main__':
    main()
