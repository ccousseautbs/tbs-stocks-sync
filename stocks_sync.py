"""
STOCKS SYNC TBS — Script Python
1. Enregistre le projet GCP auprès de la Merchant API
2. Lit le flux Lengow → index GTIN → offer_id
3. Génère le flux local GMC (CSV plat) → publié via GitHub Pages
4. Lit le flux stocks en streaming → filtre in_stock + store_code valide
5. Pousse l'inventaire local via Merchant Inventories API v1 (10 threads parallèles)
6. Écrit le résultat dans Google Sheets
"""

import os
import csv
import json
import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
STOCKS_URL        = 'https://tbs.fr/Storage/Lengow/stocks.csv'
LENGOW_URL        = 'https://feeds.lengow.io/3/jzdj298'
MERCHANT_ID       = '110798793'
DEVELOPER_EMAIL   = 'ton@email.com'
SHEET_ID          = '1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU'
TAB_NAME          = 'Stocks'
LOCAL_FLUX_PATH   = 'docs/flux_local_gmc.csv'
SCOPES            = [
    'https://www.googleapis.com/auth/content',
    'https://www.googleapis.com/auth/spreadsheets',
]
MERCHANT_API_BASE      = 'https://merchantapi.googleapis.com/inventories/v1'
MERCHANT_ACCOUNTS_BASE = 'https://merchantapi.googleapis.com/accounts/v1'
MAX_WORKERS            = 10  # threads parallèles
# ────────────────────────────────────────────────────────────

LOCAL_FLUX_HEADERS = [
    'id', 'title', 'price', 'availability', 'image_link', 'link',
    'gtin', 'brand', 'condition', 'mpn', 'description', 'color',
    'size', 'gender', 'item_group_id',
]

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
    hdrs = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    url  = f"{MERCHANT_ACCOUNTS_BASE}/accounts/{MERCHANT_ID}/developerRegistration:registerGcp"
    resp = requests.post(url, headers=hdrs, json={'developerEmail': DEVELOPER_EMAIL}, timeout=15)
    if resp.status_code == 200:
        log.info("✅ Projet GCP enregistré avec succès.")
    elif resp.status_code == 409:
        log.info("✅ Projet GCP déjà enregistré.")
    else:
        log.warning(f"⚠️ Enregistrement GCP : {resp.status_code} — {resp.text[:300]}")


def build_gtin_index():
    log.info(f"Lecture flux Lengow : {LENGOW_URL}")
    response = requests.get(LENGOW_URL, stream=True, timeout=300)
    response.raise_for_status()

    gtin_index  = {}
    lengow_rows = []
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

            row = next(csv.reader([line], delimiter='|'))

            if headers is None:
                headers = [h.strip().strip("'\"") for h in row]
                try:
                    idx_offer = headers.index('offer_id')
                except ValueError:
                    idx_offer = 0
                try:
                    idx_gtin = headers.index('product_attributes.gtins_01')
                except ValueError:
                    idx_gtin = None
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
    log.info(f"Génération flux local GMC : {len(lengow_rows)} produits...")
    os.makedirs(os.path.dirname(LOCAL_FLUX_PATH), exist_ok=True)

    with open(LOCAL_FLUX_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(LOCAL_FLUX_HEADERS)

        for row in lengow_rows:
            def get(col):
                return row[col_indices[col]].strip() if col in col_indices else ''

            price_raw = get('price')
            price_str = f"{price_raw} EUR" if price_raw else ''

            # Convertir stock → availability GMC
            stock_val = get('stock')
            try:
                avail = 'in_stock' if int(stock_val) > 0 else 'out_of_stock'
            except (ValueError, TypeError):
                avail = 'in_stock' if stock_val and stock_val != '0' else 'out_of_stock'

            writer.writerow([
                get('identifier').lower(),
                get('Titre du produit'),
                price_str,
                avail,
                get('image_url'),
                get('product_url'),
                get('EAN'),
                get('brand'),
                'new',
                get('MPN'),
                get('description'),
                get('Couleur de filtre'),
                get('axis_size'),
                get('Genre'),
                get('parent'),
            ])

    log.info(f"✅ Flux local généré : {LOCAL_FLUX_PATH}")


def stream_and_filter(gtin_index):
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


def push_single_product(args):
    """Push inventaire pour un seul produit — appelé en parallèle."""
    p, token = args
    hdrs = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    url  = f"{MERCHANT_API_BASE}/{build_product_name(p['offer_id'])}/localInventories:insert"

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

    for attempt in range(2):
        try:
            resp = requests.post(url, headers=hdrs, json=body, timeout=15)
            if resp.status_code == 200:
                return {'product': p['offer_id'], 'ok': True}
            else:
                if attempt == 0:
                    time.sleep(1)
                    continue
                return {'product': p['offer_id'], 'ok': False, 'status': resp.status_code, 'error': resp.text[:300]}
        except Exception as e:
            if attempt == 0:
                time.sleep(1)
                continue
            return {'product': p['offer_id'], 'ok': False, 'error': str(e)}

    return {'product': p['offer_id'], 'ok': False, 'error': 'Max retries exceeded'}


def push_local_inventory(creds, products):
    log.info(f"Push inventaire local pour {len(products)} produits ({MAX_WORKERS} threads parallèles)...")

    creds.refresh(Request())
    token   = creds.token
    success = 0
    errors  = []
    done    = 0

    args = [(p, token) for p in products]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(push_single_product, arg): arg[0] for arg in args}

        for future in as_completed(futures):
            result = future.result()
            done  += 1

            if result.get('ok'):
                success += 1
            else:
                errors.append(result)

            if done % 50 == 0:
                log.info(f"  Progression : {done}/{len(products)} — {success} OK / {len(errors)} erreurs")
                if errors:
                    log.error(f"  Dernière erreur : {errors[-1]}")

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
