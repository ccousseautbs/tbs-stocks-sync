"""
STOCKS SYNC TBS — Script Python (version fusion Lengow FR)

1. Enregistre le projet GCP auprès de la Merchant API
2. Lit le flux Lengow Google Merchant → index GTIN → offer_id (+ ads_grouping)
3. Lit le flux stocks en streaming → garde in_stock + quantité > 0 + boutique renseignée (hors NONE)
4. Génère inventaire_magasin.csv (sans prix : le prix vient du flux principal Lengow FR)
5. (Optionnel) Pousse l'inventaire via Merchant Inventories API v1
6. Écrit le résultat dans Google Sheets

Transition : tant que la source Local FR (GMC_LOCAL_FR) existe encore dans GMC,
GENERATE_LEGACY_LOCAL_FILES = True continue de produire flux_local_gmc.csv et
labels_local_gmc.csv. Passer à False une fois Local FR supprimée.
"""

import csv
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ─────────────────────────────────────────────────
STOCKS_URL      = 'https://tbs.fr/Storage/Lengow/stocks.csv'
LENGOW_URL      = 'https://feeds.lengow.io/3/jzdj298'
MERCHANT_ID     = '110798793'
DEVELOPER_EMAIL = 'ton@email.com'   # ⚠️ à remplacer par ton adresse
SHEET_ID        = '1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU'
TAB_NAME        = 'Stocks'
INVENTORY_PATH  = 'docs/inventaire_magasin.csv'
LOCAL_FLUX_PATH = 'docs/flux_local_gmc.csv'    # legacy (source Local FR)
LABELS_PATH     = 'docs/labels_local_gmc.csv'  # legacy (source Local FR)
EXCLUSIONS_PATH = 'docs/exclusions_locales_gmc.csv'  # source supplémentaire rattachée à FR
SCOPES = [
    'https://www.googleapis.com/auth/content',
    'https://www.googleapis.com/auth/spreadsheets',
]
MERCHANT_API_BASE      = 'https://merchantapi.googleapis.com/inventories/v1'
MERCHANT_ACCOUNTS_BASE = 'https://merchantapi.googleapis.com/accounts/v1'
MAX_WORKERS            = 10

# Interrupteurs
GENERATE_LEGACY_LOCAL_FILES = False  # False une fois Local FR supprimée dans GMC
GENERATE_LOCAL_EXCLUSIONS   = True   # exclut des destinations locales les produits sans stock boutique
LOCAL_DESTINATIONS          = ['free_local_listings', 'local_inventory_ads']  # une colonne par valeur
ENABLE_API_PUSH             = False  # True seulement si l'inventaire passe par l'API et non par le fichier
LOWERCASE_OFFER_IDS         = False  # garder la casse exacte des offer_id Lengow FR

# Nom de produit Merchant API ciblé par le push (contentLanguage~feedLabel~offerId)
PRODUCT_NAME_TEMPLATE = 'fr~FR~{offer_id}'

# Retrait du stock boutique (surcharge le défaut ship_to_store de Lengow FR)
PICKUP_METHOD        = 'buy'
DEFAULT_PICKUP_SLA   = 'same_day'
PICKUP_SLA_BY_STORE  = {
    # '01756_LA-ROCHELLE_TBS': 'next_day',
}
# ────────────────────────────────────────────────────────────

INVENTORY_HEADERS = ['store_code', 'id', 'availability', 'quantity', 'pickup_method', 'pickup_sla']

LOCAL_FLUX_HEADERS = [
    'id', 'title', 'price', 'availability', 'image_link', 'link',
    'gtin', 'brand', 'condition', 'mpn', 'description', 'color',
    'size', 'gender', 'item_group_id',
]

LENGOW_COLS = [
    'offer_id',
    'product_attributes.title',
    'product_attributes.price.amount_micros',
    'product_attributes.price.currency_code',
    'product_attributes.availability',
    'product_attributes.image_link',
    'product_attributes.link',
    'product_attributes.gtins_01',
    'product_attributes.brand',
    'product_attributes.condition',
    'product_attributes.mpn',
    'product_attributes.description',
    'product_attributes.color',
    'product_attributes.size',
    'product_attributes.gender',
    'product_attributes.item_group_id',
    'product_attributes.ads_grouping',
]


# ─── OUTILS ─────────────────────────────────────────────────
def clean(value):
    return value.strip().strip("'\"") if value else ''


def stream_csv(url, delimiter):
    """Lit un CSV distant en streaming, ligne par ligne.
    Générateur : la réponse HTTP reste ouverte tant que les lignes sont consommées.
    csv gère les retours à la ligne entre guillemets."""
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        resp.encoding = 'utf-8'   # évite le repli ISO-8859-1 de requests

        def lines():
            buf, first = '', True
            for chunk in resp.iter_content(chunk_size=1024 * 1024, decode_unicode=True):
                if first:
                    chunk, first = chunk.lstrip('\ufeff'), False
                buf += chunk
                parts = buf.split('\n')
                buf = parts.pop()
                for part in parts:
                    yield part + '\n'   # csv recolle les lignes coupées entre guillemets
            if buf:
                yield buf

        yield from csv.reader(lines(), delimiter=delimiter)


def normalize_id(offer_id):
    return offer_id.lower() if LOWERCASE_OFFER_IDS else offer_id


# ─── AUTH ───────────────────────────────────────────────────
def get_credentials():
    key_json = os.environ.get('GCP_KEY')
    if not key_json:
        raise ValueError("Variable d'environnement GCP_KEY manquante.")
    creds = service_account.Credentials.from_service_account_info(json.loads(key_json), scopes=SCOPES)
    creds.refresh(Request())
    return creds


def register_gcp_project(creds):
    hdrs = {'Authorization': f'Bearer {creds.token}', 'Content-Type': 'application/json'}
    url  = f"{MERCHANT_ACCOUNTS_BASE}/accounts/{MERCHANT_ID}/developerRegistration:registerGcp"
    resp = requests.post(url, headers=hdrs, json={'developerEmail': DEVELOPER_EMAIL}, timeout=15)
    if resp.status_code in (200, 409):
        log.info("✅ Projet GCP enregistré.")
    else:
        log.warning(f"⚠️ Enregistrement GCP : {resp.status_code} — {resp.text[:300]}")


# ─── 1. LENGOW ──────────────────────────────────────────────
def build_gtin_index():
    """Index GTIN → offer_id et GTIN → ads_grouping depuis le flux Lengow (séparateur |)."""
    log.info(f"Lecture flux Lengow : {LENGOW_URL}")
    reader = stream_csv(LENGOW_URL, '|')

    headers = [clean(h) for h in next(reader)]
    col = {c: headers.index(c) for c in LENGOW_COLS if c in headers}
    if 'offer_id' not in col or 'product_attributes.gtins_01' not in col:
        raise ValueError(f"Colonnes offer_id / gtins_01 introuvables dans Lengow : {headers[:10]}…")

    gtin_index, gtin_ads, lengow_rows, all_offer_ids = {}, {}, [], set()
    for row in reader:
        row = [clean(v) for v in row]
        get = lambda c: row[col[c]] if c in col and col[c] < len(row) else ''
        offer_id, gtin = get('offer_id'), get('product_attributes.gtins_01')
        if offer_id:
            all_offer_ids.add(normalize_id(offer_id))
        if not (offer_id and gtin):
            continue
        gtin_index[gtin] = normalize_id(offer_id)
        gtin_ads[gtin]   = get('product_attributes.ads_grouping')
        if GENERATE_LEGACY_LOCAL_FILES:
            lengow_rows.append({c: get(c) for c in col})

    log.info(f"Index GTIN → offer_id : {len(gtin_index)} entrées | offres Lengow : {len(all_offer_ids)}")
    return gtin_index, gtin_ads, lengow_rows, all_offer_ids


# ─── 2. STOCKS ──────────────────────────────────────────────
def stream_and_filter(gtin_index, gtin_ads):
    """L'export stock est un historique : chaque couple EAN × boutique y apparaît plusieurs fois,
    dans l'ordre chronologique. Seule la DERNIÈRE ligne reflète l'état actuel.
    On garde donc la dernière ligne par (boutique, EAN), puis on filtre :
    in_stock, quantité > 0, boutique renseignée (hors NONE), GTIN connu de Lengow.
    Prix et dates promo de l'export sont ignorés (le prix vient de Lengow)."""
    log.info(f"Lecture stocks en streaming : {STOCKS_URL}")
    reader = stream_csv(STOCKS_URL, ';')

    headers = [clean(h) for h in next(reader)]
    i_id, i_store, i_avail = headers.index('id'), headers.index('store_code'), headers.index('availability')
    i_qty = headers.index('quantity') if 'quantity' in headers else None

    latest = {}   # (store, gtin) → (availability, quantity) de la dernière ligne lue
    lues = 0
    for row in reader:
        lues += 1
        if i_store >= len(row) or i_id >= len(row):
            continue
        store = clean(row[i_store])
        if not store or store == 'NONE':
            continue
        avail = clean(row[i_avail]) if i_avail < len(row) else ''
        try:
            qty = int(float(clean(row[i_qty]) or 0)) if i_qty is not None and i_qty < len(row) else 0
        except ValueError:
            qty = 0
        latest[(store, clean(row[i_id]))] = (avail, qty)   # écrase : la dernière ligne gagne

    stats = {'lues': lues, 'couples_boutique_ean': len(latest), 'hors_stock': 0, 'sans_match': 0}
    products = []
    for (store, gtin), (avail, qty) in latest.items():
        if avail != 'in_stock' or qty <= 0:
            stats['hors_stock'] += 1
            continue
        offer_id = gtin_index.get(gtin)
        if not offer_id:
            stats['sans_match'] += 1
            continue
        products.append({
            'store_code':   store,
            'offer_id':     offer_id,
            'availability': 'in_stock',
            'quantity':     qty,
            'pickup_sla':   PICKUP_SLA_BY_STORE.get(store, DEFAULT_PICKUP_SLA),
            'ads_grouping': gtin_ads.get(gtin, ''),
        })

    log.info(f"Stocks — {stats} | lignes retenues : {len(products)}")
    return products


# ─── 3. FICHIERS ────────────────────────────────────────────
def generate_inventory_file(products):
    os.makedirs(os.path.dirname(INVENTORY_PATH), exist_ok=True)
    with open(INVENTORY_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(INVENTORY_HEADERS)
        for p in products:
            w.writerow([p['store_code'], p['offer_id'], p['availability'],
                        p['quantity'], PICKUP_METHOD, p['pickup_sla']])
    log.info(f"✅ Inventaire magasin : {INVENTORY_PATH} ({len(products)} lignes)")


def generate_local_exclusions(all_offer_ids, products):
    """Source supplémentaire GMC (libellé FR) : exclut des destinations locales
    toutes les offres Lengow sans stock dans aucune boutique.
    Un produit qui revient en stock boutique sort du fichier et redevient éligible au local."""
    in_store = {p['offer_id'] for p in products}
    excluded = sorted(all_offer_ids - in_store)
    os.makedirs(os.path.dirname(EXCLUSIONS_PATH), exist_ok=True)
    with open(EXCLUSIONS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        # Attribut répété : une colonne excluded_destination par destination,
        # sinon GMC lit 'a,b' comme une seule valeur (non reconnue)
        w.writerow(['id'] + ['excluded_destination'] * len(LOCAL_DESTINATIONS))
        for offer_id in excluded:
            w.writerow([offer_id] + LOCAL_DESTINATIONS)
    log.info(f"✅ Exclusions locales : {EXCLUSIONS_PATH} ({len(excluded)} produits exclus | "
             f"{len(in_store)} produits avec stock boutique)")


def generate_legacy_files(lengow_rows, products):
    """Fichiers de la source Local FR, conservés pendant la transition uniquement."""
    os.makedirs(os.path.dirname(LOCAL_FLUX_PATH), exist_ok=True)
    with open(LOCAL_FLUX_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(LOCAL_FLUX_HEADERS)
        for r in lengow_rows:
            price = r.get('product_attributes.price.amount_micros', '')
            cur   = r.get('product_attributes.price.currency_code') or 'EUR'
            w.writerow([
                r.get('offer_id', ''),
                r.get('product_attributes.title', ''),
                f"{price} {cur}" if price else '',
                r.get('product_attributes.availability', '').lower(),
                r.get('product_attributes.image_link', ''),
                r.get('product_attributes.link', ''),
                r.get('product_attributes.gtins_01', ''),
                r.get('product_attributes.brand', ''),
                r.get('product_attributes.condition', '').lower() or 'new',
                r.get('product_attributes.mpn', ''),
                r.get('product_attributes.description', ''),
                r.get('product_attributes.color', ''),
                r.get('product_attributes.size', ''),
                r.get('product_attributes.gender', '').lower(),
                r.get('product_attributes.item_group_id', ''),
            ])

    seen = {}
    for p in products:
        seen.setdefault(p['offer_id'], p)
    with open(LABELS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id', 'custom_label_0', 'custom_label_1', 'custom_label_2',
                    'custom_label_3', 'custom_label_4'])
        for offer_id, p in seen.items():
            w.writerow([offer_id, 'IN_STORE', p['store_code'], p['quantity'], '', p['ads_grouping']])
    log.info("✅ Fichiers legacy Local FR générés (transition)")


# ─── 4. PUSH API (optionnel) ────────────────────────────────
def push_single_product(args):
    p, token = args
    name = f"accounts/{MERCHANT_ID}/products/{PRODUCT_NAME_TEMPLATE.format(offer_id=p['offer_id'])}"
    url  = f"{MERCHANT_API_BASE}/{name}/localInventories:insert"
    body = {
        'storeCode': p['store_code'],
        'localInventoryAttributes': {
            'availability': 'IN_STOCK',
            'quantity':     str(p['quantity']),
            'pickupMethod': PICKUP_METHOD.upper(),
            'pickupSla':    p['pickup_sla'].upper(),
        },
    }
    hdrs = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    for attempt in range(2):
        try:
            resp = requests.post(url, headers=hdrs, json=body, timeout=15)
            if resp.status_code == 200:
                return {'product': p['offer_id'], 'ok': True}
            err = {'product': p['offer_id'], 'ok': False, 'status': resp.status_code, 'error': resp.text[:300]}
        except Exception as e:
            err = {'product': p['offer_id'], 'ok': False, 'error': str(e)}
        if attempt == 0:
            time.sleep(1)
    return err


def push_local_inventory(creds, products):
    log.info(f"Push API pour {len(products)} lignes ({MAX_WORKERS} threads)...")
    creds.refresh(Request())
    success, errors = 0, []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(push_single_product, (p, creds.token)) for p in products]
        for n, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res.get('ok'):
                success += 1
            else:
                errors.append(res)
            if n % 200 == 0:
                log.info(f"  {n}/{len(products)} — {success} OK / {len(errors)} erreurs")
    log.info(f"Push terminé — {success} OK / {len(errors)} erreurs")
    for e in errors[:10]:
        log.error(f"  {e.get('product')} [{e.get('status', '')}] : {e.get('error', '')[:200]}")
    return success, errors


# ─── 5. SHEET ───────────────────────────────────────────────
def write_to_sheet(sheets, products):
    rows = [['id', 'store_code', 'availability', 'quantity', 'pickup_sla', 'ads_grouping']]
    rows += [[p['offer_id'], p['store_code'], p['availability'], p['quantity'],
              p['pickup_sla'], p['ads_grouping']] for p in products]
    for attempt in range(3):
        try:
            sheets.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=TAB_NAME).execute()
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!A1",
                valueInputOption='RAW', body={'values': rows},
            ).execute()
            log.info(f"✅ Sheet mis à jour : {len(products)} lignes")
            return
        except Exception as e:
            if attempt < 2:
                log.warning(f"Sheets indisponible, retry {attempt + 1}/3 dans 15s ({e})")
                time.sleep(15)
            else:
                log.error(f"Sheets erreur après 3 tentatives : {e}")


# ─── MAIN ───────────────────────────────────────────────────
def main():
    creds  = get_credentials()
    sheets = build('sheets', 'v4', credentials=creds)
    register_gcp_project(creds)

    gtin_index, gtin_ads, lengow_rows, all_offer_ids = build_gtin_index()
    products = stream_and_filter(gtin_index, gtin_ads)
    if not products:
        log.warning("Aucune ligne valide : fichiers non écrasés.")
        return

    generate_inventory_file(products)
    if GENERATE_LOCAL_EXCLUSIONS:
        generate_local_exclusions(all_offer_ids, products)
    if GENERATE_LEGACY_LOCAL_FILES:
        generate_legacy_files(lengow_rows, products)
    write_to_sheet(sheets, products)

    if ENABLE_API_PUSH:
        push_local_inventory(creds, products)
    else:
        log.info("Push API désactivé : l'inventaire passe par le fichier inventaire_magasin.csv")


if __name__ == '__main__':
    main()
