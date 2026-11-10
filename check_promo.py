"""
Affiche les champs promo d'un ou plusieurs produits dans le flux Lengow Google.

Usage :
    python check_promo.py product-lazaria-v7004-40
    python check_promo.py product-lazaria-v7004-40 product-abigael-b7055-36

Lit cache/flux_gmc.csv s'il existe, sinon télécharge le flux Lengow.
"""
import csv
import os
import sys

import requests

LENGOW_URL = 'https://feeds.lengow.io/3/jzdj298'
CACHE      = 'cache/flux_gmc.csv'


def lines_from_url(url):
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        buf = ''
        for chunk in resp.iter_content(chunk_size=1024 * 1024, decode_unicode=True):
            buf += chunk
            parts = buf.split('\n')
            buf = parts.pop()
            for part in parts:
                yield part + '\n'
        if buf:
            yield buf


def main():
    ids = {a.lower() for a in sys.argv[1:]}
    if not ids:
        sys.exit("Indique au moins un identifiant produit, ex. : python check_promo.py product-lazaria-v7004-40")

    if os.path.exists(CACHE):
        print(f"Lecture : {CACHE}")
        source = open(CACHE, encoding='utf-8-sig', newline='')
    else:
        print(f"Téléchargement : {LENGOW_URL}")
        source = lines_from_url(LENGOW_URL)

    reader  = csv.reader(source, delimiter='|')
    headers = [h.strip().strip("'\"") for h in next(reader)]
    i_id    = headers.index('offer_id')
    cols    = [i for i, h in enumerate(headers)
               if 'sale' in h.lower() or h in ('product_attributes.price.amount_micros',
                                               'product_attributes.price.currency_code',
                                               'product_attributes.availability')]

    print("Colonnes promo trouvées :", [headers[i] for i in cols if 'sale' in headers[i].lower()] or 'AUCUNE')

    found = set()
    for row in reader:
        if i_id >= len(row):
            continue
        offer_id = row[i_id].strip().strip("'\"")
        if offer_id.lower() in ids:
            found.add(offer_id.lower())
            print(f"\n=== {offer_id} ===")
            for i in cols:
                value = row[i].strip().strip("'\"") if i < len(row) else ''
                print(f"  {headers[i]} = {value or '(vide)'}")
            if found == ids:
                break

    for missing in ids - found:
        print(f"\n⚠️ {missing} introuvable dans le flux")


if __name__ == '__main__':
    main()
