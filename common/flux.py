"""Lecture des flux : téléchargement, streaming CSV, catalogue Lengow, index GTIN → offer_id."""

import csv
import logging
import os
from collections import defaultdict

import requests

log = logging.getLogger(__name__)
csv.field_size_limit(10 * 1024 * 1024)


def clean(value):
    return value.strip().strip("'\"") if value else ''


def _lignes_http(url):
    """Générateur de lignes texte depuis une URL, connexion gardée ouverte pendant la lecture."""
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        resp.encoding = 'utf-8'   # évite le repli ISO-8859-1 de requests
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


def stream_csv(source, delimiter):
    """Lit un CSV ligne par ligne, depuis une URL ou un fichier local."""
    if source.startswith('http'):
        yield from csv.reader(_lignes_http(source), delimiter=delimiter)
    else:
        with open(source, newline='', encoding='utf-8-sig') as f:
            yield from csv.reader(f, delimiter=delimiter)


def telecharger(url, chemin):
    """Télécharge un fichier en streaming sur disque (job 1)."""
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(chemin, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    taille = os.path.getsize(chemin)
    if taille < 1000:
        raise ValueError(f"Fichier téléchargé trop petit ({taille} octets) : {url}")
    log.info(f"✅ {url} → {chemin} ({taille / 1e6:.1f} Mo)")


def source_disponible(chemin_local, url):
    """Préfère la copie locale du job 1, sinon l'URL (exécution manuelle hors workflow)."""
    return chemin_local if os.path.exists(chemin_local) else url


# ─── FLUX CANAL GOOGLE ──────────────────────────────────────
def index_gtin_offer_id(source):
    """GTIN → offer_id tel que GMC le voit (flux canal Google Lengow, séparateur |)."""
    reader = stream_csv(source, '|')
    headers = [clean(h) for h in next(reader)]
    try:
        i_id, i_gtin = headers.index('offer_id'), headers.index('product_attributes.gtins_01')
    except ValueError:
        raise ValueError(f"Colonnes offer_id / gtins_01 introuvables : {headers[:10]}…")
    index = {}
    for row in reader:
        if len(row) <= max(i_id, i_gtin):
            continue
        offer_id, gtin = clean(row[i_id]), clean(row[i_gtin])
        if offer_id and gtin:
            index[gtin] = offer_id
    log.info(f"Index GTIN → offer_id : {len(index)} entrées")
    return index


# ─── CATALOGUE ──────────────────────────────────────────────
def code_coloris(mpn):
    """Les 8 premiers caractères du MPN identifient le modèle, la suite le coloris."""
    return mpn[8:] if len(mpn) > 8 else ''


def nom_coloris(axis_color, mpn):
    """'*OPALEA7142' + MPN '...A7142' → 'Opale'."""
    nom = (axis_color or '').strip()
    code = code_coloris(mpn)
    if code and nom.upper().endswith(code.upper()):
        nom = nom[: -len(code)]
    return nom.strip(' *-+').capitalize()


def lire_catalogue(source):
    """Retourne (skus, coloris) depuis le catalogue Lengow.
    skus    : liste de dicts par taille (lignes child et simple)
    coloris : MPN → dict agrégé (parent, champs produit, stock total, tailles en stock)
    """
    reader = stream_csv(source, ',')
    headers = [clean(h) for h in next(reader)]
    skus = []
    for row in reader:
        r = {h: (row[i].strip() if i < len(row) else '') for i, h in enumerate(headers)}
        if r.get('type') not in ('child', 'simple') or not r.get('MPN'):
            continue
        try:
            r['_stock'] = int(float(r.get('stock') or 0))
        except ValueError:
            r['_stock'] = 0
        try:
            r['_prix'] = float((r.get('Prix remisé') or r.get('price') or '0').replace(',', '.'))
        except ValueError:
            r['_prix'] = 0.0
        if r.get('type') == 'simple' and not r.get('parent'):
            r['parent'] = r['identifier']
        skus.append(r)

    # Titre lisible par MPN (certaines lignes portent un code à la place du titre)
    titres = {}
    for r in skus:
        t = r.get('Titre du produit', '')
        if t and r['MPN'].upper() not in t.upper() and not t.lower().startswith('product-'):
            titres.setdefault(r['MPN'], t)

    # Un même MPN peut exister en ligne « simple » ET en ligne « child » : on garde le child
    mpn_child = {r['MPN'] for r in skus if r.get('type') == 'child'}
    skus = [r for r in skus if r.get('type') == 'child' or r['MPN'] not in mpn_child]

    coloris = {}
    tailles = defaultdict(list)
    for r in skus:
        mpn = r['MPN']
        if mpn not in coloris:
            c = {k: v for k, v in r.items() if not k.startswith('_')}
            c['_stock'], c['_prix'] = 0, r['_prix']
            c['_nom_coloris'] = nom_coloris(r.get('axis_color'), mpn)
            c['_titre'] = titres.get(mpn, '')
            coloris[mpn] = c
        coloris[mpn]['_stock'] += max(r['_stock'], 0)
        if r['_stock'] > 0:
            tailles[mpn].append(r.get('axis_size', ''))
    for mpn, c in coloris.items():
        c['_tailles_stock'] = tailles.get(mpn, [])
    log.info(f"Catalogue : {len(skus)} SKU, {len(coloris)} coloris, "
             f"{len({c['parent'] for c in coloris.values()})} modèles")
    return skus, coloris


def est_chaussure(c):
    comps = ' '.join(c.get(f'Composition {i}', '') for i in range(1, 6))
    return 'Dessus' in comps or 'Semelle' in comps
