"""
JOB 4 — ENRICHISSEMENT GMC (unique source supplémentaire, sans passer par Lengow)

Produit docs/enrichissement_gmc.tsv : une ligne par offre GMC (id = offer_id du flux Lengow FR), avec
structured_description (descriptions IA déclarées comme telles), product_highlight, product_detail,
question_and_answer et related_product, au format texte Google.
La source Lengow Tbs-source-descriptions-site reste figée et ne sert plus qu'aux autres canaux.

Q&R :
- par règles, recalculées chaque nuit (autres coloris en stock, pays de fabrication, entretien) ;
- par IA, une fois par modèle, stockées dans data/referentiel_qr.csv (jamais régénérées).
  statut : auto (publiée si QR_IA_PUBLICATION_AUTO), valide (publiée), brouillon / revue / rejete (non publiées).
Produits liés (recalculés chaque nuit, suivent le stock) :
- accessory : produits d'entretien pour les chaussures ;
- substitute : modèles proches (même famille, même genre, même taille en stock, prix voisin).
"""

import csv
import logging
import os
from collections import Counter, defaultdict
from datetime import date

import config
from common.flux import (est_chaussure, index_gtin_offer_id, lire_catalogue, source_disponible)
from common.gemini import Gemini, QuotaAtteint
from common.texte import (decouper_composition, faits_produit, justification_pays, nettoyer, pays_mentionnable,
                          termes_non_justifies)
from descriptions import COLS_DT, COLS_HL, lire_referentiel

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

N_QR = config.QR_IA_PAR_MODELE
COLS_REF_QR = (['parent', 'statut'] + [x for i in range(1, N_QR + 1) for x in (f'q{i}', f'r{i}')]
               + ['date_maj', 'essais_ia', 'note'])
MAX_QR_PAR_OFFRE = 30
MAX_CARACTERES_QR = 10000
PREPOSITION_PAYS = {'Portugal': 'au Portugal', 'Maroc': 'au Maroc', 'Bangladesh': 'au Bangladesh',
                    'Vietnam': 'au Vietnam', 'Pakistan': 'au Pakistan', 'Cambodge': 'au Cambodge',
                    'Sri Lanka': 'au Sri Lanka', 'Myanmar': 'au Myanmar', 'Hong Kong': 'à Hong Kong',
                    'Maurice': 'à Maurice', 'Madagascar': 'à Madagascar'}


# ─── RÉFÉRENTIEL Q&R ────────────────────────────────────────
def lire_ref_qr():
    if not os.path.exists(config.REF_QR):
        return {}
    with open(config.REF_QR, newline='', encoding='utf-8-sig') as f:
        return {l['parent']: {c: (l.get(c) or '').strip() for c in COLS_REF_QR}
                for l in csv.DictReader(f, delimiter=';') if l.get('parent')}


def ecrire_csv(chemin, cols, lignes, sep=';'):
    os.makedirs(os.path.dirname(chemin) or '.', exist_ok=True)
    with open(chemin, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=sep, extrasaction='ignore')
        w.writeheader()
        w.writerows(lignes)


def qr_publiables(l):
    if l['statut'] == 'valide' or (l['statut'] == 'auto' and config.QR_IA_PUBLICATION_AUTO):
        return [(l[f'q{i}'], l[f'r{i}']) for i in range(1, N_QR + 1) if l[f'q{i}'] and l[f'r{i}']]
    return []


# ─── Q&R PAR RÈGLES ─────────────────────────────────────────
def phrase_titre(t):
    t = (t or '').strip()
    return t[:1].upper() + t[1:].lower() if t else ''


def liste_fr(items):
    return items[0] if len(items) == 1 else ', '.join(items[:-1]) + ' et ' + items[-1]


def qr_regles(c, ligne_desc, autres_coloris, titres_entretien):
    qr = []
    if autres_coloris:
        qr.append(("Ce modèle existe-t-il dans d'autres coloris ?",
                   f"Oui, ce modèle est aussi proposé en {liste_fr(autres_coloris)}."))
    p = pays_mentionnable(c)
    deja_hl = any('fabriqu' in (ligne_desc or {}).get(h, '').lower() for h in COLS_HL)
    if p and not deja_hl and config.QR_PAYS_FABRICATION:
        qr.append(("Où ce produit est-il fabriqué ?",
                   f"Ce modèle est fabriqué {PREPOSITION_PAYS.get(p, 'en ' + p)}."))
    if est_chaussure(c) and titres_entretien:
        qr.append(("Comment entretenir ces chaussures ?",
                   f"Pour en prendre soin, TBS propose : {liste_fr(titres_entretien)}."))
    return qr


# ─── Q&R PAR IA ─────────────────────────────────────────────
def bloc_modele(parent, c_ref, contenus, coloris_noms):
    textes = []
    for l in contenus[:4]:   # 4 coloris suffisent pour décrire un modèle
        hl = '; '.join(l[h] for h in COLS_HL if l[h])
        dt = '; '.join(f"{l[f'detail_0{i}_attribut']} : {l[f'detail_0{i}_valeur']}"
                       for i in range(1, 7) if l[f'detail_0{i}_attribut'])
        textes.append(f"  - Description : {l['description']}\n    Points forts : {hl}\n    Détails : {dt}")
    return f"""### id : {parent}
Faits catalogue :
{faits_produit(c_ref)}
Coloris : {', '.join(coloris_noms)}
Contenus validés :
{chr(10).join(textes)[:2500]}"""


def prompt_lot_qr(blocs):
    return f"""Tu rédiges la FAQ de modèles TBS (marque française de chaussures et vêtements, univers bord de mer),
destinée aux assistants de recherche conversationnels. Une FAQ par modèle ci-dessous, chacun identifié par son id.

{chr(10).join(blocs)}

CONSIGNES (pour chaque modèle) :
- Jusqu'à {N_QR} questions qu'un client se pose réellement avant d'acheter ce type de produit
  (usage, saison, style, confort, assortiment, entretien), avec des réponses utiles.
- Chaque réponse s'appuie UNIQUEMENT sur les informations de CE modèle. Si l'information manque, ne pose pas la question.
- Ne recopie pas les points forts ou les détails mot pour mot : reformule pour répondre à la question.
- Interdit : prix, promotions, stock, délais de livraison, tailles disponibles, comparaison avec d'autres marques,
  toute caractéristique absente des informations (imperméabilité, certification, matière recyclée, lieu de fabrication…).
- Aucune performance absente des informations (adhérence, résistance, durabilité, maintien, légèreté…).
- Aucune allégation de santé ou de compatibilité médicale (semelles orthopédiques, podologie, douleurs, posture),
  même si une semelle est amovible : décris le fait (« semelle amovible »), jamais l'usage médical.
- Question : 120 caractères maximum. Réponse : 1 à 3 phrases, 400 caractères maximum. Français, ton simple.

Réponds uniquement en JSON, avec exactement un élément par id :
{{"modeles": [{{"id": "...", "qr": [{{"question": "...", "reponse": "..."}}]}}]}}"""


def generer_qr_ia(ref_qr, parents_stock, coloris, ref_desc, par_parent):
    gemini = Gemini(config.MAX_APPELS_IA_QR)
    aujourd_hui = date.today().isoformat()
    stats = Counter()
    a_traiter = [p for p, s in sorted(parents_stock.items(), key=lambda x: -x[1])
                 if p not in ref_qr or (ref_qr[p]['statut'] == 'echec' and int(ref_qr[p]['essais_ia'] or 0) < 2)]

    # Préparation : seuls les modèles qui ont du contenu validé partent à l'IA
    prets = []
    for parent in a_traiter:
        mpns = par_parent[parent]
        contenus = [ref_desc[m.upper()] for m in mpns if m.upper() in ref_desc
                    and (ref_desc[m.upper()]['description'] or ref_desc[m.upper()]['product_highlight_1'])]
        if not contenus:
            stats['sans_contenu'] += 1
            continue
        c_ref = max((coloris[m] for m in mpns), key=lambda c: c['_stock'])
        noms = sorted({coloris[m]['_nom_coloris'] for m in mpns if coloris[m]['_nom_coloris']})
        prets.append((parent, c_ref, contenus, noms))

    lot = config.LOT_QR
    for debut in range(0, len(prets), lot):
        paquet = prets[debut:debut + lot]
        if not gemini.disponible:
            stats['en_attente'] += len(paquet)
            continue
        try:
            rep = gemini.json(prompt_lot_qr([bloc_modele(*p) for p in paquet]))
        except QuotaAtteint:
            stats['en_attente'] += len(paquet)
            continue
        if rep is None:   # échec technique : ne consomme pas d'essai
            stats['en_attente'] += len(paquet)
            continue
        recus = {str(x.get('id', '')): x for x in (rep.get('modeles') or []) if isinstance(x, dict)}
        for parent, c_ref, contenus, noms in paquet:
            ligne = ref_qr.get(parent) or {c: '' for c in COLS_REF_QR}
            ligne.update(parent=parent, date_maj=aujourd_hui, note='',
                         essais_ia=str(int(ligne.get('essais_ia') or 0) + 1))
            paires = [(nettoyer(x.get('question'), 150), nettoyer(x.get('reponse'), 900))
                      for x in (recus.get(parent) or {}).get('qr', []) if isinstance(x, dict)]
            paires = [(q, r) for q, r in paires if q and r][:N_QR]
            if not paires:
                ligne['statut'] = 'echec'
                ref_qr[parent] = ligne
                stats['echec'] += 1
                continue
            for i in range(1, N_QR + 1):
                ligne[f'q{i}'], ligne[f'r{i}'] = paires[i - 1] if i <= len(paires) else ('', '')
            source = faits_produit(c_ref) + '\n' + justification_pays(c_ref) + '\n' + '\n'.join(
                ' '.join(l[c] for c in ['description'] + COLS_HL + COLS_DT) for l in contenus)
            douteux = termes_non_justifies(' '.join(q + ' ' + r for q, r in paires), source)
            if douteux:
                ligne['statut'], ligne['note'] = 'revue', 'termes non justifiés : ' + ', '.join(douteux)
                stats['revue'] += 1
            else:
                ligne['statut'] = 'auto' if config.QR_IA_PUBLICATION_AUTO else 'brouillon'
                stats['generees'] += 1
            ref_qr[parent] = ligne
    return stats, gemini.resume()


# ─── PRODUITS LIÉS ──────────────────────────────────────────
def construire_liens(skus, coloris, gtin_ok):
    """Pour chaque SKU : liste de (relation, gtin)."""
    en_stock = [r for r in skus if r['_stock'] > 0 and r.get('EAN') in gtin_ok]

    # Accessoires : un SKU en stock par MPN d'entretien
    acc_gtin = {}
    for r in en_stock:
        if r['MPN'] in config.ACCESSOIRES_CHAUSSURES + config.ACCESSOIRES_CHAUSSURES_CUIR:
            acc_gtin.setdefault(r['MPN'], r['EAN'])

    # Alternatives : index (famille, genre, taille) → SKU en stock
    idx = defaultdict(list)
    for r in en_stock:
        idx[(r.get('Famille commerciale'), r.get('Genre'), r.get('axis_size'))].append(r)

    liens = {}
    for r in skus:
        if r.get('EAN') not in gtin_ok:
            continue
        c = coloris[r['MPN']]
        l = []
        if est_chaussure(c):
            mpns_acc = list(config.ACCESSOIRES_CHAUSSURES)
            dessus = ' '.join(decouper_composition(c.get(f'Composition {i}'))[1]
                              for i in range(1, 6) if decouper_composition(c.get(f'Composition {i}'))[0] == 'Dessus')
            if 'cuir' in dessus.lower():
                mpns_acc += config.ACCESSOIRES_CHAUSSURES_CUIR
            l += [('accessory', acc_gtin[m]) for m in mpns_acc if m in acc_gtin][:config.MAX_ACCESSOIRES]
        prix = r['_prix']
        candidats = [x for x in idx.get((r.get('Famille commerciale'), r.get('Genre'), r.get('axis_size')), [])
                     if x['parent'] != r['parent'] and prix
                     and abs(x['_prix'] - prix) <= config.ECART_PRIX_ALTERNATIVE * prix]
        candidats.sort(key=lambda x: (x.get('Couleur de filtre', '') != r.get('Couleur de filtre', '')
                                      or not r.get('Couleur de filtre'), abs(x['_prix'] - prix), x['MPN']))
        vus = set()
        for x in candidats:
            if x['parent'] in vus:
                continue
            vus.add(x['parent'])
            l.append(('substitute', x['EAN']))
            if len(vus) >= config.MAX_ALTERNATIVES:
                break
        liens[r['EAN']] = l
    return liens


# ─── MAIN ───────────────────────────────────────────────────
def guillemets(s):
    return nettoyer(s).replace('"', "'")


def sous_attribut(valeur, maxi, toujours=False):
    """Valeur au format texte Google : entre guillemets si elle contient « : », « , » ou « " »
    (guillemets internes doublés). Sans guillemets, Google coupe la valeur à chaque virgule
    (ex. « 2,2 cm » devient deux valeurs → « nombre de sous-attributs insuffisant »)."""
    v = nettoyer(valeur, maxi)
    if toujours or any(ch in v for ch in ':,"'):
        return '"' + v.replace('"', '""') + '"'
    return v


def main():
    skus, coloris = lire_catalogue(source_disponible(config.FICHIER_CATALOGUE, config.URL_CATALOGUE))
    gtin_offre = index_gtin_offer_id(source_disponible(config.FICHIER_FLUX_GMC, config.URL_FLUX_GMC))
    ref_desc = lire_referentiel()
    ref_qr = lire_ref_qr()

    par_parent = defaultdict(list)
    for mpn, c in coloris.items():
        par_parent[c['parent']].append(mpn)
    parents_stock = Counter()
    for c in coloris.values():
        if c['_stock'] > 0:
            parents_stock[c['parent']] += c['_stock']

    # Q&R IA : uniquement les modèles en stock absents du référentiel
    stats_ia, resume_ia = generer_qr_ia(ref_qr, parents_stock, coloris, ref_desc, par_parent)
    ecrire_csv(config.REF_QR, COLS_REF_QR, sorted(ref_qr.values(), key=lambda l: l['parent']))

    titres_entretien = []
    for m in config.ACCESSOIRES_CHAUSSURES + config.ACCESSOIRES_CHAUSSURES_CUIR:
        t = coloris.get(m, {}).get('_titre', '')
        if t and coloris[m]['_stock'] > 0:
            titres_entretien.append(phrase_titre(t))

    liens = construire_liens(skus, coloris, set(gtin_offre))

    # Une ligne par offre GMC ; chaque attribut répété occupe autant de colonnes que nécessaire
    ATTRS = ['structured_description', 'product_highlight', 'product_detail', 'question_and_answer', 'related_product']
    lignes, stats = [], Counter()
    maxi = Counter()
    cache_mpn = {}
    for r in skus:
        offre = gtin_offre.get(r.get('EAN'))
        if not offre:
            continue
        mpn = r['MPN']
        if mpn not in cache_mpn:
            c = coloris[mpn]
            l_desc = ref_desc.get(mpn.upper())
            cellules = {a: [] for a in ATTRS}
            # Contenu éditorial (référentiel descriptions)
            if l_desc:
                if l_desc['description']:
                    type_source = '' if l_desc['origine'].startswith('manuel') else 'trained_algorithmic_media'
                    cellules['structured_description'] = [f"{type_source}:{sous_attribut(l_desc['description'], 5000, toujours=True)}"]
                cellules['product_highlight'] = [sous_attribut(l_desc[h], 150) for h in COLS_HL if l_desc[h]]
                cellules['product_detail'] = [
                    ':'.join(sous_attribut(l_desc[f'detail_0{i}_{x}'], 100) for x in ('section', 'attribut', 'valeur'))
                    for i in range(1, 7) if l_desc[f'detail_0{i}_attribut'] and l_desc[f'detail_0{i}_valeur']]
            # Q&R
            autres = sorted({coloris[m]['_nom_coloris'].lower() for m in par_parent[c['parent']]
                             if m != mpn and coloris[m]['_stock'] > 0 and coloris[m]['_nom_coloris']
                             and coloris[m]['_nom_coloris'].lower() not in ('sans couleur', c['_nom_coloris'].lower())})[:6]
            qr = qr_regles(c, l_desc, autres, titres_entretien)
            qr += qr_publiables(ref_qr[c['parent']]) if c['parent'] in ref_qr else []
            total = 0
            for q, rep in qr[:MAX_QR_PAR_OFFRE]:
                cell = f'"{guillemets(q)}":"{guillemets(rep)[:1000]}"'
                if total + len(cell) > MAX_CARACTERES_QR:
                    break
                cellules['question_and_answer'].append(cell)
                total += len(cell)
            cache_mpn[mpn] = cellules
        cellules = dict(cache_mpn[mpn])
        cellules['related_product'] = [f"{rel}:gtin:{g}" for rel, g in liens.get(r['EAN'], [])]
        if not any(cellules.values()):
            continue
        lignes.append((offre, cellules))
        for a in ATTRS:
            maxi[a] = max(maxi[a], len(cellules[a]))
        stats['offres'] += 1
        stats['avec_description'] += bool(cellules['structured_description'])
        stats['avec_qr'] += bool(cellules['question_and_answer'])
        stats['avec_liens'] += bool(cellules['related_product'])

    # Écriture TSV brute (format texte Google) : aucune tabulation ni retour à la ligne dans les valeurs
    os.makedirs(os.path.dirname(config.SORTIE_GMC), exist_ok=True)
    with open(config.SORTIE_GMC, 'w', encoding='utf-8', newline='') as f:
        f.write('\t'.join(['id'] + [a for a in ATTRS for _ in range(maxi[a])]) + '\n')
        for offre, cellules in lignes:
            valeurs = [offre]
            for a in ATTRS:
                valeurs += cellules[a] + [''] * (maxi[a] - len(cellules[a]))
            f.write('\t'.join(v.replace('\t', ' ').replace('\n', ' ') for v in valeurs) + '\n')

    ecrire_csv(os.path.join(config.RAPPORTS_DIR, 'qr_a_revoir.csv'), COLS_REF_QR,
               [l for l in ref_qr.values() if l['statut'] in ('revue', 'brouillon', 'echec')])

    statuts = Counter(l['statut'] for l in ref_qr.values())
    resume = (f"Enrichissement GMC — offres : {stats['offres']} (descriptions : {stats['avec_description']}, "
              f"Q&R : {stats['avec_qr']}, produits liés : "
              f"{stats['avec_liens']}) | Q&R IA cette nuit : {stats_ia['generees']} générées, {stats_ia['revue']} en revue, "
              f"{stats_ia['en_attente']} en attente | référentiel Q&R : {dict(statuts)}\n\n{resume_ia}")
    log.info(f"✅ {resume}")
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as f:
            f.write(f"### Job conversationnel\n{resume}\n")


if __name__ == '__main__':
    main()
