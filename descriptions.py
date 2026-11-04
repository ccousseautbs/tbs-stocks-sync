"""
JOB 3 — SOURCE DESCRIPTIONS (remplace l'import manuel Tbs-source-descriptions-site dans Lengow)

Produit docs/tbs_source_descriptions_site.csv au même format que le fichier importé à la main
(mêmes colonnes, séparateur ;, clé item_group_id = MPN coloris) : aucun mapping Lengow à refaire.

Principe incrémental : data/referentiel_descriptions.csv conserve tout le contenu déjà produit.
- Une ligne existante n'est jamais régénérée (correction manuelle = protégée).
- Nouveau coloris d'un modèle connu  → highlights + details hérités des coloris frères.
- Nouveau modèle                     → highlights + details construits par règles (champs catalogue).
- Description manquante              → générée par Gemini si la clé existe, sinon listée « à rédiger ».
"""

import csv
import logging
import os
import re
from collections import Counter, defaultdict
from datetime import date

import config
from common.flux import est_chaussure, lire_catalogue, source_disponible
from common.gemini import Gemini, QuotaAtteint
from common.texte import (decouper_composition, faits_produit, format_composition, hauteur_talon_cm,
                          nettoyer, pays, sans_prefixe, termes_non_justifies)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

COLS_HL = [f'product_highlight_{i}' for i in range(1, 7)]
COLS_DT = [f'detail_0{i}_{s}' for i in range(1, 7) for s in ('section', 'attribut', 'valeur')]
COLS_SORTIE = ['item_group_id', 'description'] + COLS_HL + ['pattern'] + COLS_DT
COLS_REF = COLS_SORTIE + ['origine', 'date_maj', 'essais_ia']
MAX_ESSAIS_IA = 2

MOTIFS = [('MARINI', 'Marinière'), ('RAY', 'Rayé'), ('CARREAU', 'À carreaux'), ('CHINE', 'Chiné'),
          ('CAMO', 'Camouflage'), ('LEOPARD', 'Léopard'), ('PYTHON', 'Python'), ('CROCO', 'Croco'),
          ('FLEUR', 'Fleuri'), ('IMPRIM', 'Imprimé'), ('CHEVRON', 'Chevrons')]

FERMETURES = [('lacets + zip', 'Fermeture à lacets et zip'), ('élastique + zip', 'Fermeture élastiquée et zip'),
              ('lacets élastiques', 'Lacets élastiques'), ('lacets', 'Fermeture à lacets'),
              ('velcro', 'Fermeture à scratch'), ('boucle', 'Fermeture à boucle'),
              ('élastique', 'Fermeture élastiquée'), ('zip', 'Fermeture zippée')]

PREPOSITION_PAYS = {'Portugal': 'au Portugal'}


# ─── RÉFÉRENTIEL ────────────────────────────────────────────
def lire_referentiel():
    if not os.path.exists(config.REF_DESCRIPTIONS):
        raise FileNotFoundError(f"{config.REF_DESCRIPTIONS} introuvable : il doit être initialisé dans le dépôt.")
    with open(config.REF_DESCRIPTIONS, newline='', encoding='utf-8-sig') as f:
        lignes = list(csv.DictReader(f, delimiter=';'))
    ref = {}
    for l in lignes:
        l = {c: (l.get(c) or '').strip() for c in COLS_REF}
        if l['item_group_id']:
            ref[l['item_group_id'].upper()] = l
    log.info(f"Référentiel descriptions : {len(ref)} coloris")
    return ref


def ecrire_csv(chemin, cols, lignes):
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    with open(chemin, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=';', extrasaction='ignore')
        w.writeheader()
        w.writerows(lignes)


# ─── RÈGLES ─────────────────────────────────────────────────
def details_par_regles(c):
    matieres, caract = [], []
    chaussure = est_chaussure(c)
    for i in range(1, 6):
        lib, val = decouper_composition(c.get(f'Composition {i}'))
        if not val:
            continue
        v = format_composition(val) if val != 'Non doublée' else val
        if not lib or lib.lower().startswith('matière principale'):
            lib = 'Composition'
        matieres.append(('Matières', lib, v[:1].upper() + v[1:] if chaussure else v))
    if chaussure:
        talon = hauteur_talon_cm(c.get('Hauteur de talon chaussures'))
        if talon:
            caract.append(('Caractéristiques', 'Hauteur du talon', talon))
        ferm = sans_prefixe(c.get('Fermeture chaussures'), 'Fermeture')
        if ferm:
            caract.append(('Caractéristiques', 'Fermeture', ferm[:1].upper() + ferm[1:]))
    else:
        for champ, attr, prefixe in [('Coupe textile', 'Coupe', 'Coupe'), ('Col textile', 'Col', ''),
                                     ('Manches textile', 'Manches', ''), ('Fermeture textile', 'Fermeture', ''),
                                     ('Style', 'Style', 'Style')]:
            v = sans_prefixe(c.get(champ), prefixe) if prefixe else (c.get(champ) or '').strip()
            if v:
                caract.append(('Caractéristiques', attr, v[:1].upper() + v[1:]))
    # Matières d'abord, 6 détails maximum, en gardant au moins 2 caractéristiques si possible
    garde_caract = min(len(caract), max(2, 6 - len(matieres)))
    return (matieres[: 6 - garde_caract] + caract[:garde_caract])[:6]


def highlights_par_regles(c):
    hl = []
    if est_chaussure(c):
        for i in range(1, 6):
            lib, val = decouper_composition(c.get(f'Composition {i}'))
            v = format_composition(val).lower() if val else ''
            if lib == 'Dessus' and v:
                hl.append(f"Dessus en {v.replace(', ', ' et ')}")
            elif lib == 'Semelle intérieure' and v:
                hl.append(f"Semelle intérieure en {v.replace(', ', ' et ')}")
            elif lib == 'Semelle' and 'caoutchouc' in v:
                hl.append("Semelle en caoutchouc")
        talon = hauteur_talon_cm(c.get('Hauteur de talon chaussures'))
        if talon:
            hl.append(f"Talon de {talon}")
        ferm = (c.get('Fermeture chaussures') or '').lower()
        for cle, texte in FERMETURES:
            if cle in ferm:
                hl.append(texte)
                break
    else:
        for i in range(1, 6):
            lib, val = decouper_composition(c.get(f'Composition {i}'))
            if val and (not lib or lib.lower().startswith('matière principale')):
                hl.append(f"Composition : {format_composition(val)}")
                break
        for champ in ['Coupe textile', 'Col textile', 'Manches textile', 'Style']:
            if c.get(champ):
                hl.append(c[champ].strip())
    code_pays = (c.get('Pays de fabrication') or '').upper()
    if code_pays in config.PAYS_HIGHLIGHT and pays(code_pays):
        p = pays(code_pays)
        hl.insert(0, f"Fabriqué {PREPOSITION_PAYS.get(p, 'en ' + p)}")
    return [nettoyer(h, 150) for h in hl][:6]


def motif_par_regles(c):
    texte = ' '.join([c.get('axis_color', ''), c.get('Titre du produit', '')]).upper()
    for cle, motif in MOTIFS:
        if cle in texte:
            return motif
    return ''


def appliquer(ligne, highlights, details):
    for i, col in enumerate(COLS_HL):
        ligne[col] = highlights[i] if i < len(highlights) else ''
    for i in range(6):
        s, a, v = details[i] if i < len(details) else ('', '', '')
        ligne[f'detail_0{i + 1}_section'], ligne[f'detail_0{i + 1}_attribut'], ligne[f'detail_0{i + 1}_valeur'] = s, a, v


def contenu_herite(freres):
    """Combinaison highlights + details la plus fréquente parmi les coloris frères."""
    cle = lambda l: tuple(l[c] for c in COLS_HL + COLS_DT)
    compte = Counter(cle(l) for l in freres if any(l[c] for c in COLS_HL + COLS_DT))
    if not compte:
        return None
    valeurs = compte.most_common(1)[0][0]
    return dict(zip(COLS_HL + COLS_DT, valeurs))


# ─── GÉNÉRATION IA ──────────────────────────────────────────
def bloc_produit(c, freres, exemples):
    ref_freres = '\n'.join(f"  - {l['description']}" for l in freres if l['description'])[:700]
    ref_exemples = '\n'.join(f"  - {d}" for d in exemples)[:400]
    return f"""### id : {c['MPN']}
Faits produit (seule source autorisée pour les caractéristiques) :
{faits_produit(c)}
Descriptions validées des autres coloris du même modèle :
{ref_freres or '  (aucune)'}
Exemples de ton (même famille) :
{ref_exemples or '  (aucun)'}"""


def prompt_lot_descriptions(items):
    blocs = '\n\n'.join(bloc_produit(c, f, e) for c, f, e in items)
    return f"""Tu rédiges les descriptions produit d'articles TBS (marque française de chaussures et vêtements, univers bord de mer).
Une description par produit ci-dessous, chacun identifié par son id.

{blocs}

CONSIGNES (pour chaque produit) :
- 2 à 3 phrases, 150 à 350 caractères, en français, ton des exemples.
- Mentionne le coloris et le type de produit.
- N'invente aucune caractéristique absente des faits ou des descriptions du même modèle
  (imperméabilité, certification, lieu de fabrication, technologie de semelle, matière recyclée…).
- Pas de prix, pas de promotion, pas de majuscules d'insistance.
- Chaque description ne parle que de son produit.

Réponds uniquement en JSON, avec exactement un élément par id :
{{"produits": [{{"id": "...", "description": "..."}}]}}"""


def main():
    ref = lire_referentiel()
    _, coloris = lire_catalogue(source_disponible(config.FICHIER_CATALOGUE, config.URL_CATALOGUE))
    aujourd_hui = date.today().isoformat()

    par_parent = defaultdict(list)
    for mpn, c in coloris.items():
        par_parent[c['parent']].append(mpn.upper())
    par_famille = defaultdict(list)
    for mpn, c in coloris.items():
        l = ref.get(mpn.upper())
        if l and l['description']:
            par_famille[c.get('Famille commerciale', '')].append(l['description'])

    perimetre = {m.upper(): c for m, c in coloris.items()
                 if c['_stock'] > 0 or not config.SEULEMENT_EN_STOCK}
    stats = Counter()

    # 1. Nouveaux coloris : héritage ou règles
    for cle, c in sorted(perimetre.items(), key=lambda x: -x[1]['_stock']):
        if cle in ref:
            continue
        ligne = {col: '' for col in COLS_REF}
        ligne.update(item_group_id=c['MPN'], date_maj=aujourd_hui, essais_ia='0')
        freres = [ref[m] for m in par_parent[c['parent']] if m in ref]
        herite = contenu_herite(freres)
        if herite:
            ligne.update(herite)
            ligne['origine'] = 'heritage'
        else:
            appliquer(ligne, highlights_par_regles(c), details_par_regles(c))
            ligne['origine'] = 'regles'
        ligne['pattern'] = motif_par_regles(c)
        ref[cle] = ligne
        stats[ligne['origine']] += 1

    # 2. Descriptions manquantes (nouveaux coloris d'abord, puis anciens trous), par stock décroissant
    gemini = Gemini(config.MAX_APPELS_IA_DESCRIPTIONS)
    a_rediger, revue = [], []
    manquantes = [(cle, c) for cle, c in perimetre.items()
                  if not ref[cle]['description'] and int(ref[cle]['essais_ia'] or 0) < MAX_ESSAIS_IA]
    manquantes.sort(key=lambda x: (ref[x[0]]['origine'] == 'import_initial', -x[1]['_stock']))
    lot = config.LOT_DESCRIPTIONS
    for debut in range(0, len(manquantes), lot):
        paquet = manquantes[debut:debut + lot]
        if not gemini.disponible:
            a_rediger += [c for _, c in paquet]
            continue
        items, contexte = [], {}
        for cle, c in paquet:
            freres = [ref[m] for m in par_parent[c['parent']] if m in ref and m != cle]
            exemples = par_famille.get(c.get('Famille commerciale', ''), [])[:1]
            items.append((c, freres, exemples))
            contexte[c['MPN'].upper()] = (cle, c, freres)
        try:
            rep = gemini.json(prompt_lot_descriptions(items))
        except QuotaAtteint:
            a_rediger += [c for _, c in paquet]
            continue
        if rep is None:   # échec technique : ne consomme pas d'essai
            a_rediger += [c for _, c in paquet]
            continue
        recus = {str(x.get('id', '')).upper(): x for x in (rep.get('produits') or []) if isinstance(x, dict)}
        for id_maj, (cle, c, freres) in contexte.items():
            ligne = ref[cle]
            ligne['essais_ia'] = str(int(ligne['essais_ia'] or 0) + 1)
            desc = nettoyer((recus.get(id_maj) or {}).get('description', ''), 1500)
            if not desc:
                a_rediger.append(c)
                continue
            source = faits_produit(c) + '\n' + '\n'.join(l['description'] + ' ' + ' '.join(l[h] for h in COLS_HL)
                                                         for l in freres)
            douteux = termes_non_justifies(desc, source)
            if douteux:
                revue.append({'MPN': c['MPN'], 'texte_propose': desc, 'termes_non_justifies': ', '.join(douteux)})
                stats['revue'] += 1
                continue
            ligne['description'] = desc
            ligne['date_maj'] = aujourd_hui
            if ligne['origine'] in ('heritage', 'regles'):
                ligne['origine'] += '+ia'
            stats['description_ia'] += 1

    # 3. Écritures
    lignes = sorted(ref.values(), key=lambda l: l['item_group_id'])
    ecrire_csv(config.REF_DESCRIPTIONS, COLS_REF, lignes)
    ecrire_csv(config.SORTIE_DESCRIPTIONS, COLS_SORTIE, lignes)
    ecrire_csv(os.path.join(config.RAPPORTS_DIR, 'descriptions_a_rediger.csv'),
               ['MPN', 'parent', 'Famille commerciale', 'coloris', 'stock'],
               [{'MPN': c['MPN'], 'parent': c['parent'], 'Famille commerciale': c.get('Famille commerciale', ''),
                 'coloris': c['_nom_coloris'], 'stock': c['_stock']} for c in a_rediger])
    ecrire_csv(os.path.join(config.RAPPORTS_DIR, 'descriptions_revue.csv'),
               ['MPN', 'texte_propose', 'termes_non_justifies'], revue)

    resume = (f"Descriptions — nouveaux coloris : {stats['heritage']} hérités, {stats['regles']} par règles | "
              f"descriptions IA : {stats['description_ia']} ({gemini.appels} appels) | "
              f"en revue : {stats['revue']} | à rédiger : {len(a_rediger)} | référentiel : {len(lignes)}")
    log.info(f"✅ {resume}")
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as f:
            f.write(f"### Job descriptions\n{resume}\n")


if __name__ == '__main__':
    main()
