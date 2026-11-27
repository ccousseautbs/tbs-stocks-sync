"""
tailles_cassees.py — job du workflow tbs-stocks-sync

Lit le flux (cache partagé du job 1), détecte les modèles à tailles cassées
et produit une source supplémentaire GMC : tailles_cassees_gmc.tsv (id, custom label 1).

Règles, par modèle (item_group_id) :
  1. moins de MIN_TAILLES tailles en stock                  -> 'une_taille'
  2. toutes les tailles en stock sont en bout de grille       -> 'tailles_extremes'
Les autres variantes reçoivent 'ok' : un modèle réassorti retrouve sa diffusion
au passage suivant.

Réglages à déplacer dans config.py (préfixe TC_).
"""
import csv
from collections import defaultdict

ORDRE_LETTRES = ["XXS", "XS", "S", "M", "L", "XL", "XXL", "3XL", "4XL"]
ALIAS = {"2XL": "XXL", "XXXL": "3XL"}


def rang_taille(size):
    s = str(size).strip().upper().replace(",", ".")
    s = ALIAS.get(s, s)
    try:
        return float(s)
    except ValueError:
        return 1000 + ORDRE_LETTRES.index(s) if s in ORDRE_LETTRES else None


def est_dispo(valeur):
    """Accepte un statut texte (in stock, in_stock, disponible…) ou une quantité (> 0)."""
    v = str(valeur).strip().lower().replace("_", " ")
    if v in ("in stock", "instock", "disponible", "en stock", "true", "oui", "yes"):
        return True
    try:
        return float(v.replace(",", ".")) > 0
    except ValueError:
        return False


def _norm(nom):
    n = str(nom).strip().lower()
    return n[2:] if n.startswith("g:") else n


def _trouver(colonnes, candidats):
    """Renvoie le nom réel de la première colonne qui correspond à un candidat."""
    if isinstance(candidats, str):
        candidats = [candidats]
    index = {_norm(c): c for c in colonnes}
    for cand in candidats:
        if _norm(cand) in index:
            return index[_norm(cand)]
    return None


def detecter(variantes):
    """variantes : liste de dicts {id, groupe, taille, dispo} -> {id: label}"""
    modeles = defaultdict(list)
    for v in variantes:
        modeles[v["groupe"]].append(v)

    labels = {v["id"]: "ok" for v in variantes}
    for vars_ in modeles.values():
        grille = sorted({r for r in (rang_taille(v["taille"]) for v in vars_) if r is not None})
        dispo = [v for v in vars_ if v["dispo"]]
        if len(grille) < 2 or not dispo:
            continue  # taille unique / non reconnue, ou modèle déjà entièrement en rupture

        motif = None
        if len(dispo) < TC_MIN_TAILLES:
            motif = "une_taille"
        elif len(grille) >= TC_GRILLE_MIN_REGLE_2:
            n = TC_NB_EXTREMES if len(grille) >= TC_SEUIL_GRILLE_LONGUE else TC_NB_EXTREMES_PETITE
            extremes = set(grille[:n] + grille[-n:])
            if all(rang_taille(v["taille"]) in extremes for v in dispo):
                motif = "tailles_extremes"

        if motif:
            for v in dispo:
                labels[v["id"]] = motif
    return labels


def run(lignes_flux, dossier_sortie=".", colonnes=None):
    """lignes_flux : liste de dicts issus du cache du job 1."""
    colonnes = colonnes or _colonnes(lignes_flux[0].keys())
    c_id, c_grp, c_tai, c_dis = colonnes
    variantes = [
        {
            "id": l[c_id],
            "groupe": (l.get(c_grp) if c_grp else None) or l[c_id],
            "taille": l.get(c_tai, ""),
            "dispo": est_dispo(l.get(c_dis, "")),
        }
        for l in lignes_flux
        if l.get(c_id)
    ]
    if not c_grp:
        print("⚠️ Pas de colonne modèle (item_group_id) : chaque variante est traitée seule")
    labels = detecter(variantes)

    with open(f"{dossier_sortie}/{TC_FICHIER_SORTIE}", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["id", TC_CHAMP_LABEL])
        w.writerows(labels.items())

    compte = defaultdict(int)
    for lab in labels.values():
        compte[lab] += 1
    # Résumé pour l'onglet Actions
    print(f"Tailles cassées — variantes : {len(labels)} | " +
          " | ".join(f"{k} : {v}" for k, v in sorted(compte.items())))
    return labels


# --- Lecture du cache du job 1 -----------------------------------------------
def _lire_fichier(chemin):
    """Renvoie une liste de dicts pour un fichier CSV/TSV/TXT ou XML (flux Google)."""
    if chemin.endswith(".xml"):
        import xml.etree.ElementTree as ET
        lignes = []
        for item in ET.parse(chemin).getroot().iter():
            if item.tag.split("}")[-1] not in ("item", "entry"):
                continue
            # On retire le préfixe d'espace de noms : {http://base.google.com/ns/1.0}id -> id
            lignes.append({c.tag.split("}")[-1]: (c.text or "").strip() for c in item})
        return lignes
    with open(chemin, encoding="utf-8-sig", newline="") as f:
        echantillon = f.read(20000)
        f.seek(0)
        try:
            dialecte = csv.Sniffer().sniff(echantillon, delimiters="\t;,|")
        except csv.Error:
            dialecte = csv.excel_tab
        return list(csv.DictReader(f, dialect=dialecte))


def _colonnes(noms):
    noms = list(noms)
    return (_trouver(noms, TC_COL_ID), _trouver(noms, TC_COL_GROUPE),
            _trouver(noms, TC_COL_TAILLE), _trouver(noms, TC_COL_DISPO))


MOTS_CLES = ("id", "group", "parent", "size", "taille", "point", "avail", "dispo", "stock", "quant")


def charger_flux(dossier="cache"):
    """Cherche dans le cache le fichier qui contient id + disponibilité + taille."""
    import os
    for racine, _, fichiers in os.walk(dossier):
        for nom in sorted(fichiers):
            if not nom.lower().endswith((".csv", ".tsv", ".txt", ".xml")):
                continue
            chemin = os.path.join(racine, nom)
            try:
                lignes = _lire_fichier(chemin)
            except Exception as e:
                print(f"Lecture impossible : {chemin} ({e})")
                continue
            if not lignes:
                print(f"Ignoré : {chemin} — vide")
                continue
            cols = _colonnes(lignes[0].keys())
            if cols[0] and cols[2] and cols[3]:
                print(f"Flux utilisé : {chemin} ({len(lignes)} lignes) — "
                      f"id={cols[0]} | modèle={cols[1]} | taille={cols[2]} | dispo={cols[3]}")
                return lignes, cols
            utiles = [c for c in lignes[0].keys() if any(m in c.lower() for m in MOTS_CLES)]
            print(f"Ignoré : {chemin} — {len(lignes[0])} colonnes ; trouvées : "
                  f"id={cols[0]} | modèle={cols[1]} | taille={cols[2]} | dispo={cols[3]}")
            print(f"   colonnes pertinentes : {utiles}")
    raise SystemExit("Aucun fichier du cache ne contient id + taille + disponibilité : "
                     "ajouter le bon nom en tête de TC_COL_* (voir 'colonnes pertinentes' ci-dessus)")


if __name__ == "__main__":
    import os
    os.makedirs("docs", exist_ok=True)
    lignes, cols = charger_flux("cache")
    run(lignes, "docs", cols)
