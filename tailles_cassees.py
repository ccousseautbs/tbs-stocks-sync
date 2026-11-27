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

# --- À déplacer dans config.py -------------------------------------------
TC_MIN_TAILLES = 2
TC_NB_EXTREMES = 2            # tailles "extrêmes" de chaque côté, grilles longues
TC_NB_EXTREMES_PETITE = 1     # idem, grilles courtes
TC_SEUIL_GRILLE_LONGUE = 8
TC_GRILLE_MIN_REGLE_2 = 4     # en dessous, seule la règle 1 s'applique
TC_CHAMP_LABEL = "custom label 1"
TC_FICHIER_SORTIE = "tailles_cassees_gmc.tsv"
# Noms des champs dans le cache du flux (à ajuster)
TC_COL_ID = "id"
TC_COL_GROUPE = "item_group_id"
TC_COL_TAILLE = "size"
TC_COL_DISPO = "availability"
# ---------------------------------------------------------------------------

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
    return str(valeur).strip().lower().replace("_", " ") == "in stock"


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


def run(lignes_flux, dossier_sortie="."):
    """lignes_flux : itérable de dicts issus du cache du job 1."""
    variantes = [
        {
            "id": l[TC_COL_ID],
            "groupe": l.get(TC_COL_GROUPE) or l[TC_COL_ID],
            "taille": l.get(TC_COL_TAILLE, ""),
            "dispo": est_dispo(l.get(TC_COL_DISPO, "")),
        }
        for l in lignes_flux
        if l.get(TC_COL_ID)
    ]
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


def charger_flux(dossier="cache"):
    """Cherche dans le cache le fichier qui contient id + disponibilité + taille."""
    import os
    requis = {TC_COL_ID, TC_COL_DISPO, TC_COL_TAILLE}
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
            if lignes and requis <= set(lignes[0].keys()):
                print(f"Flux utilisé : {chemin} ({len(lignes)} lignes)")
                return lignes
            print(f"Ignoré : {chemin} — colonnes : {sorted(lignes[0].keys())[:15] if lignes else 'vide'}")
    raise SystemExit(f"Aucun fichier du cache ne contient les colonnes {sorted(requis)} : ajuster TC_COL_*")


if __name__ == "__main__":
    import os
    os.makedirs("docs", exist_ok=True)
    run(charger_flux("cache"), "docs")
