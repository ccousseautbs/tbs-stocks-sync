"""
CONFIGURATION CENTRALE — dépôt tbs-stocks-sync

Tous les réglages modifiables sont ici. Les scripts n'ont pas besoin d'être touchés
pour changer une URL, un seuil, le modèle Gemini ou les règles d'accessoires.
"""

# ─── SOURCES ────────────────────────────────────────────────
URL_FLUX_GMC     = 'https://feeds.lengow.io/3/jzdj298'          # flux canal Google (séparateur |)
URL_CATALOGUE    = 'https://tbs.fr/Storage/Lengow/Lengow.csv'   # catalogue Lengow (séparateur ,)
URL_STOCKS       = 'https://tbs.fr/Storage/Lengow/stocks.csv'   # export stocks boutiques (séparateur ;)

# Copies locales produites par le job 1 et partagées avec les autres jobs
CACHE_DIR            = 'cache'
FICHIER_FLUX_GMC     = 'cache/flux_gmc.csv'
FICHIER_CATALOGUE    = 'cache/catalogue.csv'

# ─── SORTIES PUBLIÉES (GitHub Pages) ────────────────────────
SORTIE_DIR               = 'docs'
SORTIE_DESCRIPTIONS      = 'docs/tbs_source_descriptions_site.csv'   # source additionnelle Lengow
SORTIE_CONVERSATIONNEL   = 'docs/conversationnel_gmc.tsv'            # source supplémentaire GMC
RAPPORTS_DIR             = 'docs/rapports'

# ─── RÉFÉRENTIELS VERSIONNÉS (commités par le workflow) ─────
REF_DESCRIPTIONS = 'data/referentiel_descriptions.csv'
REF_QR           = 'data/referentiel_qr.csv'

# ─── GÉNÉRATION IA (Gemini) ─────────────────────────────────
# Sans secret GEMINI_API_KEY, les scripts tournent quand même :
# héritage + règles, et les manques partent dans le rapport « à rédiger ».
GEMINI_MODELE              = 'gemini-3.8-flash'   # à ajuster selon les modèles disponibles en gratuit
GEMINI_PAUSE_SECONDES      = 7      # temporisation entre deux appels (niveau gratuit ≈ 10 req/min)
MAX_APPELS_IA_DESCRIPTIONS = 150    # plafond par nuit, job descriptions (prioritaire)
MAX_APPELS_IA_QR           = 150    # plafond par nuit, job Q&R

# Q&R générées par IA : publiées automatiquement si les garde-fous passent.
# Mettre False pour exiger une validation (statut 'valide') dans data/referentiel_qr.csv.
QR_IA_PUBLICATION_AUTO = True
QR_IA_PAR_MODELE       = 3      # nombre de Q&R spécifiques demandées par modèle
QR_PAYS_FABRICATION    = True   # Q&R « Où ce produit est-il fabriqué ? » (pays issu du catalogue)

# ─── PÉRIMÈTRE ──────────────────────────────────────────────
SEULEMENT_EN_STOCK = True   # ne génère du contenu que pour les coloris avec du stock

# ─── GARDE-FOUS ─────────────────────────────────────────────
# Si un texte généré contient l'un de ces termes alors qu'aucune donnée source
# ne le justifie, le contenu n'est pas publié et part en revue.
TERMES_SENSIBLES = [
    'imperméable', 'étanche', 'déperlant', 'made in france', 'fabriqué en france',
    'fabriquée en france', 'fabriqués en france', 'fabriquées en france',
    'leather working group', 'certifié', 'certifiée', 'orthopédique', 'vegan',
    'recyclé', 'recyclée', 'bio', 'biologique', 'éco-responsable', 'écoresponsable',
    'garanti', 'garantie', 'anti-dérapant', 'antidérapant',
]

# Highlight « Fabriqué en … » généré par règle uniquement pour ces pays
PAYS_HIGHLIGHT = {'FR', 'IT', 'PT', 'ES'}

PAYS = {
    'CN': 'Chine', 'VN': 'Vietnam', 'MA': 'Maroc', 'BD': 'Bangladesh', 'IN': 'Inde',
    'PT': 'Portugal', 'FR': 'France', 'TN': 'Tunisie', 'TR': 'Turquie', 'ES': 'Espagne',
    'IT': 'Italie', 'RO': 'Roumanie', 'AL': 'Albanie', 'MY': 'Malaisie', 'TH': 'Thaïlande',
    'HK': 'Hong Kong', 'PK': 'Pakistan', 'KH': 'Cambodge', 'ID': 'Indonésie', 'MM': 'Myanmar',
    'LK': 'Sri Lanka', 'BG': 'Bulgarie', 'MG': 'Madagascar', 'MU': 'Maurice', 'EG': 'Égypte',
}
# Codes non géographiques ou ambigus : aucune mention de pays
PAYS_IGNORES = {'EU', 'MO'}

# ─── PRODUITS LIÉS ──────────────────────────────────────────
# Produits d'entretien proposés en accessoire (MPN), à valider.
ACCESSOIRES_CHAUSSURES      = ['ECOCLEAN01000', 'ECOPROT-01000']   # toutes les chaussures
ACCESSOIRES_CHAUSSURES_CUIR = ['FAMAECO01000']                      # en plus si dessus en cuir
MAX_ACCESSOIRES  = 3
MAX_ALTERNATIVES = 3
ECART_PRIX_ALTERNATIVE = 0.25   # ±25 % autour du prix du produit
