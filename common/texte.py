"""Mise en forme des champs catalogue et garde-fous sur les textes générés."""

import re
import unicodedata

import config


def _minuscule_matiere(s):
    s = s.strip()
    return s[:1].lower() + s[1:] if s and not s[:2].isupper() else s.lower()


def format_composition(valeur):
    """'97%Coton,3%Elasthanne' → '97 % coton, 3 % élasthanne' ; 'Caoutchouc,Synthétique' → 'Caoutchouc, synthétique'."""
    v = (valeur or '').strip()
    if not v:
        return ''
    v = v.replace('Elasthanne', 'élasthanne')
    parts = re.findall(r'(\d+(?:[.,]\d+)?)\s*%\s*([^,%\d]+)', v)
    if parts:
        return ', '.join(f"{p} % {_minuscule_matiere(m).strip()}" for p, m in parts)
    items = [i.strip() for i in v.split(',') if i.strip()]
    if not items:
        return ''
    return ', '.join([items[0]] + [_minuscule_matiere(i) for i in items[1:]])


def decouper_composition(champ):
    """'Dessus:Cuir' → ('Dessus', 'Cuir') ; 'Non doublé:' → ('Doublure', 'Non doublée') ;
    'Matière principale100%Coton' → ('Matière principale', '100%Coton') ; '100%Coton' → ('', '100%Coton')."""
    c = (champ or '').strip()
    if not c:
        return '', ''
    if c.lower().startswith('non doubl'):
        return 'Doublure', 'Non doublée'
    if ':' in c:
        lib, val = c.split(':', 1)
    else:
        m = re.match(r'^([^\d]+?)\s*(\d.*)$', c)
        lib, val = (m.group(1), m.group(2)) if m else ('', c)
    lib = lib.split('/')[0].strip()   # 'Rembourrage / Padding' → 'Rembourrage'
    return lib, val.strip()


def hauteur_talon_cm(valeur):
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*mm', valeur or '')
    if not m:
        return ''
    cm = float(m.group(1).replace(',', '.')) / 10
    return (f"{cm:.1f}".rstrip('0').rstrip('.') + ' cm').replace('.', ',')


def sans_prefixe(valeur, prefixe):
    """'Fermeture lacets' → 'lacets' ; 'Coupe regular' → 'regular'."""
    v = (valeur or '').strip()
    return v[len(prefixe):].strip() if v.lower().startswith(prefixe.lower()) else v


def pays(code):
    code = (code or '').strip().upper()
    if not code or code in config.PAYS_IGNORES:
        return ''
    return config.PAYS.get(code, '')


def pays_mentionnable(c):
    """Pays affiché dans les contenus uniquement s'il fait partie de config.PAYS_HIGHLIGHT (pays européens)."""
    code = (c.get('Pays de fabrication') or '').strip().upper()
    return pays(code) if code in config.PAYS_HIGHLIGHT else ''


def justification_pays(c):
    """Formulations autorisées par le pays de fabrication (évite qu'un « fabriqué en France » exact parte en revue)."""
    p = pays_mentionnable(c).lower()
    if not p:
        return ''
    formes = [f"{v} {prep} {p}" for v in ('fabriqué', 'fabriquée', 'fabriqués', 'fabriquées', 'confectionné',
                                         'confectionnée', 'made') for prep in ('en', 'au', 'in')]
    return ' '.join(formes)


def faits_produit(c):
    """Texte factuel (catalogue) servant de base aux prompts et aux garde-fous."""
    lignes = []
    champs = ['Famille commerciale', 'Sous Famille commerciale', 'Genre', 'Hauteur de talon chaussures',
              'Fermeture chaussures', 'Coupe textile', 'Fermeture textile', 'Col textile',
              'Manches textile', 'Style', 'Finitions textile', 'Capuche textile', 'Doublure textile',
              'Poches textile', 'Ceinture textile']
    for ch in champs:
        if c.get(ch):
            lignes.append(f"{ch} : {c[ch]}")
    for i in range(1, 6):
        lib, val = decouper_composition(c.get(f'Composition {i}'))
        if val:
            lignes.append(f"Composition{' ' + lib if lib else ''} : {format_composition(val) or val}")
    p = pays_mentionnable(c)
    if p:
        lignes.append(f"Pays de fabrication : {p}")
    if c.get('_nom_coloris'):
        lignes.append(f"Coloris : {c['_nom_coloris']}")
    return '\n'.join(lignes)


def _norm(s):
    return unicodedata.normalize('NFC', (s or '').lower())


def termes_non_justifies(texte_genere, texte_source):
    """Termes sensibles présents dans le texte généré mais absents des données source."""
    g, s = _norm(texte_genere), _norm(texte_source)
    return [t for t in config.TERMES_SENSIBLES
            if re.search(r'\b' + re.escape(t) + r'\b', g) and not re.search(r'\b' + re.escape(t) + r'\b', s)]


def nettoyer(s, maxi=None):
    s = re.sub(r'\s+', ' ', (s or '').replace('\t', ' ')).strip()
    return s[:maxi].rsplit(' ', 1)[0] if maxi and len(s) > maxi else s
