# tbs-stocks-sync — flux GMC et Lengow TBS

Workflow nocturne (3h, heure de Paris) en 5 jobs :

| Job | Script | Sortie publiée sur Pages | Destination |
|---|---|---|---|
| 1. Lecture flux | `fetch_flux.py` | — (cache partagé) | — |
| 2. Stocks magasin | `stocks_sync.py` | `inventaire_magasin.csv`, `exclusions_locales_gmc.csv` | GMC |
| 3. Descriptions | `descriptions.py` | `tbs_source_descriptions_site.csv` | Lengow (source additionnelle) |
| 4. Conversationnel | `conversationnel.py` | `conversationnel_gmc.tsv` | GMC (source supplémentaire) |
| 5. Publication | — | tout, avec reprise de la veille si un fichier manque | — |

Réglages : `config.py` uniquement.

## Référentiels (dossier `data/`, commités chaque nuit)

**`referentiel_descriptions.csv`** — une ligne par coloris (MPN). Une ligne existante n'est jamais régénérée :
pour corriger un contenu, modifiez la ligne directement dans GitHub. Pour forcer une régénération, supprimez-la.
Colonne `origine` : `import_initial`, `heritage` (copié des coloris frères), `regles` (champs catalogue), suffixe `+ia` si la description vient de Gemini.

**`referentiel_qr.csv`** — une ligne par modèle (parent), colonne `statut` :
- `auto` : publiée (si `QR_IA_PUBLICATION_AUTO = True`)
- `valide` : publiée (validation manuelle)
- `revue` : bloquée par les garde-fous (terme sensible non justifié), à corriger puis passer en `valide`
- `brouillon` : en attente de validation (si publication auto désactivée)
- `rejete` : jamais publiée, jamais régénérée

## Rapports (publiés dans `rapports/`)
- `descriptions_a_rediger.csv` : coloris sans description (pas de clé Gemini ou quota atteint)
- `descriptions_revue.csv` : descriptions IA bloquées par les garde-fous
- `qr_a_revoir.csv` : Q&R en revue, brouillon ou échec

Le résumé de chaque nuit apparaît aussi dans l'onglet Actions, sur la page de l'exécution.
