# Stocks Sync TBS

Synchronisation quotidienne du flux stocks TBS vers Google Sheets.

## Setup

### 1. Créer le repo GitHub
Pousse ces fichiers dans un repo privé GitHub.

### 2. Ajouter le secret GitHub
GitHub → repo → Settings → Secrets and variables → Actions → New repository secret
- Nom : `GCP_SERVICE_ACCOUNT_KEY`
- Valeur : colle le contenu COMPLET du fichier JSON de ta clé de service

### 3. Partager le Google Sheet
Partage le Sheet `1x2E77GkjdFdPfVkBH6rW-V3fWiu9kuMxFA1MJI1v4gU`
avec le compte de service en mode **Éditeur**.

### 4. Activer Google Sheets API
Google Cloud Console → APIs & Services → Bibliothèque → 
cherche "Google Sheets API" → Activer sur le projet `gmc-merchant-api-500409`.

### 5. Tester manuellement
GitHub → repo → Actions → "Stocks Sync TBS" → "Run workflow"

## Fichiers
- `stocks_sync.py` — script principal
- `requirements.txt` — dépendances Python
- `.github/workflows/stocks_sync.yml` — workflow GitHub Actions
