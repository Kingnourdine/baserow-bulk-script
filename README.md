# Baserow Bulk Script vers n8n

Ce script permet de récupérer les lignes de Baserow dont le statut est `get monthly traffic` et d'envoyer les données vers un webhook n8n.

## Prérequis
- Python 3.7+
- `requests` (installé automatiquement via `pip install -r requirements.txt`)

## Variables d'environnement à définir
- `BASEROW_API_URL` : URL de l'API Baserow, ex : `https://api.baserow.io/api/database/rows/table/{table_id}/?user_field_names=true`
- `BASEROW_API_TOKEN` : Token d'API Baserow
- `BASEROW_TABLE_ID` : ID de la table Baserow
- `N8N_WEBHOOK_URL` : URL du webhook n8n

## Utilisation

1. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```
2. Exporter les variables d'environnement :
   ```bash
   export BASEROW_API_URL="https://api.baserow.io/api/database/rows/table/{table_id}/?user_field_names=true"
   export BASEROW_API_TOKEN="votre_token"
   export BASEROW_TABLE_ID="123"
   export N8N_WEBHOOK_URL="https://votre-n8n/webhook/queue-add"
   ```
3. Lancer le script :
   ```bash
   python main.py
   ```

## Déclenchement via GitHub Actions

Vous pouvez créer un workflow GitHub Actions pour lancer ce script manuellement (voir exemple dans `.github/workflows/`).