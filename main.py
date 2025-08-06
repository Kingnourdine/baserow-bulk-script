import os
import requests

BASEROW_API_URL = os.environ.get('BASEROW_API_URL')  # ex: https://api.baserow.io/api/database/rows/table/{table_id}/?user_field_names=true
BASEROW_API_TOKEN = os.environ.get('BASEROW_API_TOKEN')
BASEROW_TABLE_ID = os.environ.get('BASEROW_TABLE_ID')
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL')

STATUS_FIELD = 'field_23'
TARGET_STATUS = 'get monthly traffic'
DOMAIN_FIELD = 'field_17'

if not all([BASEROW_API_URL, BASEROW_API_TOKEN, BASEROW_TABLE_ID, N8N_WEBHOOK_URL]):
    raise Exception("Veuillez définir BASEROW_API_URL, BASEROW_API_TOKEN, BASEROW_TABLE_ID, N8N_WEBHOOK_URL dans vos variables d'environnement.")

def get_baserow_rows():
    url = BASEROW_API_URL.format(table_id=BASEROW_TABLE_ID)
    headers = {
        'Authorization': f'Token {BASEROW_API_TOKEN}'
    }
    params = {
        'user_field_names': 'true',
        'size': 200,
        f'filter__{STATUS_FIELD}__value': TARGET_STATUS
    }
    all_rows = []
    next_url = url
    while next_url:
        resp = requests.get(next_url, headers=headers, params=params if next_url == url else None)
        resp.raise_for_status()
        data = resp.json()
        all_rows.extend(data.get('results', []))
        next_url = data.get('next')
        params = None  # params only for first call
    return all_rows

def build_payload(rows):
    items = []
    for row in rows:
        domain = row.get(DOMAIN_FIELD, '').strip()
        if not domain:
            continue
        items.append({
            'domain': domain,
            'record_id': row.get('id'),
            'status': row.get(STATUS_FIELD),
            'baserow_data': row
        })
    return items

def send_to_n8n(items):
    payload = {
        'items': items
    }
    resp = requests.post(N8N_WEBHOOK_URL, json=payload)
    resp.raise_for_status()
    print(f"✅ Données envoyées à n8n. Réponse: {resp.text}")

def main():
    print("🔎 Récupération des rows Baserow...")
    rows = get_baserow_rows()
    print(f"Trouvé {len(rows)} rows avec le statut '{TARGET_STATUS}'.")
    items = build_payload(rows)
    print(f"Envoi de {len(items)} items à n8n...")
    send_to_n8n(items)

if __name__ == "__main__":
    main()
