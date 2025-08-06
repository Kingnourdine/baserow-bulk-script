import os
import requests
import time
import logging
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASEROW_API_URL = os.environ.get('BASEROW_API_URL')
BASEROW_API_TOKEN = os.environ.get('BASEROW_API_TOKEN')
BASEROW_TABLE_ID = os.environ.get('BASEROW_TABLE_ID')
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL')

STATUS_FIELD = 'field_23'
TARGET_STATUS_ID = 1  # ID réel de "get monthly traffic"
DOMAIN_FIELD = 'field_17'
REQUEST_DELAY = 0.1  # Delay between requests to avoid rate limiting

if not all([BASEROW_API_URL, BASEROW_API_TOKEN, BASEROW_TABLE_ID, N8N_WEBHOOK_URL]):
    raise Exception("❌ Veuillez définir BASEROW_API_URL, BASEROW_API_TOKEN, BASEROW_TABLE_ID, N8N_WEBHOOK_URL dans vos variables d'environnement.")

def validate_domain(domain: str) -> bool:
    """Basic domain validation"""
    if not domain or not isinstance(domain, str):
        return False
    domain = domain.strip()
    # Basic checks: contains dot, no spaces, reasonable length
    return '.' in domain and ' ' not in domain and 3 <= len(domain) <= 253

def get_baserow_rows() -> List[Dict[Any, Any]]:
    """Fetch all rows from Baserow with the target status"""
    url = BASEROW_API_URL.format(table_id=BASEROW_TABLE_ID)
    headers = {
        'Authorization': f'Token {BASEROW_API_TOKEN}',
        'Content-Type': 'application/json'
    }
    params = {
        'user_field_names': 'true',
        'size': 200,
        f'filter__{STATUS_FIELD}': TARGET_STATUS_ID
    }
    
    all_rows = []
    next_url = url
    page = 1
    
    try:
        while next_url:
            logger.info(f"📄 Récupération de la page {page}...")
            
            resp = requests.get(
                next_url, 
                headers=headers, 
                params=params if next_url == url else None,
                timeout=30
            )
            resp.raise_for_status()
            
            data = resp.json()
            current_results = data.get('results', [])
            all_rows.extend(current_results)
            
            logger.info(f"✔️ Page {page}: {len(current_results)} rows récupérées")
            
            next_url = data.get('next')
            params = None  # params only for first call
            page += 1
            
            # Small delay to avoid rate limiting
            if next_url:
                time.sleep(REQUEST_DELAY)
                
    except requests.RequestException as e:
        logger.error(f"❌ Erreur lors de la récupération des données Baserow: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue: {e}")
        raise
    
    return all_rows

def build_payload(rows: List[Dict[Any, Any]]) -> List[Dict[str, Any]]:
    """Build payload for n8n from Baserow rows"""
    items = []
    invalid_domains = 0
    
    for row in rows:
        domain = row.get(DOMAIN_FIELD, '')
        
        if not validate_domain(domain):
            invalid_domains += 1
            logger.warning(f"⚠️ Domaine invalide ignoré: '{domain}' (ID: {row.get('id')})")
            continue
        
        # Extraction correcte du statut (champ single select)
        status_obj = row.get(STATUS_FIELD)
        status_value = status_obj.get('value') if isinstance(status_obj, dict) else status_obj
        
        items.append({
            'domain': domain.strip(),
            'record_id': row.get('id'),
            'status': status_value,
            'baserow_data': row
        })
    
    if invalid_domains > 0:
        logger.warning(f"⚠️ {invalid_domains} domaines invalides ont été ignorés")
    
    return items

def send_to_n8n(items: List[Dict[str, Any]]) -> None:
    """Send items to n8n webhook"""
    if not items:
        logger.warning("⚠️ Aucun item à envoyer à n8n.")
        return
    
    payload = {'items': items}
    
    try:
        logger.info(f"📤 Envoi de {len(items)} items à n8n...")
        resp = requests.post(
            N8N_WEBHOOK_URL, 
            json=payload, 
            timeout=60,
            headers={'Content-Type': 'application/json'}
        )
        resp.raise_for_status()
        
        logger.info(f"✅ Données envoyées avec succès à n8n")
        logger.debug(f"Réponse n8n: {resp.text}")
        
    except requests.RequestException as e:
        logger.error(f"❌ Erreur lors de l'envoi à n8n: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Réponse d'erreur: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue lors de l'envoi: {e}")
        raise

def main():
    """Main execution function"""
    try:
        logger.info("🚀 Démarrage du script Baserow vers n8n")
        logger.info(f"🎯 Recherche des rows avec le statut ID: {TARGET_STATUS_ID}")
        
        # Fetch rows from Baserow
        rows = get_baserow_rows()
        logger.info(f"✔️ {len(rows)} rows trouvées avec le statut ID {TARGET_STATUS_ID}")
        
        # Build payload
        items = build_payload(rows)
        logger.info(f"📦 {len(items)} items valides préparés pour n8n")
        
        # Send to n8n
        send_to_n8n(items)
        
        logger.info("🎉 Script terminé avec succès!")
        
    except Exception as e:
        logger.error(f"💥 Échec du script: {e}")
        raise

if __name__ == "__main__":
    main()