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

# ✅ CHAMPS CORRIGÉS BASÉS SUR VOS DONNÉES RÉELLES
STATUS_FIELD = 'status'  # Le vrai nom du champ status
TARGET_STATUS = 'monthly traffic generated'  # La vraie valeur à filtrer
DOMAIN_FIELD = 'organization primary domain'  # Le vrai nom du champ domaine

REQUEST_DELAY = 0.1

if not all([BASEROW_API_URL, BASEROW_API_TOKEN, BASEROW_TABLE_ID, N8N_WEBHOOK_URL]):
    raise Exception("❌ Variables d'environnement manquantes")

def validate_domain(domain: str) -> bool:
    """Domain validation"""
    if not domain:
        return False
    
    domain_str = str(domain).strip().lower()
    if not domain_str or len(domain_str) < 3:
        return False
        
    if '.' not in domain_str:
        return False
        
    parts = domain_str.split('.')
    if len(parts) < 2 or len(parts[-1]) < 2:
        return False
        
    return True

def get_baserow_rows() -> List[Dict[Any, Any]]:
    """Fetch all rows from Baserow"""
    url = BASEROW_API_URL.format(table_id=BASEROW_TABLE_ID)
    headers = {'Authorization': f'Token {BASEROW_API_TOKEN}'}
    params = {
        'user_field_names': 'true',
        'size': 200
        # ✅ ON FILTRE MANUELLEMENT APRÈS RÉCUPÉRATION car le status est un objet
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
            params = None
            page += 1
            
            if next_url:
                time.sleep(REQUEST_DELAY)
                
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération: {e}")
        raise
    
    # ✅ FILTRAGE MANUEL PAR STATUS
    filtered_rows = []
    for row in all_rows:
        status_obj = row.get(STATUS_FIELD)
        if isinstance(status_obj, dict) and status_obj.get('value') == TARGET_STATUS:
            filtered_rows.append(row)
    
    logger.info(f"✔️ {len(filtered_rows)} rows avec le status '{TARGET_STATUS}' sur {len(all_rows)} total")
    return filtered_rows

def build_payload(rows: List[Dict[Any, Any]]) -> List[Dict[str, Any]]:
    """Build payload for n8n from Baserow rows"""
    items = []
    invalid_domains = 0
    empty_domains = 0
    
    logger.info(f"🔍 Traitement de {len(rows)} rows...")
    
    for i, row in enumerate(rows):
        if i % 1000 == 0 and i > 0:
            logger.info(f"📊 Progression: {i}/{len(rows)} rows traitées")
            
        # ✅ UTILISER LE BON CHAMP DOMAINE
        domain = row.get(DOMAIN_FIELD, '')
        row_id = row.get('id', 'unknown')
        
        if not domain or str(domain).strip() == '':
            empty_domains += 1
            continue
        
        if not validate_domain(str(domain)):
            invalid_domains += 1
            if invalid_domains <= 10:
                logger.warning(f"⚠️ Domaine invalide ignoré: '{domain}' (ID: {row_id})")
            continue
        
        # ✅ EXTRACTION CORRECTE DU STATUS
        status_obj = row.get(STATUS_FIELD, {})
        status_value = status_obj.get('value') if isinstance(status_obj, dict) else status_obj
        
        items.append({
            'domain': str(domain).strip(),
            'record_id': row_id,
            'status': status_value,
            'email': row.get('email', ''),
            'organization_name': row.get('organization name', ''),
            'baserow_data': row
        })
    
    # Statistiques finales
    logger.info(f"📈 Statistiques de traitement:")
    logger.info(f"   ✅ Domaines valides: {len(items)}")
    logger.info(f"   🔹 Domaines vides: {empty_domains}")
    logger.info(f"   ⚠️ Domaines invalides: {invalid_domains}")
    logger.info(f"   📊 Total traité: {len(rows)}")
    
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
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'envoi à n8n: {e}")
        raise

def main():
    """Main execution function"""
    try:
        logger.info("🚀 Démarrage du script Baserow vers n8n (VERSION CORRIGÉE)")
        logger.info(f"🎯 Recherche des rows avec le statut: '{TARGET_STATUS}'")
        logger.info(f"🌐 Champ domaine utilisé: '{DOMAIN_FIELD}'")
        
        # Fetch rows from Baserow
        rows = get_baserow_rows()
        
        if len(rows) == 0:
            logger.warning("⚠️ Aucune row trouvée avec le statut demandé")
            return
        
        # Build payload
        items = build_payload(rows)
        
        if len(items) == 0:
            logger.warning("⚠️ Aucun item valide trouvé après traitement")
            return
        
        # Send to n8n
        send_to_n8n(items)
        
        logger.info("🎉 Script terminé avec succès!")
        
    except Exception as e:
        logger.error(f"💥 Échec du script: {e}")
        raise

if __name__ == "__main__":
    main()