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

# ✅ CHAMPS CORRIGÉS
STATUS_FIELD = 'status'
TARGET_STATUS = 'get monthly traffic'  # ✅ Le bon statut à filtrer
DOMAIN_FIELD = 'organization primary domain'

# ✅ CONFIGURATION DES BATCHES POUR TEST
BATCH_SIZE = 1000  # Test avec 1000 items par batch
BATCH_INTERVAL_SECONDS = 90  # 3 minutes entre chaque batch

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
    """Fetch all rows from Baserow and filter by status"""
    url = BASEROW_API_URL.format(table_id=BASEROW_TABLE_ID)
    headers = {'Authorization': f'Token {BASEROW_API_TOKEN}'}
    params = {
        'user_field_names': 'true',
        'size': 200
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
    
    # ✅ FILTRAGE PAR STATUS "get monthly traffic"
    filtered_rows = []
    status_counts = {}
    
    for row in all_rows:
        status_obj = row.get(STATUS_FIELD, {})
        if isinstance(status_obj, dict):
            status_value = status_obj.get('value', '')
            status_counts[status_value] = status_counts.get(status_value, 0) + 1
            
            if status_value == TARGET_STATUS:
                filtered_rows.append(row)
    
    logger.info(f"📊 STATISTIQUES DES STATUTS:")
    for status, count in sorted(status_counts.items()):
        indicator = "🎯" if status == TARGET_STATUS else "  "
        logger.info(f"   {indicator} '{status}': {count} rows")
    
    logger.info(f"✔️ {len(filtered_rows)} rows avec le status '{TARGET_STATUS}' sur {len(all_rows)} total")
    return filtered_rows

def build_payload(rows: List[Dict[Any, Any]]) -> List[Dict[str, Any]]:
    """Build payload for n8n from Baserow rows"""
    items = []
    invalid_domains = 0
    empty_domains = 0
    
    logger.info(f"🔍 Traitement de {len(rows)} rows...")
    
    for i, row in enumerate(rows):
        domain = row.get(DOMAIN_FIELD, '')
        row_id = row.get('id', 'unknown')
        
        if not domain or str(domain).strip() == '':
            empty_domains += 1
            continue
        
        if not validate_domain(str(domain)):
            invalid_domains += 1
            if invalid_domains <= 5:  # Log seulement les 5 premiers
                logger.warning(f"⚠️ Domaine invalide ignoré: '{domain}' (ID: {row_id})")
            continue
        
        # Extraction du status
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
    logger.info(f"📈 RÉSULTATS DU TRAITEMENT:")
    logger.info(f"   ✅ Domaines valides: {len(items)}")
    logger.info(f"   🔹 Domaines vides: {empty_domains}")
    logger.info(f"   ⚠️ Domaines invalides: {invalid_domains}")
    logger.info(f"   📊 Total traité: {len(rows)}")
    
    return items

def send_batch_to_n8n(batch_items: List[Dict[str, Any]], batch_num: int, total_batches: int) -> bool:
    """Send a single batch to n8n webhook - Format adapté pour le code n8n existant"""
    
    # 🔄 TRANSFORMATION AU FORMAT ATTENDU PAR N8N
    # Extraire les domaines
    domains = [item['domain'] for item in batch_items]
    
    # Créer le mapping domain -> record_id
    mapping = {item['domain']: item['record_id'] for item in batch_items}
    
    # Créer le payload au format attendu par votre code n8n
    payload = {
        'body': {
            'domains': domains,
            'mapping': mapping
        }
    }
    
    logger.info(f"📤 Envoi du batch {batch_num}/{total_batches} ({len(batch_items)} items)")
    
    # Show sample domains from this batch
    sample_domains = domains[:3]
    logger.info(f"   Exemples: {', '.join(sample_domains)}{'...' if len(domains) > 3 else ''}")
    
    # Debug: montrer la structure du payload
    logger.info(f"   📋 Structure: body.domains={len(domains)} items, body.mapping={len(mapping)} entries")
    
    try:
        resp = requests.post(
            N8N_WEBHOOK_URL,
            json=payload,
            timeout=60,
            headers={'Content-Type': 'application/json'},
            verify=False  # Pour éviter les erreurs SSL
        )
        resp.raise_for_status()
        
        logger.info(f"✅ Batch {batch_num} envoyé avec succès!")
        logger.info(f"   Réponse: {resp.text[:200]}{'...' if len(resp.text) > 200 else ''}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur pour le batch {batch_num}: {e}")
        return False

def send_items_in_batches(items: List[Dict[str, Any]]) -> None:
    """Send items to n8n in batches with intervals"""
    if not items:
        logger.warning("⚠️ Aucun item à envoyer.")
        return
    
    total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"🚀 DÉMARRAGE DE L'ENVOI PAR BATCHES")
    logger.info(f"   📦 {len(items)} items total")
    logger.info(f"   📊 {total_batches} batches de {BATCH_SIZE} items")
    logger.info(f"   ⏱️ Intervalle: {BATCH_INTERVAL_SECONDS//60}min {BATCH_INTERVAL_SECONDS%60}s entre chaque batch")
    
    successful_batches = 0
    failed_batches = 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min((batch_num + 1) * BATCH_SIZE, len(items))
        batch_items = items[start_idx:end_idx]
        
        # Send batch
        success = send_batch_to_n8n(batch_items, batch_num + 1, total_batches)
        
        if success:
            successful_batches += 1
        else:
            failed_batches += 1
        
        # Wait before next batch (except for the last one)
        if batch_num < total_batches - 1:
            logger.info(f"⏳ Attente de {BATCH_INTERVAL_SECONDS//60}min {BATCH_INTERVAL_SECONDS%60}s avant le prochain batch...")
            
            # Show countdown every 30 seconds
            remaining = BATCH_INTERVAL_SECONDS
            while remaining > 0:
                if remaining % 30 == 0 or remaining <= 10:
                    logger.info(f"   ⏰ {remaining//60}min {remaining%60}s restantes...")
                time.sleep(1)
                remaining -= 1
    
    # Final summary
    logger.info(f"🏁 RÉSUMÉ FINAL:")
    logger.info(f"   ✅ Batches réussis: {successful_batches}")
    logger.info(f"   ❌ Batches échoués: {failed_batches}")
    logger.info(f"   📊 Taux de succès: {successful_batches/total_batches*100:.1f}%")

def main():
    """Main execution function"""
    try:
        logger.info("🚀 SCRIPT DE TEST - BATCHES DE 10 ITEMS")
        logger.info(f"🎯 Filtrage par statut: '{TARGET_STATUS}'")
        logger.info(f"🌐 Champ domaine: '{DOMAIN_FIELD}'")
        logger.info(f"📦 Taille des batches: {BATCH_SIZE} items")
        
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
        
        # Send in batches
        send_items_in_batches(items)
        
        logger.info("🎉 Test terminé!")
        
    except Exception as e:
        logger.error(f"💥 Échec du script: {e}")
        raise

if __name__ == "__main__":
    main()