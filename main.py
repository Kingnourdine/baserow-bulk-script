import os
import requests
import json

BASEROW_API_URL = os.environ.get('BASEROW_API_URL')
BASEROW_API_TOKEN = os.environ.get('BASEROW_API_TOKEN')
BASEROW_TABLE_ID = os.environ.get('BASEROW_TABLE_ID')

STATUS_FIELD = 'field_23'
TARGET_STATUS_ID = 1
DOMAIN_FIELD = 'field_17'

def get_first_page():
    """Récupère seulement la première page pour debug"""
    url = BASEROW_API_URL.format(table_id=BASEROW_TABLE_ID)
    headers = {'Authorization': f'Token {BASEROW_API_TOKEN}'}
    params = {
        'user_field_names': 'true',
        'size': 10,  # Seulement 10 rows pour debug
        f'filter__{STATUS_FIELD}': TARGET_STATUS_ID
    }
    
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def main():
    print("🔍 RÉCUPÉRATION DES 10 PREMIÈRES ROWS...")
    data = get_first_page()
    rows = data.get('results', [])
    
    print(f"✔️ {len(rows)} rows récupérées")
    print("\n" + "="*80)
    print("ANALYSE DÉTAILLÉE DES PREMIÈRES ROWS:")
    print("="*80)
    
    for i, row in enumerate(rows[:5]):  # Seulement les 5 premières
        print(f"\n📋 ROW {i+1} (ID: {row.get('id')}):")
        print("-" * 50)
        
        # Afficher TOUS les champs pour voir la structure
        print("🔍 TOUS LES CHAMPS:")
        for key, value in row.items():
            print(f"  {key}: {value} (type: {type(value)})")
        
        print(f"\n🎯 CHAMPS SPÉCIFIQUES:")
        
        # Champ domaine
        domain_value = row.get(DOMAIN_FIELD)
        print(f"  Domain ({DOMAIN_FIELD}): '{domain_value}'")
        print(f"    Type: {type(domain_value)}")
        print(f"    Repr: {repr(domain_value)}")
        print(f"    Len: {len(str(domain_value)) if domain_value else 0}")
        print(f"    Stripped: '{str(domain_value).strip()}' (len: {len(str(domain_value).strip()) if domain_value else 0})")
        
        # Champ statut
        status_value = row.get(STATUS_FIELD)
        print(f"  Status ({STATUS_FIELD}): {status_value}")
        print(f"    Type: {type(status_value)}")
        print(f"    Repr: {repr(status_value)}")
        
        # Test de validation simple
        if domain_value:
            domain_str = str(domain_value).strip()
            print(f"\n  🧪 TESTS DE VALIDATION:")
            print(f"    Est vide après strip: {domain_str == ''}")
            print(f"    Contient un point: {'.' in domain_str}")
            print(f"    Contient des espaces: {' ' in domain_str}")
            print(f"    Longueur OK (3-253): {3 <= len(domain_str) <= 253}")
            
            if '.' in domain_str:
                parts = domain_str.split('.')
                print(f"    Parties après split: {parts}")
                print(f"    TLD valide: {len(parts) >= 2 and len(parts[-1]) >= 2}")
        
        print("\n" + "="*50)
    
    print(f"\n📊 RÉSUMÉ RAPIDE DE TOUTES LES {len(rows)} ROWS:")
    empty_domains = 0
    valid_looking = 0
    
    for row in rows:
        domain = row.get(DOMAIN_FIELD, '')
        if not domain or str(domain).strip() == '':
            empty_domains += 1
        else:
            domain_str = str(domain).strip()
            if '.' in domain_str and len(domain_str) > 3:
                valid_looking += 1
                print(f"  ✅ Row {row.get('id')}: {domain_str}")
            else:
                print(f"  ❌ Row {row.get('id')}: '{domain_str}' (problème détecté)")
    
    print(f"\n📈 STATISTIQUES:")
    print(f"  Total: {len(rows)}")
    print(f"  Vides: {empty_domains}")
    print(f"  Valides: {valid_looking}")
    print(f"  Problématiques: {len(rows) - empty_domains - valid_looking}")

if __name__ == "__main__":
    main()