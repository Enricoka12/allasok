import os
from dotenv import load_dotenv
from supabase import create_client, Client
from typesense import Client as TypesenseClient
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime

# Naplózás beállítása
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Környezeti változók betöltése
load_dotenv()

# Supabase kliensek inicializálása
supabase_url: str = os.getenv('SUPABASE_URL')
supabase_key: str = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(supabase_url, supabase_key)

# Typesense kliensek inicializálása
typesense = TypesenseClient({
    'nodes': [{
        'host': os.getenv('TYPESENSE_HOST'),
        'port': '443',
        'protocol': 'https'
    }],
    'api_key': os.getenv('TYPESENSE_API_KEY'),
    'connection_timeout_seconds': 10
})


def normalize_place_name(place_name: Optional[str]) -> str:
    """Helynevek normalizálása a koordinátákhoz való illesztéshez"""
    if not place_name:
        return ''
    
    # Kisbetűsítés és felesleges szóközök eltávolítása
    normalized = place_name.strip().lower()
    
    # Budapest kerületek kezelése
    if 'budapest' in normalized:
        normalized = 'budapest'
    
    # Egyéb gyakori helynév normalizálások
    replacements = {
        'bp.': 'budapest',
        'budaörs': 'budaörs',
        'rákócziújfalu': 'rákócziújfalu',
        'ászár': 'ászár'
    }
    
    # Ismert helynevek cseréje
    for from_str, to_str in replacements.items():
        if from_str in normalized:
            normalized = to_str
            break
    
    return normalized





async def sync_data():
    """Fő szinkronizáló függvény"""
    try:
        logger.info('Lekérjük az összes állást...')
        allasok_response = supabase.table('allasok').select('*').execute()
        allasok = allasok_response.data
        logger.info(f'Talált állások száma: {len(allasok)}')

        logger.info('Lekérjük a koordinátákat...')
        kordinatak_response = supabase.table('helyseg_koordinatak').select('*').execute()
        kordinatak = kordinatak_response.data
        logger.info(f'Talált koordináták száma: {len(kordinatak)}')

        # Összefűzzük a kettőt: állás + koordináta
        transformed = []
        for allas in allasok:
            hely = normalize_place_name(allas.get('hely'))
            kordi = next(
                (k for k in kordinatak 
                 if normalize_place_name(k.get('helyseg_nev')) == hely),
                None
            )

            if not kordi:
                logger.warning(f'Nem található koordináta ehhez a helyhez: {allas.get("hely")} (normalizálva: {hely})')
                continue

            # Javított location formátum: [lat, lng]
            location = None
            try:
                lat = float(kordi.get('szel_fok', 0))
                lng = float(kordi.get('hossz_fok', 0))
                if lat and lng:
                    location = [lat, lng]
            except (ValueError, TypeError):
                pass

            transformed.append({
                'id': str(allas.get('id', '')),
                'munka_neve': allas.get('munka_neve', ''),
                'munkakor': allas.get('munkakor', ''),
                'ceg_neve': allas.get('ceg_neve', ''),
                'hely': allas.get('hely', ''),
                'ceg': allas.get('ceg', ''),
                'kepviselo_elerhetosegei': allas.get('kepviselo_elerhetosegei', ''),
                'felajanlott_havi_brutto_kereset': allas.get('felajanlott_havi_brutto_kereset'),
                'munkavegzes_helye': allas.get('munkavegzes_helye', ''),
                'megjegyzes': allas.get('megjegyzes', ''),
                'email': allas.get('email', ''),
                'utoljara_frissitve': allas.get('utoljara_frissitve', ''),
                'active': bool(allas.get('active', False)),
                'munkarend': allas.get('munkarend', ''),
                'speciális_követelmények': allas.get('speciális_követelmények', ''),
                'speciális_körülmények': allas.get('speciális_körülmények', ''),
                'a_munkakorhoz_kapcsolodo_juttatasok': allas.get('a_munkakorhoz_kapcsolodo_juttatasok', ''),
                'allas_egyeztes_helye': allas.get('allas_egyeztes_helye', ''),
                'location': location
            })

        logger.info(f'Átalakított adatok száma: {len(transformed)}')

        try:
            logger.info('Töröljük a régi collection-t, ha létezik...')
            typesense.collections['allasok'].delete()
            logger.info('Régi collection törölve.')
        except Exception as e:
            logger.info(f'Nincs régi collection, vagy nem tudtuk törölni: {str(e)}')

        logger.info('Új collection létrehozása...')
        typesense.collections.create({
            'name': 'allasok',
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'munka_neve', 'type': 'string'},
                {'name': 'munkakor', 'type': 'string'},
                {'name': 'ceg_neve', 'type': 'string'},
                {'name': 'hely', 'type': 'string'},
                {'name': 'ceg', 'type': 'string'},
                {'name': 'kepviselo_elerhetosegei', 'type': 'string', 'optional': True},
                {'name': 'felajanlott_havi_brutto_kereset', 'type': 'string', 'optional': True},
                {'name': 'munkavegzes_helye', 'type': 'string', 'optional': True},
                {'name': 'megjegyzes', 'type': 'string', 'optional': True},
                {'name': 'email', 'type': 'string', 'optional': True},
                {'name': 'utoljara_frissitve', 'type': 'string', 'optional': True},
                {'name': 'active', 'type': 'bool', 'optional': True},
                {'name': 'munkarend', 'type': 'string', 'optional': True},
                {'name': 'speciális_követelmények', 'type': 'string', 'optional': True},
                {'name': 'speciális_körülmények', 'type': 'string', 'optional': True},
                {'name': 'a_munkakorhoz_kapcsolodo_juttatasok', 'type': 'string', 'optional': True},
                {'name': 'allas_egyeztes_helye', 'type': 'string', 'optional': True},
                {'name': 'location', 'type': 'geopoint'},
            ]
        })
        logger.info('Collection létrehozva.')

        logger.info('Adatok feltöltése...')
        # Batch-eljük a feltöltést 100-as csomagokban
        batch_size = 100
        for i in range(0, len(transformed), batch_size):
            batch = transformed[i:i + batch_size]
            import_results = typesense.collections['allasok'].documents.import_(batch, {'action': 'upsert'})
            
            success_count = sum(1 for result in import_results if not result.get('success'))
            error_count = len(import_results) - success_count
            
            logger.info(f'Batch {i // batch_size + 1} importálva. Sikeres: {success_count}, Hibás: {error_count}')
            
            # Hibák logolása
            for result in import_results:
                if not result.get('success'):
                    logger.error(f'Hibás dokumentum: {result}')

        logger.info('Sikeres szinkron')
        return {'status': 'success', 'message': 'Sikeres szinkron'}

    except Exception as e:
        logger.error(f'Hiba a szinkron során: {str(e)}', exc_info=True)
        return {'status': 'error', 'message': str(e)}


if __name__ == '__main__':
    import asyncio
    result = asyncio.run(sync_data())
    logger.info(f'Szinkronizálás eredménye: {result}')
