"""
SofaScore Enrichment Pipeline
Matches Wyscout players to SofaScore using Triple-Verification (Name + DOB + Nationality).
Updates players table with sofascore_id.
"""
import logging
from datetime import datetime
from database.connection import get_conn
from scrapers.sofascore.client import SofaScoreClient

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def normalize_string(s: str) -> str:
    """Basic string normalization for fuzzy matching."""
    if not s:
        return ""
    import unicodedata
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8')
    return s.lower().strip()

def enrich_sofascore():
    # 1. Fetch players missing sofascore_id that have bio data
    players_to_enrich = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT player_id, name, date_of_birth, nationality 
                FROM players 
                WHERE sofascore_id IS NULL AND date_of_birth IS NOT NULL
                ORDER BY player_id DESC
            ''')
            players_to_enrich = cur.fetchall()
            
    logger.info(f"Found {len(players_to_enrich)} players to enrich from SofaScore.")
    
    updated_count = 0
    failed_count = 0
    
    # Initialize the Playwright client to bypass Cloudflare
    client = SofaScoreClient()
    
    try:
        for row in players_to_enrich:
            player_id, name, wyscout_dob, wyscout_nationality = row
            logger.info(f"Searching SofaScore for: {name} ({wyscout_nationality})")
            
            candidates = client.search_player(name)
            if not candidates:
                candidates = client.search_player(normalize_string(name))
            if not candidates and " " in name:
                # Fallback: wyscout uses 'H. Sakai', search for 'Sakai'
                last_name = name.split(" ")[-1]
                candidates = client.search_player(last_name)
                
            match_found = False
            for cand in candidates:
                cand_id = cand.get('id')
                cand_info = client.get_player_info(cand_id)
                if not cand_info:
                    continue
                    
                # 1. Verify Nationality
                sofa_country = cand_info.get('country', {}).get('name', '')
                
                # DEBUG LOG
                logger.info(f"  -> Candidate '{cand_info.get('name')}' | Wy Nat: {wyscout_nationality} vs Sofa Nat: {sofa_country}")
                
                if normalize_string(wyscout_nationality) != normalize_string(sofa_country):
                    if normalize_string(wyscout_nationality) not in normalize_string(sofa_country) and normalize_string(sofa_country) not in normalize_string(wyscout_nationality):
                        logger.info(f"     [REJECTED] Nationality Mismatch")
                        continue
                
                # 2. Verify Date of Birth
                sofa_timestamp = cand_info.get('dateOfBirthTimestamp')
                if not sofa_timestamp:
                    logger.info(f"     [REJECTED] No DOB found on SofaScore")
                    continue
                    
                sofa_dob = datetime.fromtimestamp(sofa_timestamp).date()
                
                # DEBUG LOG
                logger.info(f"  -> Nationality Matched! Checking DOB: Wy {wyscout_dob} vs Sofa {sofa_dob}")
                
                # Allow +/- 1 day difference for timezone shifts
                delta_days = abs((wyscout_dob - sofa_dob).days)
                if delta_days <= 1:
                    # TRIPLE-VERIFICATION PASSED!
                    logger.info(f"MATCH! {name} -> SofaScore ID {cand_id}")
                    
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute('''
                                UPDATE players 
                                SET sofascore_id = %s
                                WHERE player_id = %s
                            ''', (cand_id, player_id))
                        conn.commit()
                        
                    updated_count += 1
                    match_found = True
                    break
                    
            if not match_found:
                logger.warning(f"No valid match found for {name} after verification.")
                failed_count += 1
            
    finally:
        client.close()
        
    logger.info(f"Enrichment Complete. Updated: {updated_count}, Failed: {failed_count}")

if __name__ == '__main__':
    enrich_sofascore()
