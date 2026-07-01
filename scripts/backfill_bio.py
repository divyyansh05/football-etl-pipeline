import os
import sys
import time
import logging
from datetime import datetime
from scrapers.wyscout.client import _gql
from scrapers.wyscout.token_manager import refresh_token
from database.connection import get_conn, query

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def backfill_bio():
    """Backfill player biographical data from Wyscout."""
    logger.info("Starting player biographical data backfill...")

    # Get players missing bio data
    players = query('''
        SELECT player_id, wyscout_id 
        FROM players 
        WHERE date_of_birth IS NULL
        ORDER BY player_id
    ''', as_dict=True)

    total_players = len(players)
    if total_players == 0:
        logger.info("No players found missing date_of_birth. Backfill complete.")
        return

    logger.info(f"Found {total_players} players missing bio data. Beginning extraction...")

    updated_count = 0
    failed_count = 0

    for i, p in enumerate(players, 1):
        player_id = p['player_id']
        wyscout_id = p['wyscout_id']

        try:
            r = _gql(f'''
                query {{
                    player(id: {wyscout_id}) {{
                        fullName
                        shortName
                        birthDate
                        birthCountry {{ name }}
                        height
                        weight
                        foot
                        currentTeam {{ name id }}
                        marketValue
                        imageUrl
                        onLoan
                        tmId
                        passportCountries {{ name }}
                        playerTransfermarktData {{ contractExpirationDate }}
                    }}
                }}
            ''')
            data = r.json()
            
            if 'errors' in data:
                logger.error(f"GraphQL error for {wyscout_id}: {data['errors']}")
                failed_count += 1
                continue
                
            player_data = data.get('data', {}).get('player')
            if not player_data:
                logger.warning(f"No player data returned for wyscout_id {wyscout_id}")
                failed_count += 1
                continue

            # Extract fields safely
            full_name = player_data.get('fullName')
            dob = player_data.get('birthDate')
            
            birth_country = player_data.get('birthCountry')
            nationality = birth_country.get('name') if birth_country else None
            
            height = player_data.get('height')
            weight = player_data.get('weight')
            foot = player_data.get('foot')
            
            current_team = player_data.get('currentTeam')
            team_name = current_team.get('name') if current_team else None
            team_id = int(current_team.get('id')) if current_team and current_team.get('id') else None
            
            image_url = player_data.get('imageUrl')
            on_loan = player_data.get('onLoan') or False
            market_value = player_data.get('marketValue')
            tm_id = player_data.get('tmId')
            
            passports = player_data.get('passportCountries') or []
            passport_list = ",".join([pc['name'] for pc in passports if pc.get('name')]) if passports else None
            
            tm_data = player_data.get('playerTransfermarktData')
            contract_expiry = tm_data.get('contractExpirationDate') if tm_data else None

            # Update database
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        UPDATE players SET
                            full_name = %s,
                            date_of_birth = %s,
                            nationality = %s,
                            height_cm = %s,
                            weight_kg = %s,
                            preferred_foot = %s,
                            image_url = %s,
                            current_team_name = %s,
                            current_team_wyscout_id = %s,
                            on_loan = %s,
                            market_value_eur = %s,
                            passport_countries = %s,
                            contract_expires = %s,
                            transfermarkt_id = %s
                        WHERE player_id = %s
                    ''', (
                        full_name, dob, nationality, height, weight, foot,
                        image_url, team_name, team_id, on_loan, market_value,
                        passport_list, contract_expiry, tm_id, player_id
                    ))
                conn.commit()
            
            updated_count += 1

        except Exception as e:
            if "Wyscout API error 401" in str(e) or "Wyscout API error 403" in str(e):
                logger.error(f"FATAL: Token expired or auth error at player {wyscout_id} (error: {e}). STOPPING queue.")
                break
            logger.error(f"Failed to process player {wyscout_id}: {e}")
            failed_count += 1

        if i % 100 == 0 or i == total_players:
            logger.info(f"Progress: {i}/{total_players} — updated {updated_count}, failed {failed_count}")

    # Final summary
    logger.info(f"COMPLETE: {updated_count}/{total_players} updated, {failed_count} failed")
    
    # Calculate coverage
    coverage_stats = query('''
        SELECT 
            ROUND(COUNT(nationality)::numeric / COUNT(*) * 100, 2) as nat_cov,
            ROUND(COUNT(date_of_birth)::numeric / COUNT(*) * 100, 2) as dob_cov,
            ROUND(COUNT(height_cm)::numeric / COUNT(*) * 100, 2) as height_cov,
            ROUND(COUNT(image_url)::numeric / COUNT(*) * 100, 2) as img_cov
        FROM players
    ''', as_dict=True)[0]
    
    logger.info(f"Coverage: nationality={coverage_stats['nat_cov']}%, DOB={coverage_stats['dob_cov']}%, "
                f"height={coverage_stats['height_cov']}%, image={coverage_stats['img_cov']}%")

if __name__ == '__main__':
    backfill_bio()
