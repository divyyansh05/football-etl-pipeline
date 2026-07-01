"""
Club ELO Enrichment Pipeline.
Queries existing teams in the database, fetches their Club ELO, and updates the team_elo table.
"""
import logging
from database.connection import get_conn
from scrapers.clubelo.client import get_current_elo

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def create_table_if_needed():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS team_elo (
                    id                SERIAL PRIMARY KEY,
                    team_id           INTEGER REFERENCES teams(team_id) UNIQUE,
                    elo_rating        NUMERIC(6,2),
                    updated_at        TIMESTAMPTZ DEFAULT NOW()
                );
            ''')
        conn.commit()

def enrich_teams():
    create_table_if_needed()
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Fetch teams that do not have an ELO rating updated in the last 7 days
            cur.execute('''
                SELECT t.team_id, t.name 
                FROM teams t
                LEFT JOIN team_elo e ON t.team_id = e.team_id
                WHERE e.elo_rating IS NULL 
                   OR e.updated_at < NOW() - INTERVAL '7 days'
                ORDER BY t.team_id
            ''')
            teams = cur.fetchall()
            
    logger.info(f"Found {len(teams)} teams to enrich with Club ELO.")
    
    updated_count = 0
    failed_count = 0
    
    for row in teams:
        team_id, team_name = row
        logger.info(f"Fetching ELO for: {team_name}")
        
        elo = get_current_elo(team_name)
        if elo:
            logger.info(f" -> {team_name} ELO: {elo}")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO team_elo (team_id, elo_rating, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (team_id) 
                        DO UPDATE SET elo_rating = EXCLUDED.elo_rating, updated_at = NOW();
                    ''', (team_id, elo))
                conn.commit()
            updated_count += 1
        else:
            failed_count += 1
            
    logger.info(f"Club ELO Enrichment Complete. Updated: {updated_count}, Failed: {failed_count}")

if __name__ == '__main__':
    enrich_teams()
