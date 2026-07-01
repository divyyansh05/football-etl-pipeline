"""
Data Quality Verification Script.
Run after every extraction to ensure integrity.
"""
import sys
import os
import logging

# Ensure project root is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import query

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

def run_checks():
    failed = 0
    
    logger.info("Starting Data Quality Checks...")

    # 1. No duplicate rows in player_match_stats
    # (wyscout_player_name, match_date, competition_name, minutes_played)
    duplicates = query("""
        SELECT player_id, match_date, competition_name, COUNT(*)
        FROM player_match_stats
        GROUP BY player_id, match_date, competition_name
        HAVING COUNT(*) > 1
    """)
    if duplicates:
        logger.error(f"FAIL: Found {len(duplicates)} duplicate match records!")
        failed += 1
    else:
        logger.info("PASS: No duplicates found.")

    # 2. No NULL in required fields
    nulls = query("""
        SELECT COUNT(*) FROM player_match_stats
        WHERE match_date IS NULL 
           OR competition_name IS NULL 
           OR minutes_played IS NULL
    """)
    if nulls and nulls[0][0] > 0:
        logger.error(f"FAIL: Found {nulls[0][0]} rows with NULL in required fields!")
        failed += 1
    else:
        logger.info("PASS: No NULLs in required match fields.")

    # 3. xG values in reasonable range (0-5 per match)
    xg_outliers = query("""
        SELECT COUNT(*) FROM player_match_stats
        WHERE xg < 0 OR xg > 5
    """)
    if xg_outliers and xg_outliers[0][0] > 0:
        logger.error(f"FAIL: Found {xg_outliers[0][0]} rows with xG outside 0-5 range!")
        failed += 1
    else:
        logger.info("PASS: xG values are in reasonable range.")

    # 4. goals <= shots <= total_actions
    logical_fails = query("""
        SELECT COUNT(*) FROM player_match_stats
        WHERE (goals > shots) 
           OR (shots > total_actions AND total_actions > 0)
    """)
    if logical_fails and logical_fails[0][0] > 0:
        logger.error(f"FAIL: Found {logical_fails[0][0]} rows with logical stat errors (goals > shots, etc)!")
        failed += 1
    else:
        logger.info("PASS: Statistical logic (goals <= shots <= actions) holds.")

    # 5. Position groups are valid
    invalid_positions = query("""
        SELECT DISTINCT position_group FROM players
        WHERE position_group NOT IN ('GK', 'DEF', 'MID', 'FWD') 
          AND position_group IS NOT NULL
    """)
    if invalid_positions:
        logger.error(f"FAIL: Found invalid position groups: {[r[0] for r in invalid_positions]}")
        failed += 1
    else:
        logger.info("PASS: All position groups are valid.")

    # 6. All loaded_files entries have rows_loaded > 0
    empty_loads = query("""
        SELECT COUNT(*) FROM loaded_files WHERE rows_loaded = 0
    """)
    if empty_loads and empty_loads[0][0] > 0:
        logger.warning(f"WARN: Found {empty_loads[0][0]} files loaded with 0 rows.")
        # This might be a warning rather than a fail depending on extraction state
    else:
        logger.info("PASS: All loaded files contain data.")

    # 7. player_scores covers all players with sufficient minutes (> 450)
    missing_scores = query("""
        SELECT COUNT(*) FROM (
            SELECT pms.player_id, pms.competition_name
            FROM player_match_stats pms
            GROUP BY pms.player_id, pms.competition_name
            HAVING SUM(pms.minutes_played) >= 450
        ) p_with_min
        LEFT JOIN player_scores ps ON p_with_min.player_id = ps.player_id 
          AND ps.competition_id = (
              SELECT competition_id FROM competitions 
              WHERE name = p_with_min.competition_name LIMIT 1
          )
        WHERE ps.player_id IS NULL
    """)
    if missing_scores and missing_scores[0][0] > 0:
        logger.error(f"FAIL: {missing_scores[0][0]} players with 450+ min lack performance scores!")
        failed += 1
    else:
        logger.info("PASS: Performance scores cover all eligible players.")

    if failed == 0:
        logger.info("SUCCESS: All data quality checks passed.")
        return True
    else:
        logger.error(f"FAILED: {failed} checks failed.")
        return False

if __name__ == "__main__":
    success = run_checks()
    sys.exit(0 if success else 1)
