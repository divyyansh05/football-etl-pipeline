import sys
import os
from pathlib import Path

# Add project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.connection import get_conn

def run_fix():
    print("Connecting to database...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. & 2. Delete duplicates keeping the highest id
            print("Identifying and deleting duplicates...")
            
            # Using basic delete strategy keeping max id for each composite group
            cur.execute("""
                WITH duplicates AS (
                    SELECT id, 
                           ROW_NUMBER() OVER (
                               PARTITION BY player_id, match_date, competition_name 
                               ORDER BY id DESC
                           ) as row_num
                    FROM player_match_stats
                )
                DELETE FROM player_match_stats 
                WHERE id IN (
                    SELECT id FROM duplicates WHERE row_num > 1
                );
            """)
            deleted_count = cur.rowcount
            print(f"Deleted {deleted_count} duplicate rows.")

            # 3. Alter table to swap unique constraint
            print("Finding old UNIQUE constraint name...")
            cur.execute("""
                SELECT conname 
                FROM pg_constraint 
                WHERE conrelid = 'player_match_stats'::regclass 
                AND contype = 'u';
            """)
            constraints = cur.fetchall()
            
            for (conname,) in constraints:
                print(f"Dropping old constraint: {conname}")
                cur.execute(f"ALTER TABLE player_match_stats DROP CONSTRAINT {conname};")

            new_constraint_name = "player_match_stats_player_match_comp_key"
            print(f"Adding new UNIQUE constraint: {new_constraint_name}")
            cur.execute(f"""
                ALTER TABLE player_match_stats 
                ADD CONSTRAINT {new_constraint_name} 
                UNIQUE (player_id, match_date, competition_name);
            """)
            
            # Verify it
            cur.execute("""
                SELECT conname, pg_get_constraintdef(oid) 
                FROM pg_constraint 
                WHERE conname = %s;
            """, (new_constraint_name,))
            res = cur.fetchone()
            print(f"Verification: {res[0]} -> {res[1]}")

        conn.commit()
        print("Done!")

if __name__ == '__main__':
    run_fix()
