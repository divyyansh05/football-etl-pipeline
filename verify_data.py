import logging
import sys
from database.connection import get_db
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_db():
    try:
        db = get_db()
        
        # 1. Check Tables
        print("\n--- Verifying Tables ---")
        tables = db.execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        table_names = [t[0] for t in tables]
        required = ['matches', 'players', 'teams', 'team_match_stats', 'leagues', 'seasons']
        
        missing = [t for t in required if t not in table_names]
        if missing:
            print(f"❌ Missing tables: {missing}")
            sys.exit(1)
        else:
            print("✅ All core tables present.")
            
        # 2. Check Data
        print("\n--- Checking Data Counts ---")
        counts = {}
        for t in required:
            res = db.execute_query(f"SELECT COUNT(*) FROM {t}")
            counts[t] = res[0][0]
            print(f"{t}: {counts[t]}")
            
        if counts['matches'] > 0:
            print("\n✅ Data successfully ingested!")
        else:
            print("\n⚠️ Database tables exist but are empty.")
            
    except Exception as e:
        print(f"\n❌ Connection/Verification Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    verify_db()
