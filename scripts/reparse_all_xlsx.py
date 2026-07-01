import os
import glob
from pathlib import Path
import logging
from etl.wyscout_loader import WyscoutLoader
import database.connection as db

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Resetting loaded_files tracking so we can re-parse all files...")
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM loaded_files WHERE file_type = 'player'")
        conn.commit()

    files = glob.glob('data/raw/wyscout/players/**/*.xlsx', recursive=True)
    logger.info(f"Found {len(files)} player xlsx files. Beginning re-parse for 331 columns...")

    loader = WyscoutLoader()
    total_parsed = 0
    total_rows = 0

    for i, f in enumerate(files, 1):
        path = Path(f)
        # Extract wyscout_id from filename pattern: 12345_Name.xlsx
        try:
            wyscout_id = int(path.stem.split('_')[0])
            player_name = path.stem.split('_', 1)[1].replace('_', ' ')
        except Exception:
            logger.warning(f"Skipping malformed filename: {path.name}")
            continue

        rows = loader.load_player_xlsx(path, wyscout_id, player_name)
        total_rows += rows
        total_parsed += 1

        if i % 100 == 0:
            logger.info(f"Progress: {i}/{len(files)} processed. Total rows inserted: {total_rows}")

    logger.info(f"COMPLETE. Processed {total_parsed} files, {total_rows} match rows pushed to database.")

if __name__ == '__main__':
    main()
