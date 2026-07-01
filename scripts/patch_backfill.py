import re

with open('scripts/backfill_bio.py', 'r') as f:
    code = f.read()

# Make it explicitly exit on 401 instead of eating the error
new_code = code.replace(
    '''
        except Exception as e:
            logger.error(f"Failed to process player {wyscout_id}: {e}")
            failed_count += 1
''',
    '''
        except Exception as e:
            if "Wyscout API error 40" in str(e):
                logger.error(f"FATAL: Token expired at player {wyscout_id} (error: {e}). STOPPING execution to allow token refresh.")
                break
            logger.error(f"Failed to process player {wyscout_id}: {e}")
            failed_count += 1
'''
)

with open('scripts/backfill_bio.py', 'w') as f:
    f.write(new_code)
