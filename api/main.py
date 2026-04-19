"""
football-data-platform FastAPI
Read-only API serving football data to all consumers.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title='football-data-platform',
    description='Multi-consumer football data API',
    version='1.0.0'
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['GET'],
    allow_headers=['*'],
)


@app.get('/api/health')
def health():
    from database.connection import query
    try:
        result = query('SELECT COUNT(*) FROM player_match_stats')
        count = result[0][0] if result else 0
        return {
            'status': 'ok',
            'player_match_rows': count,
            'db': 'football_platform'
        }
    except Exception as e:
        return {'status': 'error', 'detail': str(e)}
