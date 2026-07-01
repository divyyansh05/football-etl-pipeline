"""
football-data-platform FastAPI
Read-only API serving football data to all consumers.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from api.routers import players, teams, stats

app = FastAPI(
    title='football-data-platform',
    description='Multi-consumer football data API',
    version='1.0.0',
    docs_url='/api/v1/docs',
    redoc_url='/api/v1/redoc',
    openapi_url='/api/v1/openapi.json'
)

# CORS configuration
# In production, this should be restricted to ScoutIQ domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['GET', 'OPTIONS'],
    allow_headers=['*'],
)

# Exception handler for consistent error responses
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "detail": str(exc)}
    )

app.include_router(players.router)
app.include_router(teams.router)
app.include_router(stats.router)


@app.get('/api/health')
def health():
    from database.connection import query
    try:
        # Check players table count as a quick DB health check
        result = query('SELECT COUNT(*) FROM players')
        count = result[0][0] if result else 0
        return {
            'status': 'ok',
            'players_in_db': count,
            'db': 'football_platform'
        }
    except Exception as e:
        return {'status': 'error', 'detail': str(e)}
