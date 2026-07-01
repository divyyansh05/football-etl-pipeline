"""
SofaScore API Client.
Interfaces with public SofaScore endpoints to fetch player metadata and IDs.
Spoofs User-Agent to bypass basic Cloudflare protection.
"""
import json
import time
import logging
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

BASE_URL = 'https://api.sofascore.com/api/v1'

class SofaScoreClient:
    def __init__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=False,
            executable_path='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
        )
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.page = self.context.new_page()
        
        logger.info("Opening main SofaScore hub to establish legitimate browser context...")
        try:
            self.page.goto('https://www.sofascore.com', wait_until='domcontentloaded')
            # Wait 5 seconds to ensure Cloudflare challenge completes and cookie is set
            time.sleep(5)
            logger.info("DOM established. Ready to inject API queries.")
        except Exception as e:
            logger.error(f"Failed to load hub: {e}")

    def close(self):
        self.browser.close()
        self.playwright.stop()

    def _get_json(self, url: str) -> dict:
        time.sleep(1.5)  # Respect rate limits
        try:
            # IN-BROWSER FETCH INJECTION:
            # Instead of navigating to the API (which triggers Cloudflare blocks),
            # we execute an async fetch() from inside the already-cleared www.sofascore.com DOM.
            # This perfectly attaches all cf_clearance cookies, Referer, and Origin headers.
            data = self.page.evaluate('''async (targetUrl) => {
                const response = await fetch(targetUrl);
                return await response.json();
            }''', url)
            
            if 'error' in data:
                logger.warning(f"SofaScore API returned suspicious JSON: {str(data)[:200]}")
                
            return data
        except Exception as e:
            logger.error(f"In-browser fetch failed for {url}: {e}")
            return {}

    def search_player(self, name: str) -> list:
        data = self._get_json(f"{BASE_URL}/search/all?q={name}")
        results = data.get('results', [])
        return [res['entity'] for res in results if res.get('type') == 'player']

    def get_player_info(self, sofascore_id: int) -> dict:
        data = self._get_json(f"{BASE_URL}/player/{sofascore_id}")
        return data.get('player', {})

    def get_player_season_rating(self, sofascore_id: int, tournament_id: int, season_id: int) -> float:
        data = self._get_json(f"{BASE_URL}/player/{sofascore_id}/unique-tournament/{tournament_id}/season/{season_id}/statistics/overall")
        return data.get('statistics', {}).get('rating', None)
