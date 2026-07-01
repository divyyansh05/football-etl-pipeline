"""
Auto-login to Wyscout and manage session token.
Never requires manual token copying.
"""
import os
import logging
import requests
from pathlib import Path
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv, set_key

load_dotenv()
logger = logging.getLogger(__name__)

BASE = 'https://searchapi.wyscout.com'
ENV_FILE = Path(__file__).parent.parent.parent / '.env'
TOKEN_CACHE = Path(__file__).parent / '.token_cache'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/146.0.0.0 Safari/537.36',
    'Origin': 'https://wyscout.hudl.com',
    'Referer': 'https://wyscout.hudl.com/',
}


def _is_valid(token: str) -> bool:
    if not token:
        return False
    try:
        r = requests.get(
            f'{BASE}/api/v1/team_stats/teams/2020',
            params={'token': token,
                    'groupId': os.getenv('WYSCOUT_GROUP_ID', '1059060'),
                    'subgroupId': os.getenv('WYSCOUT_SUBGROUP_ID', '93476'),
                    'lang': 'en'},
            headers=HEADERS, timeout=10
        )
        return r.status_code == 200
    except Exception:
        return False


def _login_and_extract() -> str:
    email = os.getenv('WYSCOUT_EMAIL')
    password = os.getenv('WYSCOUT_PASSWORD')
    if not email or not password:
        raise ValueError(
            'WYSCOUT_EMAIL and WYSCOUT_PASSWORD must be set in .env')

    logger.info(f'Logging into Wyscout as {email}...')

    import time as _time

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            executable_path='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = browser.new_context()
        page = context.new_page()

        page.goto('https://wyscout.hudl.com/', timeout=30000)
        page.wait_for_load_state('networkidle')

        # Hudl Auth0 login — two-step flow at identity.hudl.com
        # Step 1: Enter email/username and click Continue
        try:
            page.fill('input.u-input', email, timeout=20000, force=True)
        except Exception:
            # Fallback: force it via raw JavaScript
            page.evaluate(f"document.querySelector('input.u-input').value = '{email}'")
        
        page.click('button:has-text("Continue")', force=True)

        # Step 2: Wait for password field to become visible
        page.wait_for_selector('input[type="password"]', timeout=20000)
        try:
            page.fill('input[type="password"]', password, force=True)
        except Exception:
            page.evaluate(f"document.querySelector('input[type=\"password\"]').value = '{password}'")
            
        page.click('button:has-text("Continue")', force=True)

        # Wait for redirect back to wyscout app
        page.wait_for_url('**/app/**', timeout=30000)
        page.wait_for_load_state('networkidle')
        _time.sleep(3)

        # Handle "Multiple access attempt" — click Force login
        force_btn = page.query_selector('button:has-text("Force login")')
        if force_btn:
            logger.info('Multiple access detected — forcing login...')
            force_btn.click()
            # Wait for networkidle after force login
            page.wait_for_load_state('networkidle')

        # Poll for aengine_dtk cookie — Wyscout sets it asynchronously via JS
        # after session initialization, so networkidle is not sufficient.
        token = None
        cookie_dict = {}
        logger.info('Polling for aengine_dtk cookie (up to 30s)...')
        for attempt in range(30):
            _time.sleep(1)
            cookies = context.cookies()
            cookie_dict = {c['name']: c['value'] for c in cookies}
            token = cookie_dict.get('aengine_dtk')
            if token:
                logger.info(f'aengine_dtk found after {attempt + 1}s')
                break
            if attempt % 5 == 4:
                logger.info(f'  Still waiting... ({attempt + 1}s elapsed). '
                            f'Available: {list(cookie_dict.keys())}')

        browser.close()

    if not token:
        raise ValueError(
            f'aengine_dtk not found after 30s. Available cookies: '
            f'{list(cookie_dict.keys())}')

    # Save to .env and cache
    set_key(str(ENV_FILE), 'WYSCOUT_TOKEN', token)
    TOKEN_CACHE.write_text(token)
    logger.info(f'Token extracted and saved: {token[:20]}...')
    return token


def get_token() -> str:
    """Get valid token. Auto-refreshes via login if expired."""
    # 1. Try .env token
    load_dotenv(override=True)
    token = os.getenv('WYSCOUT_TOKEN', '')
    if _is_valid(token):
        return token

    # 2. Try cache file
    if TOKEN_CACHE.exists():
        token = TOKEN_CACHE.read_text().strip()
        if _is_valid(token):
            set_key(str(ENV_FILE), 'WYSCOUT_TOKEN', token)
            return token

    # 3. Login and get fresh token
    return _login_and_extract()


def refresh_token() -> str:
    """Force fresh login regardless of current token state."""
    return _login_and_extract()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    token = get_token()
    print(f'Token ready: {token[:30]}...')
    print('Token is valid:', _is_valid(token))
