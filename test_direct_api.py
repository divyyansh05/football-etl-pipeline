"""
Test API Key against Direct API-Football Endpoint.

Usage:
    Ensure API_FOOTBALL_KEY is set in environment or .env file.
    python test_direct_api.py
"""
import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('API_FOOTBALL_KEY')
BASE_URL = "https://v3.football.api-sports.io"

def test_connection():
    if not API_KEY:
        print("ERROR: API_FOOTBALL_KEY environment variable not set.")
        print("Please set it in your .env file or export it:")
        print("  export API_FOOTBALL_KEY=your_api_key_here")
        return False

    HEADERS = {
        "x-apisports-key": API_KEY,
    }

    url = f"{BASE_URL}/status"
    try:
        print(f"Testing connection to {url}...")
        response = requests.get(url, headers=HEADERS)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")

        if response.status_code == 200:
            print("\nSUCCESS! Key works with Direct API.")
            return True
        else:
            print("\nFAILED with Direct API.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_connection()
