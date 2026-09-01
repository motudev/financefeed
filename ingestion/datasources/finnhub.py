import requests
import time
from typing import Dict, Any, List
from .base import BaseSource

class FinnhubMarketNews(BaseSource):
    def __init__(self, api_key: str, state_dir: str = "./state"):
        super().__init__(state_dir)
        self.api_key = api_key
        self.base_url = "https://finnhub.io/api/v1/news"
        # Finnhub allows 60 calls/min. Polling every 50 minutes (3000s) is extremely safe.
        self.poll_interval = 3000

    def get_source_name(self) -> str:
        return "finnhub_general_news"

    def get_poll_interval_seconds(self) -> int:
        return self.poll_interval

    def fetch_new_data(self, last_seen_time: int) -> List[Dict[str, Any]]:
        params = {"category": "general", "token": self.api_key}
        response = requests.get(self.base_url, params=params)
        
        # Handle rate limiting explicitly
        if response.status_code == 429:
            print("[Finnhub] Rate limit hit. Backing off.")
            return []
            
        response.raise_for_status()
        raw_news = response.json()

        if not last_seen_time:
            return raw_news # First time running, return everything

        # Finnhub returns 'datetime' as a UNIX timestamp. We only keep strictly newer articles.
        new_articles = [
            article for article in raw_news 
            if article.get("datetime", 0) > last_seen_time
        ]
        
        return new_articles

    def extract_newest_cursor(self, data: List[Dict[str, Any]]) -> int:
        # Find the maximum datetime in the fetched batch to save as our new state
        return max(article.get("datetime", 0) for article in data)