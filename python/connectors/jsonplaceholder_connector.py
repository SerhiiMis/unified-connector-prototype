import requests 
from .base import Connector

class JsonPlaceholderConnector(Connector):
    """Fetches /posts from JSONPlaceholder for demo purposes."""

    API_URL = "https://jsonplaceholder.typicode.com/posts"

    def fetch(self):
        response = requests.get(self.API_URL)
        response.raise_for_status()
        return response.json()
    
    def normalize(self, raw_data):
        # Map into our unified schema fields: source, record_id, payload
        return [
            {
                "sorce": "jsonplaceholder",
                "record_id": item["id"],
                "payload": item
                }
                for item in raw_data
        ]
    

