import io
import time
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient
from datasources.finnhub import FinnhubMarketNews
from datasources.base import BaseSource

VOLUME_PATH = "/Volumes/workspace/dev_roosxfabian_financefeed/raw_financial_news/"

class DatabricksIngestor:
    def __init__(self):
        self.w = WorkspaceClient()
        
    def upload(self, source_name: str, data: list):
        if not data:
            return
            
        file_name = f"{source_name}_{int(time.time())}.json"
        remote_path = f"{VOLUME_PATH}{file_name}"
        
        # 1. Convert the JSON list to bytes directly in memory
        json_bytes = json.dumps(data).encode("utf-8")
        binary_data = io.BytesIO(json_bytes)
        
        print(f"[{datetime.now().isoformat()}] Uploading {len(data)} new records to {remote_path}...")
        
        # 2. Upload the raw byte stream directly
        self.w.files.upload(remote_path, binary_data, overwrite=True)

def run_daemon(sources: list[BaseSource], ingestor: DatabricksIngestor):
    last_run_times = {source.get_source_name(): 0 for source in sources}
    
    print("Starting Ingestion Daemon...")
    
    while True:
        current_time = time.time()
        
        for source in sources:
            source_name = source.get_source_name()
            time_since_last_run = current_time - last_run_times[source_name]
            
            # Check if enough time has passed based on the source's configured interval
            if time_since_last_run >= source.get_poll_interval_seconds():
                try:
                    print(f"Polling {source_name}...")
                    new_data = source.execute_fetch()
                    
                    if new_data:
                        ingestor.upload(source_name, new_data)
                    else:
                        print(f"No new data for {source_name}.")
                        
                except Exception as e:
                    print(f"Error fetching from {source_name}: {e}")
                
                # Update the last run time
                last_run_times[source_name] = time.time()
                
        # Sleep a short burst before checking intervals again
        time.sleep(10)

if __name__ == "__main__":
    import os
    # Load API keys from environment variables on your VPS
    FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "your_key_here")
    
    # Initialize sources
    sources = [
        FinnhubMarketNews(api_key=FINNHUB_KEY)
        # You can easily add YahooFinanceRSS() or SEC_EdgarSource() here later
    ]
    
    ingestor = DatabricksIngestor()
    
    # Start the infinite loop
    run_daemon(sources, ingestor)