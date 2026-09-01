import json
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseSource(ABC):
    def __init__(self, state_dir: str = "./state"):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, f"{self.get_source_name()}_state.json")

    @abstractmethod
    def get_source_name(self) -> str:
        """Returns the unique identifier for this source."""
        pass

    @abstractmethod
    def get_poll_interval_seconds(self) -> int:
        """How often should the daemon sleep before calling this source again?"""
        pass

    @abstractmethod
    def fetch_new_data(self, last_seen_id_or_time: Any) -> List[Dict[str, Any]]:
        """The actual API call. Must return ONLY data newer than last_seen."""
        pass

    def load_state(self) -> Any:
        """Loads the last seen timestamp/ID from disk."""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                return json.load(f).get("last_seen")
        return None

    def save_state(self, current_seen: Any):
        """Saves the newest timestamp/ID to disk."""
        with open(self.state_file, "w") as f:
            json.dump({"last_seen": current_seen}, f)

    def execute_fetch(self) -> List[Dict[str, Any]]:
        """The main wrapper called by the daemon."""
        last_seen = self.load_state()
        new_data = self.fetch_new_data(last_seen)

        if new_data:
            # Assuming the source implements a way to find the newest timestamp in the batch
            newest_record = self.extract_newest_cursor(new_data)
            self.save_state(newest_record)

        return new_data

    @abstractmethod
    def extract_newest_cursor(self, data: List[Dict[str, Any]]) -> Any:
        """Parses the payload to find the highest timestamp/ID to save for next time."""
        pass