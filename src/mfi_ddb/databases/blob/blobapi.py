import logging
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone



# -------- DATA MODEL --------
@dataclass
class Blob:
    file_id: str
    topic: str
    timestamp: int
    file_type: str
    file: bytes

# -------- API --------
class BlobAPI:
    def __init__(self, blob_dir, index_path):
        self.blob_dir = blob_dir
        self.index_path = index_path
        self.logger = logging.getLogger(__name__)

    # -------- CORE HELPERS --------
    def _to_unix(self, timestamp):
        """Convert ISO string or Unix int/float to Unix float"""
        if isinstance(timestamp, (int, float)):
            return float(timestamp)
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()

    def _iter_records(self):
        """Iterate over all records in index.jsonl"""
        with open(self.index_path, "r") as f:
            for line in f:
                yield json.loads(line)

    def _filter_records(self, topic, user_id=None, start_time=None, end_time=None):
        """Filter records by topic, optional user_id, and optional time range (Unix floats)"""
        for record in self._iter_records():
            if record.get("topic") != topic:
                continue
            if user_id is not None:
                self.logger.debug(f"user_id filtering not yet implemented, ignoring user_id={user_id}")
            if start_time or end_time:
                record_time = float(record.get("timestamp"))
                if start_time and record_time < start_time:
                    continue
                if end_time and record_time > end_time:
                    continue
            yield record

    def _load_blob(self, record):
        """Load file from disk and return Blob object"""
        file_id = record.get("file_id")
        file_type = record.get("file_type", "")
        if not file_type.startswith("."):
            file_type = f".{file_type}"
        file_path = os.path.join(self.blob_dir, f"{file_id}{file_type}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        return Blob(
            file_id=file_id,
            topic=record.get("topic"),
            timestamp=record.get("timestamp"),
            file_type=record.get("file_type"),
            file=file_bytes
        )

    # -------- PUBLIC METHODS --------
    def get_data_point(self, topic, user_id=None, timestamp=None):
        """Return a single blob matching topic + timestamp"""
        if timestamp is None:
            raise ValueError("timestamp is required for get_data_point()")
        target_unix = self._to_unix(timestamp)
        for record in self._filter_records(topic, user_id=user_id):
            if float(record.get("timestamp")) == target_unix:
                return self._load_blob(record)
        self.logger.warning(f"No blob found for topic={topic}, timestamp={timestamp}")
        return None

    def get_data_range(self, topic, user_id=None, start_time=None, end_time=None, page_size=None, page_token=None):
        """Return blobs within a time range with pagination support"""
        if start_time is None or end_time is None:
            raise ValueError("start_time and end_time are required for get_data_range()")

        parsed_start = self._to_unix(page_token if page_token else start_time)
        parsed_end = self._to_unix(end_time)

        if parsed_start >= parsed_end:
            raise ValueError("start_time must be less than end_time")

        blobs = []
        for record in self._filter_records(topic, user_id=user_id, start_time=parsed_start, end_time=parsed_end):
            try:
                blobs.append(self._load_blob(record))
            except FileNotFoundError as e:
                self.logger.warning(f"Skipping missing file: {e}")
                continue
            if page_size and len(blobs) >= page_size:
                break

        next_page_token = ""
        if page_size and len(blobs) == page_size:
            next_page_token = blobs[-1].timestamp
            self.logger.info(f"Page size reached, next_page_token={next_page_token}")

        self.logger.info(f"get_data_range: topic={topic}, user_id={user_id}, found={len(blobs)} blobs")
        return {"data": blobs, "nextPageToken": next_page_token}

    def stream_blobs(self, topic, user_id=None, start_time=None, end_time=None):
        """Stream blobs within a time range"""
        parsed_start = self._to_unix(start_time) if start_time else None
        parsed_end = self._to_unix(end_time) if end_time else None
        for record in self._filter_records(topic, user_id=user_id, start_time=parsed_start, end_time=parsed_end):
            try:
                yield self._load_blob(record)
            except FileNotFoundError as e:
                self.logger.warning(f"Skipping missing file: {e}")
                continue