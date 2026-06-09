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
        try:
            return float(timestamp)  # handles numeric strings like '1704067260.0'
        except ValueError:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()


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

    def _iter_matching_records(self, topic):
        """Single pass over index yielding records that match the topic filter"""
        is_wildcard = topic.endswith("/#")
        topic_prefix = topic[:-2] if is_wildcard else None
        with open(self.index_path, "r") as f:
            for line in f:
                record = json.loads(line)
                record_topic = record.get("topic", "")
                if is_wildcard:
                    if not record_topic.startswith(topic_prefix):
                        continue
                else:
                    if record_topic != topic:
                        continue
                yield record

    def get_data_point(self, topic, timestamp=None, closest_past=True):
        if timestamp is None:
            raise ValueError("timestamp is required for get_data_point()")

        target_unix = self._to_unix(timestamp)
        records = []
        unique_topics = set()

        for record in self._iter_matching_records(topic):
            unique_topics.add(record.get("topic"))
            records.append(record)

        # Multiple topics matched — wildcard returned more than one topic
        if len(unique_topics) > 1:
            self.logger.info(
                f"Skipping get_data_point for topic '{topic}' because "
                f"{len(unique_topics)} topics were found."
            )
            return None

        if not records:
            return None

        # Check for exact match first
        for r in records:
            if float(r.get("timestamp")) == target_unix:
                return self._load_blob(r)

        # No exact match — find closest
        if closest_past:
            candidates = [r for r in records if float(r.get("timestamp")) < target_unix]
            best = max(candidates, key=lambda r: float(r.get("timestamp")), default=None)
        else:
            candidates = [r for r in records if float(r.get("timestamp")) > target_unix]
            best = min(candidates, key=lambda r: float(r.get("timestamp")), default=None)

        if best is None:
            raise FileNotFoundError(f"No datapoint found for topic '{topic}' near timestamp {timestamp}")

        return self._load_blob(best)

    def get_data_range(self, topic, start_time=None, end_time=None, page_size=None, page_token=None):
        if start_time is None or end_time is None:
            raise ValueError("start_time and end_time are required for get_data_range()")

        parsed_start = self._to_unix(start_time)
        parsed_end = self._to_unix(end_time)
        token_map = json.loads(page_token) if page_token else {}
        effective_page_size = page_size or 1000

        # 1. SINGLE PASS OVER INDEX — filter by topic and time range together
        topic_records = {}
        for record in self._iter_matching_records(topic):
            record_topic = record.get("topic")
            ts = float(record.get("timestamp"))
            stream_start = self._to_unix(token_map[record_topic]) if record_topic in token_map else parsed_start

            if ts <= stream_start or ts > parsed_end:
                continue

            if record_topic not in topic_records:
                topic_records[record_topic] = []
            topic_records[record_topic].append(record)

        if not topic_records:
            return {"data": [], "nextPageToken": None}

        self.logger.debug(f"Found {len(topic_records)} unique topics for '{topic}'")

        # 2. SORT AND TRIM TO PAGE SIZE BEFORE LOADING FILES
        all_records = [r for records in topic_records.values() for r in records]
        all_records.sort(key=lambda r: float(r.get("timestamp")))

        has_more = len(all_records) > effective_page_size
        page_records = all_records[:effective_page_size]

        # 3. LOAD BLOBS ONLY FOR THE PAGE
        page_blobs = []
        for record in page_records:
            try:
                page_blobs.append(self._load_blob(record))
            except FileNotFoundError as e:
                self.logger.warning(f"Skipping missing file: {e}")
                continue

        # 4. BUILD NEXT TOKEN MAP
        next_token_map = {}
        for blob in page_blobs:
            next_token_map[blob.topic] = blob.timestamp

        # Carry forward tokens for exhausted topics
        for t in token_map:
            if t not in next_token_map:
                next_token_map[t] = token_map[t]

        self.logger.info(f"get_data_range: topic={topic}, found={len(page_blobs)} blobs across {len(topic_records)} topics")
        return {
            "data": page_blobs,
            "nextPageToken": json.dumps(next_token_map) if has_more else None
        }

    def stream_blobs(self, topic, start_time=None, end_time=None):
        """Stream blobs within a time range"""
        parsed_start = self._to_unix(start_time) if start_time else None
        parsed_end = self._to_unix(end_time) if end_time else None
        for record in self._iter_matching_records(topic):
            ts = float(record.get("timestamp"))
            if parsed_start and ts <= parsed_start:
                continue
            if parsed_end and ts > parsed_end:
                continue
            try:
                yield self._load_blob(record)
            except FileNotFoundError as e:
                self.logger.warning(f"Skipping missing file: {e}")
                continue
                