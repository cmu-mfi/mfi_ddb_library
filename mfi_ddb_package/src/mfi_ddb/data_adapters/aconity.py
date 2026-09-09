import asyncio
import json
import re
import sys
import threading
import time
from typing import List, Optional, Any

from pydantic import BaseModel, Field

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError

try:
    from AconitySTUDIOpy.AconitySTUDIO_client import AconitySTUDIO_client as AconityClient
except ImportError:
    AconityClient = None


class _AconityNative:
    def __init__(self, config: dict) -> None:
        if AconityClient is None:
            raise ImportError("AconitySTUDIOpy is not installed. Please install the official Aconity3D Python Client.")
            
        self.aconity_cfg = config
        self._thread: threading.Thread = None
        self._loop: asyncio.AbstractEventLoop = None
        self._client = None
        self._callbacks = {}
        self._running = False

    def _run_async_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._async_main())

    async def _async_main(self):
        login_data = {
            'rest_url': self.aconity_cfg.get("rest_url", "http://localhost:9000"),
            'ws_url': self.aconity_cfg.get("ws_url", "ws://localhost:9000"),
            'email': self.aconity_cfg.get("email"),
            'password': self.aconity_cfg.get("password")
        }
        studio_version = self.aconity_cfg.get("studio_version", 3)
        
        print(f"[Aconity] Connecting to server at {login_data['rest_url']}...")
        self._client = await AconityClient.create(login_data, studio_version=studio_version)
        print("[Aconity] Connected successfully.")
        
        # FIX: Use self.cfg (the full config) to get the topics list, not self.aconity_cfg
        for topic_cfg in self.cfg.get("topics", []):
            topic_name = topic_cfg["topic"]
            topic_type = topic_cfg.get("type", "data")
            
            def make_cb(t_name):
                def cb(topic, msg):
                    if t_name in self._callbacks:
                        class MockMsg:
                            def __init__(self, t, p):
                                self.topic = t
                                self.payload = json.dumps(p) if isinstance(p, dict) else str(p)
                        self._callbacks[t_name](MockMsg(t_name, msg))
                return cb

            cb_func = make_cb(topic_name)
            self._client.data.add_processor([topic_name], cb_func)
            
            if topic_type == "event":
                await self._client.data.subscribe_event_topic(topic_name)
            else:
                await self._client.data.subscribe_data_topic(topic_name)
            print(f"[Aconity] Subscribed to {topic_type} topic: '{topic_name}'")

        while self._running:
            await asyncio.sleep(1)
            
        if self._client:
            await self._client.close()

    def connect(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._thread.start()
        
        timeout = self.aconity_cfg.get("timeout", 10.0)
        start_time = time.time()
        while self._client is None and (time.time() - start_time) < timeout:
            time.sleep(0.5)
            
        if self._client is None:
            raise ConfigError(f"Could not connect to AconitySTUDIO server within {timeout} seconds.")

    def create_message_callback(self, topic: str, callback: callable):
        self._callbacks[topic] = callback
        
    def disconnect(self):
        self._running = False
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        print("[Aconity] Disconnected.")


class _SCHEMA:
    class _ACONITY(BaseModel):
        rest_url: str = Field("http://localhost:9000", description="REST API URL")
        ws_url: str = Field("ws://localhost:9000", description="WebSocket URL")
        email: str = Field(..., description="Email for AconitySTUDIO login")
        password: str = Field(..., description="Password for AconitySTUDIO login")
        studio_version: Optional[int] = Field(3, description="AconitySTUDIO API version")
        timeout: Optional[float] = Field(10.0, description="Timeout in seconds")

    class _TOPIC(BaseModel):
        component_id: str = Field(..., description="Identifier for the component")
        topic: str = Field(..., description="Aconity topic name (e.g., 'State', 'Positioning', 'task')")
        type: Optional[str] = Field("data", description="Topic type: 'data' or 'event'")
        trial_id: Optional[str] = Field(None, description="Trial ID (optional)")

    class SCHEMA(BaseModel):
        aconity: "_ACONITY" = Field(..., description="Configuration for the Aconity connection")
        trial_id: str = Field(..., description="Trial ID for the system.")
        queue_size: int = Field(10, description="Maximum number of messages to buffer.")
        topics: List["_TOPIC"] = Field(..., description="List of Aconity topics to subscribe to.")


class AconityDataAdapter(BaseDataAdapter, _AconityNative):
    NAME = "Aconity"

    CONFIG_HELP = {
        "aconity": {
            "rest_url": "REST API URL (e.g., http://192.168.1.50:9000)",
            "ws_url": "WebSocket URL (e.g., ws://192.168.1.50:9000)",
            "email": "Email for AconitySTUDIO login",
            "password": "Password for AconitySTUDIO login",
            "studio_version": "(optional) API version (default: 3)",
            "timeout": "(optional) Connection timeout in seconds (default: 10.0)",
        },
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "queue_size": "(optional) Max number of messages to buffer. (default: 10)",
        "topics": "List of topics. Required: ['component_id', 'topic', 'type'].",
    }

    CONFIG_EXAMPLE = {
        "adapter_name": "my_aconity_adapter",
        "aconity": {
            "rest_url": "http://192.168.1.50:9000",
            "ws_url": "ws://192.168.1.50:9000",
            "email": "admin@aconity3d.com",
            "password": "passwd",
            "studio_version": 3
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "topics": [
            {"component_id": "aconity_state", "topic": "State", "type": "data"},
            {"component_id": "aconity_positioning", "topic": "Positioning", "type": "data"},
            {"component_id": "aconity_tasks", "topic": "task", "type": "event"},
        ],
    }

    RECOMMENDED_TOPIC_FAMILY = "historian"
    SELF_UPDATE = True

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict):
        BaseDataAdapter.__init__(self, config)
        _AconityNative.__init__(self, config["aconity"])

        self.connect()

        self.buffer_data = {}
        self.queue_size = self.cfg.get("queue_size", 10)

        for topic_cfg in self.cfg["topics"]:
            component_id = topic_cfg["component_id"]
            topic_name = topic_cfg["topic"]
            if "trial_id" not in topic_cfg:
                topic_cfg["trial_id"] = self.cfg["trial_id"]

            self.buffer_data[component_id] = []
            self._data[component_id] = {}
            self.attributes[component_id] = topic_cfg
            self.component_ids.append(component_id)

            self.create_message_callback(topic_name, self._topic_callback)
            print(f"[Aconity] Configured component '{component_id}' for topic '{topic_name}'")

        print("[Aconity] AconityDataAdapter initialized and streaming data.")

    def disconnect(self):
        _AconityNative.disconnect(self)
        return super().disconnect()

    def get_data(self):
        for component_id in self.component_ids:
            if len(self.buffer_data[component_id]) > 0:
                data = self.buffer_data[component_id].pop(0)
                self._data[component_id] = data

    def _topic_callback(self, message):
        topic = message.topic
        payload = message.payload

        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        payload = self.__autotype(payload)
        component_id = self.__get_component_from_topic(topic)
        
        if topic != "Positioning": # Prevent console spam from high-frequency positioning data
            print(f"[Aconity] Received data on '{topic}' for component '{component_id}'")

        data = {}
        subscription_topic = self.attributes[component_id]["topic"]

        if subscription_topic.split("/")[-1] == "#":
            subtopic = topic[len(subscription_topic) - 1 :]
            if subtopic.startswith("/"):
                subtopic = subtopic[1:]
        else:
            subtopic = "data"

        if not isinstance(payload, dict):
            data[subtopic] = payload
        else:
            data = self.__extract_key_value(payload, subtopic)

        if len(self.buffer_data[component_id]) >= self.queue_size:
            self.buffer_data[component_id].pop(0)

        self.buffer_data[component_id].append(data)
        self._notify_observers({component_id: data})

    def __autotype(self, value: Any):
        if isinstance(value, (int, float, dict, list, bool)):
            return value
        if isinstance(value, str):
            value = value.strip()
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
            for cast in (int, float, eval):
                try:
                    if cast is eval:
                        return eval(value.replace("true", "True").replace("false", "False"))
                    else:
                        if cast is int and "." in value:
                            return float(value)
                        return cast(value)
                except Exception:
                    continue
        return value

    def __extract_key_value(self, data_item: Any, data_item_key: str):
        if len(data_item_key) > 0 and data_item_key[0] == "/":
            data_item_key = data_item_key[1:]
        if isinstance(data_item, dict):
            extracted_data = {}
            for key in data_item:
                extracted_data.update(self.__extract_key_value(data_item[key], f"{data_item_key}/{key}"))
            return extracted_data
        elif isinstance(data_item, list):
            extracted_data = {}
            for i, item in enumerate(data_item):
                extracted_data.update(self.__extract_key_value(item, f"{data_item_key}_{i}"))
            return extracted_data
        else:
            return {data_item_key: self.__autotype(data_item)}

    def __get_component_from_topic(self, topic_name: str):
        for component_id, attr in self.attributes.items():
            pattern = re.escape(attr["topic"]).replace(r"/\#", r"(?:/?.*)?")
            if re.fullmatch(pattern, topic_name):
                return component_id
        raise ConfigError(f"Could not map Aconity topic '{topic_name}' to a configured component_id.")