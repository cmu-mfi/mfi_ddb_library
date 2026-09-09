import re
import sys
import time
import json
import threading
from typing import List, Optional, Any

import websocket
from pydantic import BaseModel, Field

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError


class WSMessage:
    """A lightweight message object to mimic MQTT's message structure for callback compatibility."""
    def __init__(self, topic: str, payload: Any):
        self.topic = topic
        self.payload = payload  # Can be str, bytes, or dict


class _WebSocket:
    def __init__(self, config: dict) -> None:
        super().__init__()
        self.ws_cfg = config

        ws_keys = ["ws_url"]
        if not all(key in self.ws_cfg for key in ws_keys):
            raise ConfigError(f"Config incomplete for WebSocket. Following keys needed: {ws_keys}")

        self.ws: websocket.WebSocketApp = None
        self._thread: threading.Thread = None
        self._running = False
        self._callbacks = {}  # topic -> callback function

    def disconnect(self):
        self._running = False
        if self.ws is not None:
            self.ws.close()
            print("Disconnected from WebSocket server")
        else:
            print("No WebSocket client to disconnect")
            
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def connect(self):
        ws_url = self.ws_cfg["ws_url"]
        headers = self.ws_cfg.get("headers", None)
        timeout = self.ws_cfg.get("timeout", 5.0)

        self.ws = websocket.WebSocketApp(
            ws_url,
            header=headers,
            on_open=self.__on_open,
            on_message=self.__on_message,
            on_error=self.__on_error,
            on_close=self.__on_close
        )

        self._running = True
        self._thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self._thread.start()

        start_time = time.time()
        time_elapsed = 0
        while not (hasattr(self.ws, "sock") and self.ws.sock and self.ws.sock.connected) and time_elapsed < timeout:
            print(f"Connecting to WebSocket server... {int(time_elapsed)}s")
            time.sleep(0.5)
            time_elapsed = time.time() - start_time

        if not (hasattr(self.ws, "sock") and self.ws.sock and self.ws.sock.connected):
            raise ConfigError(f"Could not connect to WebSocket server {ws_url} after {timeout} seconds.")

    def create_message_callback(self, topic: str, callback: callable):
        self._callbacks[topic] = callback
        print(f"Registered callback for WebSocket topic: {topic}")

    def __on_open(self, ws):
        print("WebSocket connection opened successfully.")
        # FIX: Use self.cfg (the full config) instead of self.ws_cfg to get the topics list
        for topic_cfg in self.cfg.get("topics", []):
            topic_name = topic_cfg["topic"]
            # Note: This subscription payload format is tailored for Aconity. 
            # For other machines, you may need to adjust this dictionary.
            subscribe_msg = {"action": "subscribe", "topic": topic_name}
            ws.send(json.dumps(subscribe_msg))
            print(f"Sent subscription request for topic: {topic_name}")

    def __on_message(self, ws, message: str):
        try:
            data = json.loads(message)
            topic = data.get("topic", "default")
            ws_msg = WSMessage(topic=topic, payload=message)
            
            if topic in self._callbacks:
                self._callbacks[topic](ws_msg)
            elif "default" in self._callbacks:
                self._callbacks["default"](ws_msg)
            else:
                print(f"Received message for unregistered topic '{topic}': {data}")
                
        except json.JSONDecodeError:
            print(f"Received non-JSON message on WebSocket: {message}")
        except Exception as e:
            print(f"Error processing WebSocket message: {e}")

    def __on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def __on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket closed: {close_status_code} - {close_msg}")


class _SCHEMA:
    class _WS(BaseModel):
        ws_url: str = Field(..., description="WebSocket URL (e.g., ws://192.168.1.50:9000)")
        headers: Optional[dict] = Field(None, description="Optional headers for WebSocket handshake")
        timeout: Optional[float] = Field(5.0, description="Timeout in seconds for connecting")

    class _TOPIC(BaseModel):
        component_id: str = Field(..., description="Identifier for the component")
        topic: str = Field(..., description="WebSocket topic/channel to subscribe to")
        trial_id: Optional[str] = Field(None, description="Trial ID for the component (optional)")

    class SCHEMA(BaseModel):
        ws: "_WS" = Field(..., description="Configuration for the WebSocket connection")
        trial_id: str = Field(..., description="Trial ID for the system.")
        queue_size: int = Field(10, description="Maximum number of messages to buffer.")
        topics: List["_TOPIC"] = Field(..., description="List of topics to subscribe to.")


class WebSocketDataAdapter(BaseDataAdapter, _WebSocket):
    NAME = "WebSocket"

    CONFIG_HELP = {
        "ws": {
            "ws_url": "WebSocket URL (e.g., ws://localhost:9000)",
            "headers": "(optional) Dictionary of headers for WebSocket handshake",
            "timeout": "(optional) Timeout in sec (default: 5.0s)",
        },
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "queue_size": "(optional) Max number of messages to buffer. (default: 10)",
        "topics": "List of topics. Required: ['component_id', 'topic'].",
    }

    CONFIG_EXAMPLE = {
        "adapter_name": "my_generic_ws_adapter",
        "ws": {"ws_url": "ws://192.168.1.50:9000", "timeout": 5.0},
        "trial_id": "trial_001",
        "queue_size": 10,
        "topics": [
            {"component_id": "machine_state", "topic": "State"},
            {"component_id": "machine_position", "topic": "Positioning"},
        ],
    }

    RECOMMENDED_TOPIC_FAMILY = "historian"
    SELF_UPDATE = True

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict):
        BaseDataAdapter.__init__(self, config)
        _WebSocket.__init__(self, config["ws"])

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
            print(f"Configured WebSocket adapter for component: {component_id} on topic: {topic_name}")

        print("WebSocketDataAdapter initialized and listening")

    def disconnect(self):
        _WebSocket.disconnect(self)
        return super().disconnect()

    def get_data(self):
        for component_id in self.component_ids:
            if len(self.buffer_data[component_id]) > 0:
                data = self.buffer_data[component_id].pop(0)
                self._data[component_id] = data

    def _topic_callback(self, message: WSMessage):
        topic = message.topic
        payload = message.payload

        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        payload = self.__autotype(payload)
        component_id = self.__get_component_from_topic(topic)
        
        print(f"Received message on topic '{topic}' for component '{component_id}'")

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
        raise ConfigError(f"Could not map WebSocket topic '{topic_name}' to a configured component_id.")