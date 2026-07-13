import time
from typing import List, Optional

from asyncua import ua
from asyncua.sync import Client
from pydantic import BaseModel, Field, model_validator

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError

_MAX_BROWSE_DEPTH = 20  # safety net against reference cycles in a malformed address space


class _SubHandler:
    """
    Receives OPC UA data-change notifications and buffers them on the adapter.
    """

    def __init__(self, adapter: "OpcuaDataAdapter") -> None:
        self.adapter = adapter

    def datachange_notification(self, node, val, data):
        node_id = node.nodeid.to_string()
        try:
            component_id, key = self.adapter._node_map[node_id]
        except KeyError:
            return

        payload = {key: val}
        buffer = self.adapter.buffer_data[component_id]
        if len(buffer) >= self.adapter.queue_size:
            buffer.pop(0)
        buffer.append(payload)

        self.adapter.last_updated[component_id] = time.time()
        self.adapter._notify_observers({component_id: payload})

    def status_change_notification(self, status):
        self.adapter.logger.warning(f"OPC UA subscription status changed: {status}")


class _SCHEMA:
    class _NODE(BaseModel):
        component_id: str = Field(..., description="Identifier for the component this node belongs to")
        node_id: Optional[str] = Field(None, description="OPC UA node id of a single variable to subscribe to, e.g. 'ns=2;i=2'. Exactly one of 'node_id' or 'browse_root' must be set.")
        browse_root: Optional[str] = Field(None, description="OPC UA node id of an object to recursively browse; every Variable node found underneath is subscribed to automatically, with 'key' derived from its browse path relative to this root. Exactly one of 'node_id' or 'browse_root' must be set.")
        key: Optional[str] = Field(None, description="Data key name to store the node value under. Only used with 'node_id' (defaults to node_id); ignored for 'browse_root' entries.")
        trial_id: Optional[str] = Field(None, description="Trial ID for the component (optional)")

        @model_validator(mode="after")
        def _check_exactly_one_source(self):
            if bool(self.node_id) == bool(self.browse_root):
                raise ValueError("Exactly one of 'node_id' or 'browse_root' must be set for each node entry.")
            return self

    class _OPCUA(BaseModel):
        endpoint: str = Field(..., description="OPC UA server endpoint URL, e.g. opc.tcp://localhost:4840/freeopcua/server/")
        publishing_interval: Optional[float] = Field(500, description="Subscription publishing interval in ms (default: 500)")
        username: Optional[str] = Field(None, description="Username for OPC UA server authentication")
        password: Optional[str] = Field(None, description="Password for OPC UA server authentication")
        timeout: Optional[float] = Field(5.0, description="Timeout in seconds for connecting to the OPC UA server (default: 5.0)")

    class SCHEMA(BaseModel):
        opcua: "_OPCUA" = Field(..., description="Configuration for the OPC UA server connection")
        trial_id: str = Field(..., description="Trial ID for the system. No spaces or special characters allowed.")
        queue_size: int = Field(10, description="Maximum number of buffered updates per component before the oldest is dropped. If the buffer is full, the oldest message will be removed. (default: 10)")
        nodes: List["_NODE"] = Field(..., description="List of OPC UA nodes to subscribe to. Each node needs a 'component_id' and 'node_id'. Optionally 'key' and 'trial_id' can be provided.")


class OpcuaDataAdapter(BaseDataAdapter):

    NAME = "OPCUA"

    CONFIG_HELP = {
        "opcua": {
            "endpoint": "OPC UA server endpoint URL, e.g. opc.tcp://localhost:4840/freeopcua/server/",
            "publishing_interval": "(optional) Subscription publishing interval in ms (default: 500)",
            "username": "(optional) Username for OPC UA server authentication",
            "password": "(optional) Password for OPC UA server authentication",
            "timeout": "(optional) Timeout in seconds for connecting to the OPC UA server (default: 5.0)",
        },
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "queue_size": "(optional) Maximum number of buffered updates per component before the oldest is dropped. (default: 10)",
        "nodes": "List of OPC UA nodes to subscribe to. Each node needs a 'component_id' and exactly one of 'node_id' (a single variable, with optional 'key') or 'browse_root' (an object to recursively browse; every variable found underneath is auto-subscribed, keyed by its browse path). Optionally 'trial_id' can be provided.",
    }

    CONFIG_EXAMPLE = {
        "opcua": {
            "endpoint": "opc.tcp://localhost:4840/freeopcua/server/",
            "publishing_interval": 500,
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "nodes": [
            {
                "component_id": "robot-arm-1",
                "node_id": "ns=2;s=RobotArm1.Temperature",
                "key": "temperature",
            },
            {
                "component_id": "machine-a",
                "browse_root": "ns=2;s=MachineA",
                "trial_id": "trial_001",
            },
        ],
    }

    RECOMMENDED_TOPIC_FAMILY = "historian"

    SELF_UPDATE = True  # This data adapter will update the data by itself in a separate thread.

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict):
        super().__init__(config)

        self.cfg = config
        opcua_cfg = self.cfg["opcua"]

        self.queue_size = self.cfg.get("queue_size", 10)
        self.buffer_data = {}
        self._node_map = {}  # node_id string -> (component_id, key)

        self.client = Client(url=opcua_cfg["endpoint"], timeout=opcua_cfg.get("timeout", 5.0))
        if opcua_cfg.get("username"):
            self.client.set_user(opcua_cfg["username"])
        if opcua_cfg.get("password"):
            self.client.set_password(opcua_cfg["password"])

        try:
            self.client.connect()
        except Exception as e:
            raise ConfigError(f"Could not connect to OPC UA server {opcua_cfg['endpoint']}: {e}")

        # POPULATE COMPONENT IDS, ATTRIBUTES AND INITIAL (BIRTH) DATA
        current_time = time.time()
        nodes = []
        for node_cfg in self.cfg["nodes"]:
            component_id = node_cfg["component_id"]
            trial_id = node_cfg.get("trial_id", self.cfg["trial_id"])

            if component_id not in self.component_ids:
                self.component_ids.append(component_id)
                self._data[component_id] = {}
                self.buffer_data[component_id] = []
                self.attributes[component_id] = {"trial_id": trial_id}
                self.last_updated[component_id] = current_time

            if node_cfg.get("browse_root"):
                root = self.client.get_node(node_cfg["browse_root"])
                discovered = self.__browse_variables(root)
                if not discovered:
                    self.logger.warning(f"No variable nodes found under browse_root '{node_cfg['browse_root']}' for component '{component_id}'")
                for node, key in discovered:
                    self.__register_node(component_id, key, node)
                    nodes.append(node)
            else:
                key = node_cfg.get("key") or node_cfg["node_id"]
                node = self.client.get_node(node_cfg["node_id"])
                self.__register_node(component_id, key, node)
                nodes.append(node)

        # SUBSCRIBE TO THE NODES SO NEW VALUES ARRIVE VIA CALLBACK
        self._handler = _SubHandler(self)
        self._subscription = self.client.create_subscription(opcua_cfg.get("publishing_interval", 500), self._handler)
        self._subscription.subscribe_data_change(nodes)

        print("OpcuaDataAdapter initialized")

    def disconnect(self):
        try:
            if getattr(self, "_subscription", None) is not None:
                self._subscription.delete()
            self.client.disconnect()
            print("Disconnected from OPC UA server")
        except Exception as e:
            print(f"Error while disconnecting from OPC UA server: {e}")
        return super().disconnect()

    def get_data(self):
        for component_id in self.component_ids:
            if len(self.buffer_data[component_id]) > 0:
                data = self.buffer_data[component_id].pop(0)
                self._data[component_id].update(data)

    def __register_node(self, component_id: str, key: str, node) -> None:
        self._node_map[node.nodeid.to_string()] = (component_id, key)
        self._data[component_id][key] = node.read_value()

    def __browse_variables(self, node, path_prefix: str = "", depth: int = 0):
        """
        Recursively browse an OPC UA object node, returning (node, relative_key)
        pairs for every Variable node found underneath it.
        """
        if depth >= _MAX_BROWSE_DEPTH:
            self.logger.warning(f"Max browse depth ({_MAX_BROWSE_DEPTH}) reached while browsing OPC UA node '{node.nodeid.to_string()}'")
            return []

        discovered = []
        for child in node.get_children():
            try:
                name = child.read_browse_name().Name
                node_class = child.read_node_class()
            except Exception as e:
                self.logger.warning(f"Could not read node '{child.nodeid.to_string()}' while browsing: {e}")
                continue

            child_path = f"{path_prefix}/{name}" if path_prefix else name

            if node_class == ua.NodeClass.Variable:
                discovered.append((child, child_path))
            elif node_class == ua.NodeClass.Object:
                discovered.extend(self.__browse_variables(child, child_path, depth + 1))

        return discovered
