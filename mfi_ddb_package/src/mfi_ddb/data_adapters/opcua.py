"""
OPC UA data adapter.

Subscribes to Variable nodes on an OPC UA server and streams their values. Nodes can be
listed explicitly, discovered under a given object (``browse_root``), or - when no nodes are
configured - discovered automatically by browsing the server's address space starting at the
Objects folder.

Reference: https://reference.opcfoundation.org/Core/Part4/v105/docs/5.8 (Browse service)
           https://github.com/FreeOpcUa/opcua-asyncio (client library)
"""

import contextlib
import dataclasses
import datetime
import enum
import re
import threading
import time
import uuid
from collections import deque
from typing import Any, Dict, List, Optional, Tuple, Union

from asyncua import ua
from asyncua.sync import Client
from pydantic import BaseModel, Field, field_validator, model_validator

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError

_OBJECTS_FOLDER = "i=85"
_READ_CHUNK = 500  # nodes per Read request; keeps us below typical MaxNodesPerRead limits
_SUBSCRIBE_CHUNK = 500  # monitored items per CreateMonitoredItems request
_SAFE_NAME = re.compile(r"[^\w.\-]")


# ==================================================================================================
# HELPERS
# ==================================================================================================


def sanitize_name(name: str) -> str:
    """Make an OPC UA browse name safe to use as a component id / data key segment."""
    return _SAFE_NAME.sub("_", str(name).strip()) or "_"


def flatten_value(key: str, value: Any, max_array_length: int = 16) -> Dict[str, Any]:
    """
    Convert an OPC UA value into a flat {key: int | float | str | bool} dict.

    Arrays become ``key_0, key_1, ...``, structures become ``key/field``. Arrays longer than
    ``max_array_length`` and values that cannot be decoded are dropped (empty dict).
    """
    if value is None:
        return {}
    if isinstance(value, (bool, int, float, str)):
        if isinstance(value, enum.Enum):  # IntEnum and friends
            return {key: int(value)}
        return {key: value}
    if isinstance(value, enum.Enum):
        return {key: value.name}
    if isinstance(value, (datetime.datetime, datetime.date)):
        return {key: value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {key: bytes(value).hex()}
    if isinstance(value, uuid.UUID):
        return {key: str(value)}
    if isinstance(value, ua.LocalizedText):
        return {key: value.Text} if value.Text is not None else {}
    if isinstance(value, ua.QualifiedName):
        return {key: value.Name}
    if isinstance(value, ua.NodeId):
        return {key: value.to_string()}
    if isinstance(value, ua.StatusCode):
        return {key: value.name}
    if isinstance(value, ua.ExtensionObject):
        # Undecoded custom structure (type definition not loaded / unavailable).
        return {}
    if isinstance(value, (list, tuple)):
        if len(value) > max_array_length:
            return {}
        flat = {}
        for i, item in enumerate(value):
            flat.update(flatten_value(f"{key}_{i}", item, max_array_length))
        return flat
    if dataclasses.is_dataclass(value):
        flat = {}
        for field in dataclasses.fields(value):
            flat.update(
                flatten_value(f"{key}/{field.name}", getattr(value, field.name), max_array_length)
            )
        return flat
    return {key: str(value)}


@dataclasses.dataclass
class DiscoveredNode:
    """A Variable node found while browsing."""

    path: Tuple[str, ...]  # sanitized browse names relative to the browse root
    node_id: str
    is_property: bool  # reached through a HasProperty reference (static metadata)


def browse_variables(
    client: Client,
    root: Any,
    max_depth: int = 10,
    max_nodes: int = 1000,
    namespaces: Optional[List[int]] = None,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    logger: Any = None,
) -> List[DiscoveredNode]:
    """
    Breadth-first browse of the address space below ``root``, returning every Variable node.

    * Objects are descended into up to ``max_depth`` levels; Variables are not (their children
      are typically properties such as EURange that are not worth one round trip per variable).
    * ``namespaces``: only nodes in these namespace indexes are followed (None = no filter).
    * ``exclude``: regexes matched against the relative path ("Machine/Motor/RPM"); a matching
      Object prunes its whole subtree.
    * ``include``: if given, a Variable is kept only if its relative path matches one regex.
    * Stops after ``max_nodes`` Variables have been found.
    """
    include_re = [re.compile(p) for p in (include or [])]
    exclude_re = [re.compile(p) for p in (exclude or [])]
    warn = logger.warning if logger else print

    root_node = client.get_node(root) if not hasattr(root, "nodeid") else root
    visited = {root_node.nodeid.to_string()}
    queue = deque([(root_node, (), 0)])
    found: List[DiscoveredNode] = []
    depth_warned = False

    while queue:
        node, path, depth = queue.popleft()
        try:
            refs = node.get_references(
                refs=ua.ObjectIds.HierarchicalReferences,
                direction=ua.BrowseDirection.Forward,
                nodeclassmask=ua.NodeClass.Object | ua.NodeClass.Variable,
            )
        except Exception as e:
            warn(f"Could not browse OPC UA node '{node.nodeid.to_string()}': {e}")
            continue

        for ref in refs:
            node_id = ua.NodeId(
                ref.NodeId.Identifier, ref.NodeId.NamespaceIndex, ref.NodeId.NodeIdType
            )
            node_id_str = node_id.to_string()
            if node_id_str in visited:
                continue
            if namespaces is not None and node_id.NamespaceIndex not in namespaces:
                continue
            visited.add(node_id_str)

            child_path = path + (sanitize_name(ref.BrowseName.Name),)
            rel_path = "/".join(child_path)
            if any(p.search(rel_path) for p in exclude_re):
                continue

            if ref.NodeClass == ua.NodeClass.Variable:
                if include_re and not any(p.search(rel_path) for p in include_re):
                    continue
                is_property = ref.ReferenceTypeId == ua.NodeId(ua.ObjectIds.HasProperty)
                found.append(DiscoveredNode(child_path, node_id_str, is_property))
                if len(found) >= max_nodes:
                    warn(
                        f"OPC UA discovery stopped after max_nodes={max_nodes} variables. "
                        "Narrow it with discovery.root / include / exclude or raise max_nodes."
                    )
                    return found
            elif ref.NodeClass == ua.NodeClass.Object:
                if depth + 1 < max_depth:
                    queue.append((client.get_node(node_id), child_path, depth + 1))
                elif not depth_warned:
                    warn(
                        f"OPC UA discovery reached max_depth={max_depth} at '{rel_path}'; "
                        "deeper objects are skipped."
                    )
                    depth_warned = True

    return found


# ==================================================================================================
# CONFIG SCHEMA
# ==================================================================================================


class _SCHEMA:
    class _OPCUA(BaseModel):
        endpoint: str = Field(
            ..., description="OPC UA server endpoint, e.g. opc.tcp://192.168.1.10:4840"
        )
        username: Optional[str] = Field(None, description="Username for authentication")
        password: Optional[str] = Field(None, description="Password for authentication")
        security_string: Optional[str] = Field(
            None,
            description="asyncua security string 'Policy,Mode,client_cert,client_key[,server_cert]'"
            ", e.g. 'Basic256Sha256,SignAndEncrypt,cert.der,key.pem'. Default: no security.",
        )
        timeout: float = Field(5.0, gt=0, description="Request timeout in seconds")
        publishing_interval: float = Field(
            500, gt=0, description="Subscription publishing interval in ms"
        )
        sampling_interval: Optional[float] = Field(
            None, ge=0, description="Server-side sampling interval in ms (default: publishing)"
        )
        health_check_interval: float = Field(
            5.0, ge=0, description="Seconds between connection checks / reconnects (0 = off)"
        )
        load_custom_types: bool = Field(
            True, description="Load server-specific structure definitions to decode them"
        )

    class _DISCOVERY(BaseModel):
        root: str = Field(_OBJECTS_FOLDER, description="Node to start browsing from")
        component_depth: int = Field(
            1,
            ge=0,
            description="Number of object levels below root that form the component_id",
        )
        max_depth: int = Field(10, ge=1, description="Maximum object levels to browse")
        max_nodes: int = Field(1000, ge=1, description="Maximum variables to subscribe to")
        namespaces: Optional[List[Union[int, str]]] = Field(
            None,
            description="Namespace indexes or URIs to browse (default: all except 0, the "
            "OPC UA standard namespace)",
        )
        include: List[str] = Field([], description="Regexes; keep only matching variable paths")
        exclude: List[str] = Field([], description="Regexes; drop matching paths/subtrees")
        properties_as_attributes: bool = Field(
            True, description="Store HasProperty variables as attributes, not streamed data"
        )

        @field_validator("include", "exclude")
        @classmethod
        def _check_regex(cls, patterns):
            for p in patterns:
                try:
                    re.compile(p)
                except re.error as e:
                    raise ValueError(f"Invalid regex '{p}': {e}") from None
            return patterns

    class _NODE(BaseModel):
        component_id: str = Field(..., description="Component the node(s) belong to")
        node_id: Optional[str] = Field(None, description="Single variable, e.g. 'ns=2;i=2'")
        browse_root: Optional[str] = Field(
            None, description="Object whose variables are all subscribed (recursively)"
        )
        key: Optional[str] = Field(None, description="Data key for 'node_id' (default: node_id)")
        trial_id: Optional[str] = Field(None, description="Trial ID for the component")

        @model_validator(mode="after")
        def _check_exactly_one_source(self):
            if bool(self.node_id) == bool(self.browse_root):
                raise ValueError("Exactly one of 'node_id' or 'browse_root' must be set.")
            return self

    class SCHEMA(BaseModel):
        opcua: "_SCHEMA._OPCUA" = Field(..., description="OPC UA server connection")
        nodes: List["_SCHEMA._NODE"] = Field(
            [], description="Nodes to subscribe to. Leave empty to auto-discover."
        )
        discovery: "_SCHEMA._DISCOVERY" = Field(
            default_factory=lambda: _SCHEMA._DISCOVERY(),
            description="Auto-discovery settings (also limit 'browse_root' entries)",
        )
        attributes: Dict[str, Any] = Field({}, description="Static metadata for every component")
        max_array_length: int = Field(
            16, ge=0, description="Arrays longer than this are not streamed"
        )


_SCHEMA.SCHEMA.model_rebuild()


# ==================================================================================================
# DATA ADAPTER
# ==================================================================================================


class _SubHandler:
    """Receives data-change notifications from the asyncua subscription thread."""

    def __init__(self, adapter: "OpcuaDataAdapter") -> None:
        self.adapter = adapter

    def datachange_notification(self, node, val, data):
        self.adapter._on_datachange(node.nodeid.to_string(), val)

    def status_change_notification(self, status):
        self.adapter.logger.warning(f"OPC UA subscription status changed: {status}")


class OpcuaDataAdapter(BaseDataAdapter):
    NAME = "OPCUA"

    CONFIG_HELP = {
        "adapter_name": "(optional) Name of the adapter instance",
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "opcua": {
            "endpoint": "OPC UA server endpoint URL, e.g. opc.tcp://192.168.1.10:4840",
            "username": "(optional) Username for authentication",
            "password": "(optional) Password for authentication",
            "security_string": "(optional) 'Policy,Mode,client_cert,client_key[,server_cert]' "
            "e.g. 'Basic256Sha256,SignAndEncrypt,cert.der,key.pem' (default: no security)",
            "timeout": "(optional) Request timeout in seconds (default: 5)",
            "publishing_interval": "(optional) Subscription publishing interval in ms "
            "(default: 500)",
            "sampling_interval": "(optional) Server sampling interval in ms "
            "(default: publishing_interval)",
            "health_check_interval": "(optional) Seconds between connection checks; the adapter "
            "reconnects and resubscribes if the server is lost. 0 disables (default: 5)",
            "load_custom_types": "(optional) Load server-specific structure definitions so they "
            "can be decoded (default: true)",
        },
        "nodes": "(optional) Explicit nodes. Each entry needs 'component_id' and exactly one of "
        "'node_id' (one variable, optional 'key') or 'browse_root' (all variables under an "
        "object). Optional 'trial_id'. Leave empty to auto-discover the whole server.",
        "discovery": {
            "root": "(optional) Node to auto-discover from (default: i=85, the Objects folder)",
            "component_depth": "(optional) Object levels below root that form the component_id, "
            "e.g. 1 -> 'MachineA', 2 -> 'MachineA/Spindle' (default: 1)",
            "max_depth": "(optional) Max object levels to browse (default: 10)",
            "max_nodes": "(optional) Max variables to subscribe to (default: 1000)",
            "namespaces": "(optional) Namespace indexes or URIs to auto-discover "
            "(default: all except 0)",
            "include": "(optional) Regexes; only variables whose path matches are kept",
            "exclude": "(optional) Regexes; matching paths (and object subtrees) are skipped",
            "properties_as_attributes": "(optional) Put HasProperty variables (serial number, "
            "model, ...) in attributes instead of streamed data (default: true)",
        },
        "attributes": "(optional) Static metadata added to every component",
        "max_array_length": "(optional) Arrays longer than this are skipped (default: 16)",
    }

    CONFIG_EXAMPLE = {
        "adapter_name": "my_opcua_adapter",
        "trial_id": "trial_001",
        "opcua": {
            "endpoint": "opc.tcp://192.168.1.10:4840",
            "publishing_interval": 500,
        },
        "discovery": {
            "component_depth": 1,
            "max_nodes": 1000,
            "exclude": ["^Aliases", "^DeviceSet"],
        },
        "attributes": {"location": "cell-1"},
        "nodes": [],
    }

    RECOMMENDED_TOPIC_FAMILY = "historian"

    SELF_UPDATE = True  # Subscription notifications are pushed to observers.

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    # ----------------------------------------------------------------------------------------------
    # LIFECYCLE
    # ----------------------------------------------------------------------------------------------

    def __init__(self, config: dict):
        try:
            config = OpcuaDataAdapter.SCHEMA(**config).model_dump()
        except Exception as e:
            raise ConfigError(f"Invalid configuration: {e}") from None

        super().__init__(config)

        self._opcua_cfg = self.cfg["opcua"]
        self._max_array = self.cfg["max_array_length"]
        self._node_map: Dict[str, List[Tuple[str, str]]] = {}  # node id -> [(component, key)]
        self._pending: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._client: Optional[Client] = None
        self._subscription = None
        self._dropped_keys = set()

        self._client = self.__connect()
        try:
            self.__resolve_nodes()
            self.__read_initial_values()
            self.__subscribe()
        except Exception:
            self.__close_client()
            raise

        self._worker = threading.Thread(target=self.__worker_loop, daemon=True)
        self._worker.start()

        print(
            f"OpcuaDataAdapter initialized: {len(self.component_ids)} components, "
            f"{len(self._node_map)} nodes subscribed"
        )

    def disconnect(self):
        self._stop.set()
        worker = getattr(self, "_worker", None)
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=5)
        self.__close_client()
        print("Disconnected from OPC UA server")
        return super().disconnect()

    # ----------------------------------------------------------------------------------------------
    # DATA
    # ----------------------------------------------------------------------------------------------

    def get_data(self):
        self.__drain_pending()

    def _on_datachange(self, node_id: str, value: Any) -> None:
        targets = self._node_map.get(node_id)
        if not targets:
            return
        now = time.time()
        with self._lock:
            for component_id, key in targets:
                flat = self.__flatten(component_id, key, value)
                if flat:
                    self._pending.setdefault(component_id, {}).update(flat)
                    self.last_updated[component_id] = now

    def __drain_pending(self) -> dict:
        with self._lock:
            pending, self._pending = self._pending, {}
        for component_id, values in pending.items():
            self._data.setdefault(component_id, {}).update(values)
        return pending

    def __flatten(self, component_id: str, key: str, value: Any) -> dict:
        flat = flatten_value(key, value, self._max_array)
        if not flat and value is not None and (component_id, key) not in self._dropped_keys:
            self._dropped_keys.add((component_id, key))
            self.logger.warning(
                f"Value of '{component_id}/{key}' ({type(value).__name__}) cannot be streamed "
                f"(array longer than max_array_length={self._max_array} or undecodable struct)."
            )
        return flat

    # ----------------------------------------------------------------------------------------------
    # BACKGROUND WORKER: batches notifications for observers, checks connection health
    # ----------------------------------------------------------------------------------------------

    def __worker_loop(self):
        flush_period = self._opcua_cfg["publishing_interval"] / 1000.0
        health_period = self._opcua_cfg["health_check_interval"]
        next_health = time.time() + health_period

        while not self._stop.wait(flush_period):
            if self._observers:
                pending = self.__drain_pending()
                if pending:
                    self._notify_observers(pending)

            if health_period and time.time() >= next_health:
                next_health = time.time() + health_period
                self.__check_health()

    def __check_health(self):
        if self._client is not None:
            try:
                self._client.get_node(ua.ObjectIds.Server_ServerStatus_State).read_value()
                return
            except Exception as e:
                if self._stop.is_set():
                    return
                self.logger.warning(f"OPC UA connection lost ({e}). Reconnecting ...")
            self.__close_client()

        if self._stop.is_set():
            return
        try:
            self._client = self.__connect()
            self.__subscribe()
            self.logger.warning("Reconnected to OPC UA server and resubscribed.")
        except Exception as e:
            self.__close_client()  # retry the full connect + subscribe on the next check
            self.logger.warning(f"OPC UA reconnect failed: {e}. Will retry.")

    # ----------------------------------------------------------------------------------------------
    # CONNECTION
    # ----------------------------------------------------------------------------------------------

    def __connect(self) -> Client:
        cfg = self._opcua_cfg
        client = Client(url=cfg["endpoint"], timeout=cfg["timeout"])
        try:
            if cfg["username"]:
                client.set_user(cfg["username"])
            if cfg["password"]:
                client.set_password(cfg["password"])
            if cfg["security_string"]:
                client.set_security_string(cfg["security_string"])
            print(f"Connecting to OPC UA server {cfg['endpoint']} ...")
            client.connect()
        except Exception as e:
            with contextlib.suppress(Exception):
                client.disconnect()
            raise ConnectionError(
                f"Could not connect to OPC UA server {cfg['endpoint']}: {e}"
            ) from None

        if cfg["load_custom_types"]:
            try:
                client.load_data_type_definitions()
            except Exception as e:
                self.logger.warning(f"Could not load custom data types: {e}")
        print(f"Connected to OPC UA server {cfg['endpoint']}")
        return client

    def __close_client(self):
        if self._subscription is not None:
            with contextlib.suppress(Exception):
                self._subscription.delete()
            self._subscription = None
        if self._client is not None:
            client, self._client = self._client, None
            with contextlib.suppress(Exception):
                client.disconnect()

    def __subscribe(self):
        cfg = self._opcua_cfg
        sampling = cfg["sampling_interval"]
        if sampling is None:
            sampling = cfg["publishing_interval"]

        self._subscription = self._client.create_subscription(
            cfg["publishing_interval"], _SubHandler(self)
        )
        nodes = [self._client.get_node(node_id) for node_id in self._node_map]
        failed = 0
        for i in range(0, len(nodes), _SUBSCRIBE_CHUNK):
            results = self._subscription.subscribe_data_change(
                nodes[i : i + _SUBSCRIBE_CHUNK], sampling_interval=sampling
            )
            failed += sum(1 for r in results if isinstance(r, ua.StatusCode))
        if failed:
            self.logger.warning(f"{failed} of {len(nodes)} OPC UA nodes could not be monitored.")

    # ----------------------------------------------------------------------------------------------
    # NODE RESOLUTION
    # ----------------------------------------------------------------------------------------------

    def __resolve_nodes(self):
        """Build component ids, data keys and node map from `nodes` or by auto-discovery."""
        disc = self.cfg["discovery"]
        self._properties: Dict[str, Dict[str, str]] = {}  # component -> {key: node id}
        trial_ids: Dict[str, str] = {}

        def add(component_id, key, node_id, is_property=False, trial_id=None):
            if component_id not in trial_ids:
                trial_ids[component_id] = trial_id or self.cfg["trial_id"]
                self._properties[component_id] = {}
                self.attributes[component_id] = {"node_ids": {}}
            bucket = (
                self._properties[component_id]
                if is_property
                else self.attributes[component_id]["node_ids"]
            )
            unique, n = key, 1
            while unique in bucket:
                n += 1
                unique = f"{key}_{n}"
            bucket[unique] = node_id
            if not is_property:
                self._node_map.setdefault(node_id, []).append((component_id, unique))

        if self.cfg["nodes"]:
            for entry in self.cfg["nodes"]:
                if entry["node_id"]:
                    node_id = self._client.get_node(entry["node_id"]).nodeid.to_string()
                    raw_key = entry["key"] or entry["node_id"]
                    key = "/".join(sanitize_name(part) for part in raw_key.split("/"))
                    add(entry["component_id"], key, node_id, trial_id=entry["trial_id"])
                    continue
                found = self.__browse(entry["browse_root"], namespaces=None)
                if not found:
                    self.logger.warning(
                        f"No variables found under browse_root '{entry['browse_root']}'."
                    )
                for item in found:
                    is_prop = item.is_property and disc["properties_as_attributes"]
                    key = "/".join(item.path)
                    add(entry["component_id"], key, item.node_id, is_prop, entry["trial_id"])
        else:
            root = self._client.get_node(disc["root"])
            root_name = sanitize_name(root.read_browse_name().Name)
            found = self.__browse(disc["root"], namespaces=self.__resolve_namespaces())
            depth = disc["component_depth"]
            for item in found:
                n = min(depth, len(item.path) - 1)
                component_id = "/".join(item.path[:n]) or root_name
                key = "/".join(item.path[n:])
                is_prop = item.is_property and disc["properties_as_attributes"]
                add(component_id, key, item.node_id, is_prop)

        if not self._node_map:
            raise ConfigError(
                "No OPC UA variables found to subscribe to. Check 'nodes' / 'discovery' settings "
                "(e.g. discovery.root, namespaces, include/exclude)."
            )

        for component_id, trial_id in trial_ids.items():
            self.component_ids.append(component_id)
            self._data[component_id] = {}
            self.attributes[component_id] = {
                "trial_id": trial_id,
                **self.cfg["attributes"],
                "opcua": {"endpoint": self._opcua_cfg["endpoint"]},
                **self.attributes[component_id],
            }

    def __browse(self, root: str, namespaces: Optional[List[int]]) -> List[DiscoveredNode]:
        disc = self.cfg["discovery"]
        return browse_variables(
            self._client,
            root,
            max_depth=disc["max_depth"],
            max_nodes=disc["max_nodes"],
            namespaces=namespaces,
            include=disc["include"],
            exclude=disc["exclude"],
            logger=self.logger,
        )

    def __resolve_namespaces(self) -> List[int]:
        ns_array = self._client.get_namespace_array()
        wanted = self.cfg["discovery"]["namespaces"]
        if wanted is None:
            return list(range(1, len(ns_array)))
        indexes = []
        for ns in wanted:
            if isinstance(ns, int) or str(ns).isdigit():
                indexes.append(int(ns))
            elif ns in ns_array:
                indexes.append(ns_array.index(ns))
            else:
                self.logger.warning(f"Namespace '{ns}' not found on server. Available: {ns_array}")
        return indexes

    def __read_initial_values(self):
        """Read every node once: data -> birth values, properties -> attributes."""
        # Data nodes
        node_ids = list(self._node_map)
        for node_id, value in zip(node_ids, self.__read_values(node_ids)):
            for component_id, key in self._node_map[node_id]:
                self._data[component_id].update(self.__flatten(component_id, key, value))

        # Property nodes -> attributes
        for component_id, props in self._properties.items():
            if not props:
                continue
            keys, ids = list(props), list(props.values())
            properties = {}
            for key, value in zip(keys, self.__read_values(ids)):
                properties.update(flatten_value(key, value, self._max_array))
            if properties:
                self.attributes[component_id]["properties"] = properties

        # The streamer waits until every component has birth data; drop components that have
        # none so a server with unreadable nodes cannot block startup forever.
        empty = [c for c in self.component_ids if not self._data[c]]
        for component_id in empty:
            self.logger.warning(
                f"Component '{component_id}' has no readable values; it will not be streamed."
            )
            self.component_ids.remove(component_id)
            del self._data[component_id]
            del self.attributes[component_id]
        if empty:
            for node_id in list(self._node_map):
                targets = [t for t in self._node_map[node_id] if t[0] not in empty]
                if targets:
                    self._node_map[node_id] = targets
                else:
                    del self._node_map[node_id]
        if not self.component_ids:
            raise ConfigError("None of the configured/discovered OPC UA variables are readable.")

        now = time.time()
        for component_id in self.component_ids:
            self.last_updated[component_id] = now

    def __read_values(self, node_ids: List[str]) -> List[Any]:
        values: List[Any] = []
        for i in range(0, len(node_ids), _READ_CHUNK):
            chunk = [self._client.get_node(n) for n in node_ids[i : i + _READ_CHUNK]]
            try:
                values.extend(self._client.read_values(chunk))
            except Exception:
                # Fall back to one-by-one reads so a single bad node does not drop the chunk.
                for node in chunk:
                    try:
                        values.append(node.read_value())
                    except Exception as e:
                        self.logger.warning(f"Could not read '{node.nodeid.to_string()}': {e}")
                        values.append(None)
        return values

    # ----------------------------------------------------------------------------------------------
    # DISCOVERY UTILITY
    # ----------------------------------------------------------------------------------------------

    @staticmethod
    def discover(config: dict) -> List[dict]:
        """
        Connect with ``config`` (same format as the adapter config; only ``opcua`` and
        ``discovery`` are used) and return the variables auto-discovery would subscribe to,
        as a ready-to-use ``nodes`` list: [{"component_id", "node_id", "key"}, ...].
        """
        cfg = dict(config)
        cfg.setdefault("trial_id", "discovery")
        try:
            cfg = OpcuaDataAdapter.SCHEMA(**cfg).model_dump()
        except Exception as e:
            raise ConfigError(f"Invalid configuration: {e}") from None
        cfg["nodes"] = []

        adapter = OpcuaDataAdapter.__new__(OpcuaDataAdapter)
        BaseDataAdapter.__init__(adapter, cfg)
        adapter._opcua_cfg = cfg["opcua"]
        adapter._node_map = {}
        adapter._client = adapter._OpcuaDataAdapter__connect()
        try:
            adapter._OpcuaDataAdapter__resolve_nodes()
        finally:
            adapter._client.disconnect()

        nodes = []
        for component_id in adapter.component_ids:
            for key, node_id in adapter.attributes[component_id]["node_ids"].items():
                nodes.append({"component_id": component_id, "node_id": node_id, "key": key})
        return nodes
