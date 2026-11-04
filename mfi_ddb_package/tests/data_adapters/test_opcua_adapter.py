"""
Tests for the OPC UA data adapter, run against a real in-process asyncua server.
"""

import datetime
import socket
import time
from pathlib import Path

import pytest
import yaml
from asyncua import ua
from asyncua.sync import Server

from mfi_ddb.data_adapters.opcua import OpcuaDataAdapter, flatten_value, sanitize_name
from mfi_ddb.utils.exceptions import ConfigError

CONFIG_DIR = Path(__file__).resolve().parents[2] / "examples" / "configs" / "data_adapters"
NS_URI = "http://mfi.example/opcua-test"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_until(predicate, timeout=5.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _build_server(port):
    """
    Objects/
      MachineA/                (ns=2)
        Pressure     float
        Running      bool
        Array        [1, 2, 3]
        BigArray     100 elements          -> not streamed
        Timestamp    datetime
        SerialNumber (property)            -> attribute
        Motor/
          RPM        float
          Mode       LocalizedText
      Robot Arm#1/             (name needs sanitizing)
        Temperature  float
      Diagnostics/
        Counter      int
      Empty/                   (no variables)
      Loose          int  (variable directly in Objects)
      Server/                  (ns=0, standard; must be skipped)
    """
    server = Server()
    endpoint = f"opc.tcp://127.0.0.1:{port}/test/"
    server.set_endpoint(endpoint)
    idx = server.register_namespace(NS_URI)
    objects = server.nodes.objects

    machine = objects.add_object(idx, "MachineA")
    n = {
        "pressure": machine.add_variable(idx, "Pressure", 1.5),
        "running": machine.add_variable(idx, "Running", True),
        "array": machine.add_variable(idx, "Array", [1, 2, 3]),
        "big": machine.add_variable(idx, "BigArray", list(range(100))),
        "timestamp": machine.add_variable(
            idx, "Timestamp", datetime.datetime(2026, 1, 1, 12, 0, 0)
        ),
    }
    machine.add_property(idx, "SerialNumber", "SN-001")
    motor = machine.add_object(idx, "Motor")
    n["rpm"] = motor.add_variable(idx, "RPM", 1200.0)
    n["mode"] = motor.add_variable(idx, "Mode", ua.LocalizedText("Auto"))

    robot = objects.add_object(idx, "Robot Arm#1")
    n["temperature"] = robot.add_variable(idx, "Temperature", 20.0)

    diagnostics = objects.add_object(idx, "Diagnostics")
    n["counter"] = diagnostics.add_variable(idx, "Counter", 0)

    n["empty"] = objects.add_object(idx, "Empty")
    n["loose"] = objects.add_variable(idx, "Loose", 7)

    for node in n.values():
        if node.read_node_class() == ua.NodeClass.Variable:
            node.set_writable()

    n["machine"] = machine
    n["robot"] = robot
    return server, endpoint, n


@pytest.fixture(scope="module")
def opcua_server():
    port = _free_port()
    server, endpoint, nodes = _build_server(port)
    server.start()
    try:
        yield {"endpoint": endpoint, "port": port, "nodes": nodes, "server": server}
    finally:
        server.stop()


def _config(endpoint, **overrides):
    cfg = {
        "adapter_name": "test_opcua",
        "trial_id": "trial_test",
        "opcua": {"endpoint": endpoint, "publishing_interval": 100, "timeout": 3},
    }
    cfg.update(overrides)
    return cfg


@pytest.fixture
def make_adapter():
    adapters = []

    def _make(config):
        adapter = OpcuaDataAdapter(config)
        adapters.append(adapter)
        return adapter

    yield _make
    for adapter in adapters:
        adapter.disconnect()


# ==================================================================================================
# PURE HELPERS
# ==================================================================================================


def test_flatten_value():
    assert flatten_value("k", None) == {}
    assert flatten_value("k", 3) == {"k": 3}
    assert flatten_value("k", True) == {"k": True}
    assert flatten_value("k", "x") == {"k": "x"}
    assert flatten_value("k", [1, 2]) == {"k_0": 1, "k_1": 2}
    assert flatten_value("k", list(range(17)), max_array_length=16) == {}
    assert flatten_value("k", datetime.datetime(2026, 1, 1)) == {"k": "2026-01-01T00:00:00"}
    assert flatten_value("k", b"\x01\xff") == {"k": "01ff"}
    assert flatten_value("k", ua.LocalizedText("Auto")) == {"k": "Auto"}
    assert flatten_value("k", ua.QualifiedName("Name", 2)) == {"k": "Name"}
    assert flatten_value("k", ua.NodeId(5, 2)) == {"k": "ns=2;i=5"}
    assert flatten_value("k", ua.NodeClass.Variable) == {"k": 2}
    # structures (asyncua dataclasses) are flattened by field
    flat = flatten_value("k", ua.Range(Low=0.0, High=10.0))
    assert flat == {"k/Low": 0.0, "k/High": 10.0}


def test_sanitize_name():
    assert sanitize_name("Robot Arm#1") == "Robot_Arm_1"
    assert sanitize_name("temp.sensor-1") == "temp.sensor-1"
    assert sanitize_name("a/b+c") == "a_b_c"


# ==================================================================================================
# CONFIG VALIDATION
# ==================================================================================================


def test_example_configs_are_valid():
    for name in ("opcua.yaml", "opcua_explicit_nodes.yaml"):
        cfg = yaml.safe_load((CONFIG_DIR / name).read_text(encoding="utf-8"))
        OpcuaDataAdapter.SCHEMA(**cfg)
    OpcuaDataAdapter.SCHEMA(**OpcuaDataAdapter.CONFIG_EXAMPLE)


@pytest.mark.parametrize(
    "overrides",
    [
        {"opcua": {}},  # endpoint missing
        {"nodes": [{"component_id": "c"}]},  # neither node_id nor browse_root
        {"nodes": [{"component_id": "c", "node_id": "i=1", "browse_root": "i=85"}]},
        {"discovery": {"exclude": ["("]}},  # invalid regex
        {"discovery": {"max_nodes": 0}},
    ],
)
def test_invalid_config_rejected(overrides):
    cfg = _config("opc.tcp://127.0.0.1:1")
    cfg.update(overrides)
    with pytest.raises(ConfigError):
        OpcuaDataAdapter(cfg)


def test_unreachable_server():
    cfg = _config(f"opc.tcp://127.0.0.1:{_free_port()}")
    cfg["opcua"]["timeout"] = 1
    with pytest.raises(ConnectionError):
        OpcuaDataAdapter(cfg)


# ==================================================================================================
# AUTO-DISCOVERY
# ==================================================================================================


def test_auto_discovery(opcua_server, make_adapter):
    adapter = make_adapter(_config(opcua_server["endpoint"]))

    assert adapter.SELF_UPDATE is True
    # Server object (ns=0) skipped, Empty object has no variables, Loose -> root component
    assert sorted(adapter.component_ids) == ["Diagnostics", "MachineA", "Objects", "Robot_Arm_1"]

    machine = adapter.data["MachineA"]
    assert machine["Pressure"] == 1.5
    assert machine["Running"] is True
    assert (machine["Array_0"], machine["Array_1"], machine["Array_2"]) == (1, 2, 3)
    assert machine["Timestamp"].startswith("2026-01-01T12:00:00")
    assert machine["Motor/RPM"] == 1200.0
    assert machine["Motor/Mode"] == "Auto"
    assert not any(k.startswith("BigArray") for k in machine)  # longer than max_array_length
    assert "SerialNumber" not in machine  # property -> attribute

    assert adapter.data["Robot_Arm_1"] == {"Temperature": 20.0}
    assert adapter.data["Objects"] == {"Loose": 7}

    attrs = adapter.attributes["MachineA"]
    assert attrs["trial_id"] == "trial_test"
    assert attrs["properties"] == {"SerialNumber": "SN-001"}
    assert attrs["opcua"]["endpoint"] == opcua_server["endpoint"]
    assert attrs["node_ids"]["Motor/RPM"] == opcua_server["nodes"]["rpm"].nodeid.to_string()

    for component_id in adapter.component_ids:
        assert component_id in adapter.last_updated


def test_component_depth_and_filters(opcua_server, make_adapter):
    adapter = make_adapter(
        _config(
            opcua_server["endpoint"],
            discovery={
                "component_depth": 2,
                "exclude": ["^Diagnostics", "^Loose$"],
                "properties_as_attributes": False,
            },
            attributes={"location": "lab"},
        )
    )
    assert sorted(adapter.component_ids) == ["MachineA", "MachineA/Motor", "Robot_Arm_1"]
    assert adapter.data["MachineA/Motor"] == {"RPM": 1200.0, "Mode": "Auto"}
    assert adapter.data["MachineA"]["SerialNumber"] == "SN-001"  # streamed as data now
    assert adapter.attributes["MachineA/Motor"]["location"] == "lab"


def test_include_and_max_nodes(opcua_server, make_adapter):
    adapter = make_adapter(
        _config(opcua_server["endpoint"], discovery={"include": [r"/(RPM|Pressure)$"]})
    )
    assert adapter.component_ids == ["MachineA"]
    assert adapter.data["MachineA"] == {"Pressure": 1.5, "Motor/RPM": 1200.0}

    adapter = make_adapter(_config(opcua_server["endpoint"], discovery={"max_nodes": 2}))
    assert sum(len(adapter.attributes[c]["node_ids"]) for c in adapter.component_ids) <= 2


def test_namespace_and_root(opcua_server, make_adapter):
    machine_id = opcua_server["nodes"]["machine"].nodeid.to_string()
    adapter = make_adapter(
        _config(
            opcua_server["endpoint"],
            discovery={"root": machine_id, "namespaces": [NS_URI], "component_depth": 0},
        )
    )
    assert adapter.component_ids == ["MachineA"]
    assert adapter.data["MachineA"]["Motor/RPM"] == 1200.0

    # Namespace that does not exist -> nothing to subscribe to
    with pytest.raises(ConfigError):
        OpcuaDataAdapter(
            _config(opcua_server["endpoint"], discovery={"namespaces": ["urn:does-not-exist"]})
        )


def test_no_variables_raises(opcua_server):
    empty_id = opcua_server["nodes"]["empty"].nodeid.to_string()
    with pytest.raises(ConfigError):
        OpcuaDataAdapter(_config(opcua_server["endpoint"], discovery={"root": empty_id}))


def test_discover_helper(opcua_server, make_adapter):
    nodes = OpcuaDataAdapter.discover({"opcua": {"endpoint": opcua_server["endpoint"]}})
    keys = {(n["component_id"], n["key"]) for n in nodes}
    assert ("MachineA", "Motor/RPM") in keys
    assert ("Robot_Arm_1", "Temperature") in keys

    # The output round-trips as an explicit `nodes` config with the same ids and keys
    explicit = make_adapter(_config(opcua_server["endpoint"], nodes=nodes))
    auto = make_adapter(_config(opcua_server["endpoint"]))
    assert explicit.data == auto.data


# ==================================================================================================
# EXPLICIT NODES
# ==================================================================================================


def test_explicit_nodes(opcua_server, make_adapter):
    nodes = opcua_server["nodes"]
    adapter = make_adapter(
        _config(
            opcua_server["endpoint"],
            nodes=[
                {
                    "component_id": "robot",
                    "node_id": nodes["temperature"].nodeid.to_string(),
                    "key": "temperature",
                },
                {
                    "component_id": "machine",
                    "browse_root": nodes["machine"].nodeid.to_string(),
                    "trial_id": "trial_other",
                },
            ],
        )
    )
    assert adapter.component_ids == ["robot", "machine"]
    assert adapter.data["robot"] == {"temperature": 20.0}
    assert adapter.data["machine"]["Motor/RPM"] == 1200.0
    assert adapter.attributes["robot"]["trial_id"] == "trial_test"
    assert adapter.attributes["machine"]["trial_id"] == "trial_other"
    assert adapter.attributes["machine"]["properties"] == {"SerialNumber": "SN-001"}


# ==================================================================================================
# STREAMING
# ==================================================================================================


def test_polling_receives_updates(opcua_server, make_adapter):
    nodes = opcua_server["nodes"]
    adapter = make_adapter(
        _config(
            opcua_server["endpoint"],
            nodes=[{"component_id": "machine", "browse_root": nodes["machine"].nodeid.to_string()}],
        )
    )
    adapter.clear_data_buffer()
    try:
        nodes["rpm"].write_value(1500.0)
        nodes["array"].write_value([4, 5, 6])

        def updated():
            adapter.get_data()
            data = adapter.data["machine"]
            return data.get("Motor/RPM") == 1500.0 and data.get("Array_2") == 6

        assert _wait_until(updated)
    finally:
        nodes["rpm"].write_value(1200.0)
        nodes["array"].write_value([1, 2, 3])


def test_observers_get_batched_updates(opcua_server, make_adapter):
    nodes = opcua_server["nodes"]
    adapter = make_adapter(
        _config(
            opcua_server["endpoint"],
            nodes=[{"component_id": "robot", "browse_root": nodes["robot"].nodeid.to_string()}],
        )
    )

    received = []

    class Observer:
        def on_data_update(self, data):
            received.append(data)

    adapter.add_observer(Observer())
    try:
        nodes["temperature"].write_value(42.0)
        assert _wait_until(
            lambda: any(d.get("robot", {}).get("Temperature") == 42.0 for d in received)
        )
    finally:
        nodes["temperature"].write_value(20.0)


def test_reconnects_after_server_restart(make_adapter):
    port = _free_port()
    server, endpoint, nodes = _build_server(port)
    server.start()
    cfg = _config(endpoint, discovery={"include": ["Temperature$"]})
    cfg["opcua"]["health_check_interval"] = 0.5
    cfg["opcua"]["timeout"] = 1
    adapter = make_adapter(cfg)
    assert adapter.data["Robot_Arm_1"] == {"Temperature": 20.0}

    server.stop()
    time.sleep(1.5)  # let the health check notice the outage

    server, endpoint, nodes = _build_server(port)  # same address space, fresh server
    server.start()
    try:
        adapter.clear_data_buffer()

        def updated():
            try:
                nodes["temperature"].write_value(55.0)
            except Exception:
                return False
            adapter.get_data()
            return adapter.data["Robot_Arm_1"].get("Temperature") == 55.0

        assert _wait_until(updated, timeout=15, interval=0.5)
    finally:
        adapter.disconnect()
        server.stop()
