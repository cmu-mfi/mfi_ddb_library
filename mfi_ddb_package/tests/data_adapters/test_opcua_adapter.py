import time

import pytest
from asyncua.sync import Server

import mfi_ddb


def _start_test_server():
    server = Server()
    server.set_endpoint("opc.tcp://127.0.0.1:4841/freeopcua/server/")
    idx = server.register_namespace("http://mfi.example/opcua")

    device = server.nodes.objects.add_object(idx, "RobotArm1")
    temperature = device.add_variable(idx, "Temperature", 20.0)
    temperature.set_writable()

    # A nested object tree, for exercising browse_root discovery.
    machine = server.nodes.objects.add_object(idx, "MachineA")
    pressure = machine.add_variable(idx, "Pressure", 1.0)
    pressure.set_writable()
    motor = machine.add_object(idx, "Motor")
    rpm = motor.add_variable(idx, "RPM", 0.0)
    rpm.set_writable()

    server.start()
    return server, {
        "endpoint": "opc.tcp://127.0.0.1:4841/freeopcua/server/",
        "temperature_node_id": temperature.nodeid.to_string(),
        "temperature": temperature,
        "machine_node_id": machine.nodeid.to_string(),
        "pressure": pressure,
        "rpm": rpm,
    }


@pytest.fixture
def opcua_server():
    server, server_info = _start_test_server()
    try:
        yield server_info
    finally:
        server.stop()


def _adapter_config(server_info):
    return {
        "opcua": {
            "endpoint": server_info["endpoint"],
            "publishing_interval": 200,
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "nodes": [
            {
                "component_id": "robot-arm-1",
                "node_id": server_info["temperature_node_id"],
                "key": "temperature",
            }
        ],
    }


def _wait_until(predicate, timeout=5.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_system_polling(opcua_server):
    adapter = mfi_ddb.data_adapters.OpcuaDataAdapter(_adapter_config(opcua_server))
    assert adapter.SELF_UPDATE is True
    assert adapter.data["robot-arm-1"]["temperature"] == 20.0

    opcua_server["temperature"].set_value(42.0)

    # The subscription notification lands in the buffer asynchronously;
    # get_data() only drains what's already buffered, so poll it.
    assert _wait_until(lambda: (adapter.get_data(), adapter.data["robot-arm-1"]["temperature"] == 42.0)[1])

    adapter.disconnect()


def test_system_callback(opcua_server):
    events = []

    class Observer:
        def on_data_update(self, new_value):
            events.append(new_value)

    adapter = mfi_ddb.data_adapters.OpcuaDataAdapter(_adapter_config(opcua_server))
    adapter.add_observer(Observer())

    opcua_server["temperature"].set_value(55.0)

    assert _wait_until(lambda: any(e["robot-arm-1"]["temperature"] == 55.0 for e in events))

    adapter.disconnect()


def test_browse_root(opcua_server):
    config = {
        "opcua": {
            "endpoint": opcua_server["endpoint"],
            "publishing_interval": 200,
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "nodes": [
            {
                "component_id": "machine-a",
                "browse_root": opcua_server["machine_node_id"],
            }
        ],
    }

    adapter = mfi_ddb.data_adapters.OpcuaDataAdapter(config)

    # Both the top-level "Pressure" variable and the nested "Motor/RPM"
    # variable should have been discovered and subscribed to.
    assert adapter.data["machine-a"]["Pressure"] == 1.0
    assert adapter.data["machine-a"]["Motor/RPM"] == 0.0

    opcua_server["pressure"].set_value(3.5)
    opcua_server["rpm"].set_value(1200.0)

    assert _wait_until(lambda: (adapter.get_data(), adapter.data["machine-a"]["Pressure"] == 3.5)[1])
    assert _wait_until(lambda: (adapter.get_data(), adapter.data["machine-a"]["Motor/RPM"] == 1200.0)[1])

    adapter.disconnect()


# Run tests if this file is executed directly
if __name__ == "__main__":
    print("Running tests for OpcuaDataAdapter...")
    print("==================================")

    server, server_info = _start_test_server()
    try:
        print("STARTING TEST 1: test_system_polling")
        test_system_polling(server_info)
        print("\nTEST 1 COMPLETED")
    finally:
        server.stop()
    print("==================================")

    server, server_info = _start_test_server()
    try:
        print("STARTING TEST 2: test_system_callback")
        test_system_callback(server_info)
        print("\nTEST 2 COMPLETED")
    finally:
        server.stop()
    print("==================================")

    server, server_info = _start_test_server()
    try:
        print("STARTING TEST 3: test_browse_root")
        test_browse_root(server_info)
        print("\nTEST 3 COMPLETED")
    finally:
        server.stop()
    print("==================================")
