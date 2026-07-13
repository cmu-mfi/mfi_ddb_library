import threading
import time

import pytest
from pydantic import ValidationError
from pymodbus.client import ModbusTcpClient
from pymodbus.client.mixin import ModbusClientMixin
from pymodbus.datastore import ModbusDeviceContext, ModbusSequentialDataBlock, ModbusServerContext
from pymodbus.server import ServerStop, StartTcpServer

import mfi_ddb

_HOST = "127.0.0.1"
_PORT = 15020


def _start_test_server():
    # ModbusSequentialDataBlock's starting address is 1-based internally.
    hr = ModbusSequentialDataBlock(1, [0] * 100)
    co = ModbusSequentialDataBlock(1, [0] * 100)
    device = ModbusDeviceContext(hr=hr, co=co, ir=hr, di=co)
    context = ModbusServerContext(devices=device, single=True)

    thread = threading.Thread(
        target=StartTcpServer,
        kwargs={"context": context, "address": (_HOST, _PORT)},
        daemon=True,
    )
    thread.start()
    time.sleep(0.5)  # give the server a moment to bind before clients connect

    seed_client = ModbusTcpClient(_HOST, port=_PORT, timeout=5.0)
    seed_client.connect()
    seed_client.write_register(0, 42, device_id=1)  # holding_register uint16
    float_regs = seed_client.convert_to_registers(3.5, ModbusClientMixin.DATATYPE.FLOAT32, word_order="big")
    seed_client.write_registers(10, float_regs, device_id=1)  # holding_register float32
    seed_client.write_coil(0, True, device_id=1)
    seed_client.close()

    return thread, seed_client


@pytest.fixture
def modbus_server():
    thread, seed_client = _start_test_server()
    try:
        yield {"host": _HOST, "port": _PORT}
    finally:
        ServerStop()


def _adapter_config(server_info):
    return {
        "modbus": {
            "host": server_info["host"],
            "port": server_info["port"],
        },
        "trial_id": "trial_001",
        "tags": [
            {
                "component_id": "machine-a",
                "register_type": "holding_register",
                "address": 0,
                "key": "count",
                "data_type": "uint16",
            },
            {
                "component_id": "machine-a",
                "register_type": "holding_register",
                "address": 10,
                "key": "temperature",
                "data_type": "float32",
            },
            {
                "component_id": "machine-a",
                "register_type": "coil",
                "address": 0,
                "key": "running",
            },
        ],
    }


def test_system_polling(modbus_server):
    adapter = mfi_ddb.data_adapters.ModbusDataAdapter(_adapter_config(modbus_server))
    assert adapter.SELF_UPDATE is False

    assert adapter.data["machine-a"]["count"] == 42
    assert adapter.data["machine-a"]["temperature"] == 3.5
    assert adapter.data["machine-a"]["running"] is True

    write_client = ModbusTcpClient(modbus_server["host"], port=modbus_server["port"], timeout=5.0)
    write_client.connect()
    write_client.write_register(0, 99, device_id=1)
    write_client.write_coil(0, False, device_id=1)
    write_client.close()

    adapter.get_data()
    assert adapter.data["machine-a"]["count"] == 99
    assert adapter.data["machine-a"]["running"] is False

    adapter.disconnect()


def test_bad_address_does_not_crash_polling(modbus_server):
    config = _adapter_config(modbus_server)
    config["tags"].append(
        {
            "component_id": "machine-a",
            "register_type": "holding_register",
            "address": 9000,  # outside the seeded datastore range
            "key": "bogus",
        }
    )

    adapter = mfi_ddb.data_adapters.ModbusDataAdapter(config)
    # the valid tags still populated; the bad one is simply absent
    assert adapter.data["machine-a"]["count"] == 42
    assert "bogus" not in adapter.data["machine-a"]

    adapter.get_data()  # should not raise
    adapter.disconnect()


def test_schema_rejects_inconsistent_data_type():
    with pytest.raises(ValidationError):
        mfi_ddb.data_adapters.ModbusDataAdapter.SCHEMA(**{
            "modbus": {"host": "127.0.0.1"},
            "trial_id": "t",
            "tags": [{"component_id": "c1", "register_type": "coil", "address": 0, "data_type": "float32"}],
        })

    with pytest.raises(ValidationError):
        mfi_ddb.data_adapters.ModbusDataAdapter.SCHEMA(**{
            "modbus": {"host": "127.0.0.1"},
            "trial_id": "t",
            "tags": [{"component_id": "c1", "register_type": "holding_register", "address": 0, "data_type": "bool"}],
        })


# Run tests if this file is executed directly
if __name__ == "__main__":
    print("Running tests for ModbusDataAdapter...")
    print("==================================")

    thread, seed_client = _start_test_server()
    try:
        print("STARTING TEST 1: test_system_polling")
        test_system_polling({"host": _HOST, "port": _PORT})
        print("\nTEST 1 COMPLETED")
    finally:
        ServerStop()
    print("==================================")

    thread, seed_client = _start_test_server()
    try:
        print("STARTING TEST 2: test_bad_address_does_not_crash_polling")
        test_bad_address_does_not_crash_polling({"host": _HOST, "port": _PORT})
        print("\nTEST 2 COMPLETED")
    finally:
        ServerStop()
    print("==================================")

    print("STARTING TEST 3: test_schema_rejects_inconsistent_data_type")
    test_schema_rejects_inconsistent_data_type()
    print("\nTEST 3 COMPLETED")
    print("==================================")
