import copy
import struct
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import mfi_ddb.data_adapters.modbus_tcp as modbus
from mfi_ddb.data_adapters.modbus_tcp import (
    ModbusTcpDataAdapter,
    _Point,
    decode_registers,
    parse_address,
    plan_read_blocks,
)
from mfi_ddb.utils.exceptions import ConfigError

CONFIG_DIR = Path(__file__).resolve().parents[2] / "examples" / "configs" / "data_adapters"
GENERIC = yaml.safe_load((CONFIG_DIR / "modbustcp.yaml").read_text(encoding="utf-8"))

GENERIC_VALUES = {
    "serial_number": "SN-12345",
    "firmware_version": 7,
    "running": 1,
    "fault": 0,
    "speed": 1500.5,
    "temperature": -235,  # raw int16, scale 0.1 -> -23.5
    "power": 12.25,
    "energy_total": 123456789,
}

# PLC-style map: low-word-first floats, low-byte-first text, 6-digit Modicon addresses,
# device-level and component-level attribute registers.
PLC = {
    "adapter_name": "test_plc",
    "trial_id": "trial_test",
    "modbus": {"host": "192.0.2.10", "port": 502, "word_order": "little"},
    "attributes": {"manufacturer": "Test Corp", "model": "PLC-1"},
    "attribute_registers": [
        {"name": "firmware_major", "address": "400201", "data_type": "int16"},
        {"name": "firmware_minor", "address": "400202", "data_type": "int16"},
        {
            "name": "serial_number",
            "address": "436865",
            "data_type": "string",
            "count": 10,
            "byte_order": "little",
        },
    ],
    "components": [
        {
            "component_id": "line-1/status",
            "registers": [
                {"name": "error_flag", "address": "161459"},
                {"name": "error_code", "address": "361441", "data_type": "int16"},
            ],
        },
        {
            "component_id": "line-1/oven",
            "attribute_registers": [
                {"name": "max_temperature", "address": "400004", "data_type": "int16"},
            ],
            "registers": [
                {"name": "heater_on", "address": "000001"},
                {"name": "fan_on", "address": "000003"},
                {
                    "name": "temperature_setpoint",
                    "address": "428673",
                    "data_type": "float32",
                    "units": "degC",
                },
                {
                    "name": "temperature",
                    "address": "428675",
                    "data_type": "float32",
                    "units": "degC",
                },
                {"name": "pressure", "address": "428681", "data_type": "float32", "units": "psi"},
            ],
        },
        {
            "component_id": "line-1/door",
            "registers": [
                {"name": "door_open", "address": "100001"},
                {"name": "open_time", "address": "400190", "data_type": "int16", "units": "ms"},
            ],
        },
    ],
}

PLC_VALUES = {
    "firmware_major": 3,
    "firmware_minor": 41,
    "serial_number": "PLC-TEST-0001",
    "max_temperature": 250,
    "error_flag": 0,
    "error_code": 0,
    "heater_on": 1,
    "fan_on": 0,
    "temperature_setpoint": 180.0,
    "temperature": 179.5,
    "pressure": 14.75,
    "door_open": 1,
    "open_time": 500,
}


# FAKE MODBUS SERVER MEMORY + CLIENT
# =======================================================

_FORMATS = {
    "int16": ">h",
    "uint16": ">H",
    "int32": ">i",
    "uint32": ">I",
    "float32": ">f",
    "int64": ">q",
    "uint64": ">Q",
    "float64": ">d",
}


def encode(value, data_type, byte_order="big", word_order="big", count=1):
    """Encode a value into 16-bit registers (independent of the adapter's decoder)."""
    if data_type == "string":
        raw = value.encode("ascii").ljust(count * 2, b"\x00")
        pairs = [raw[i : i + 2] for i in range(0, len(raw), 2)]
        if byte_order == "little":
            pairs = [p[::-1] for p in pairs]
        return [int.from_bytes(p, "big") for p in pairs]
    raw = struct.pack(_FORMATS[data_type], value)
    words = [raw[i : i + 2] for i in range(0, len(raw), 2)]
    if word_order == "little":
        words.reverse()
    if byte_order == "little":
        words = [w[::-1] for w in words]
    return [int.from_bytes(w, "big") for w in words]


def build_memory(config, values):
    """Build {table: {offset: value}} for every register in an adapter config."""
    memory = {table: {} for table in modbus._READ_FUNCTIONS}
    modbus_cfg = config["modbus"]
    regs = list(config.get("attribute_registers", []))
    for component in config["components"]:
        regs += component.get("attribute_registers", []) + component["registers"]
    for reg in regs:
        table, offset = parse_address(reg["address"], reg.get("register_type"))
        if table in ("coil", "discrete_input"):
            memory[table][offset] = int(values[reg["name"]])
            continue
        words = encode(
            values[reg["name"]],
            reg.get("data_type") or "uint16",
            reg.get("byte_order") or modbus_cfg.get("byte_order", "big"),
            reg.get("word_order") or modbus_cfg.get("word_order", "big"),
            reg.get("count") or 1,
        )
        for i, word in enumerate(words):
            memory[table][offset + i] = word
    return memory


class FakeResponse:
    def __init__(self, values=None, bits=False, error=False):
        self.registers = [] if bits else values
        self.bits = values if bits else []
        self._error = error

    def isError(self):
        return self._error


class FakeModbusTcpClient:
    """Stands in for pymodbus.client.ModbusTcpClient (pymodbus >= 3.10 signature)."""

    memory = {}
    reachable = True
    fail_tables = set()  # tables whose reads return an error response
    raise_tables = set()  # tables whose reads raise an exception
    instances = []

    def __init__(self, host, port=502, framer=None, timeout=3, retries=3):
        self.host, self.port, self.framer = host, port, framer
        self.connected = False
        self.calls = []
        FakeModbusTcpClient.instances.append(self)

    def connect(self):
        self.connected = self.reachable
        return self.connected

    def close(self):
        self.connected = False

    def _read(self, table, address, count, unit_id):
        self.calls.append((table, address, count, unit_id))
        if table in self.raise_tables:
            raise OSError("connection reset")
        if table in self.fail_tables:
            return FakeResponse(error=True)
        values = [self.memory[table].get(address + i, 0) for i in range(count)]
        if table in ("coil", "discrete_input"):
            values = [bool(v) for v in values] + [False] * (-count % 8)  # padded like pymodbus
            return FakeResponse(values, bits=True)
        return FakeResponse(values)

    def read_coils(self, address, *, count=1, device_id=1):
        return self._read("coil", address, count, device_id)

    def read_discrete_inputs(self, address, *, count=1, device_id=1):
        return self._read("discrete_input", address, count, device_id)

    def read_holding_registers(self, address, *, count=1, device_id=1):
        return self._read("holding_register", address, count, device_id)

    def read_input_registers(self, address, *, count=1, device_id=1):
        return self._read("input_register", address, count, device_id)


class FakeRenamedKwargClient(FakeModbusTcpClient):
    """Client whose unit ID keyword has a different name, as in older pymodbus versions."""

    def read_coils(self, address, count=1, unit=1, **kwargs):
        return self._read("coil", address, count, unit)

    def read_discrete_inputs(self, address, count=1, unit=1, **kwargs):
        return self._read("discrete_input", address, count, unit)

    def read_holding_registers(self, address, count=1, unit=1, **kwargs):
        return self._read("holding_register", address, count, unit)

    def read_input_registers(self, address, count=1, unit=1, **kwargs):
        return self._read("input_register", address, count, unit)


@pytest.fixture
def fake_client(monkeypatch):
    def install(config, values, client_class=FakeModbusTcpClient):
        client_class.memory = build_memory(config, values)
        client_class.reachable = True
        client_class.fail_tables = set()
        client_class.raise_tables = set()
        client_class.instances = []
        monkeypatch.setattr(modbus, "ModbusTcpClient", client_class)
        return client_class

    return install


def plc_adapter(fake_client, **modbus_overrides):
    fake_client(PLC, PLC_VALUES)
    config = copy.deepcopy(PLC)
    config["modbus"].update(modbus_overrides)
    return ModbusTcpDataAdapter(config)


# ADDRESSES, DECODING, READ PLANNING
# =======================================================


@pytest.mark.parametrize(
    "address,register_type,expected",
    [
        ("428757", None, ("holding_register", 28756)),
        (428757, None, ("holding_register", 28756)),
        ("016394", None, ("coil", 16393)),
        ("008193", None, ("coil", 8192)),
        ("100001", None, ("discrete_input", 0)),
        ("361441", None, ("input_register", 61440)),
        ("40001", None, ("holding_register", 0)),
        (100, "input_register", ("input_register", 100)),
    ],
)
def test_parse_address(address, register_type, expected):
    assert parse_address(address, register_type) == expected


@pytest.mark.parametrize("address", [16394, 8193, "216394", "4x0001", "4000001", "400000"])
def test_parse_address_rejects_invalid_or_ambiguous(address):
    with pytest.raises(ConfigError):
        parse_address(address)


def test_decode_registers():
    # 50.0 == 0x42480000 in all four byte/word orders
    assert decode_registers([0x4248, 0x0000], "float32", "big", "big") == 50.0
    assert decode_registers([0x0000, 0x4248], "float32", "big", "little") == 50.0
    assert decode_registers([0x4842, 0x0000], "float32", "little", "big") == 50.0
    assert decode_registers([0x0000, 0x4842], "float32", "little", "little") == 50.0
    assert decode_registers([0xFFFF], "int16") == -1
    assert decode_registers([0xFFFF], "uint16") == 65535
    assert decode_registers([0x0001, 0x0000], "uint32", "big", "little") == 1
    assert decode_registers([0x504C, 0x4331, 0x0000], "string", "big") == "PLC1"
    assert decode_registers([0x4C50, 0x3143], "string", "little") == "PLC1"


@pytest.mark.parametrize("data_type", ["int16", "uint16", "int32", "uint32", "int64", "float64"])
@pytest.mark.parametrize("byte_order", ["big", "little"])
@pytest.mark.parametrize("word_order", ["big", "little"])
def test_decode_round_trip(data_type, byte_order, word_order):
    value = 1.5 if data_type.startswith("float") else 1234
    registers = encode(value, data_type, byte_order, word_order)
    assert decode_registers(registers, data_type, byte_order, word_order) == value


def test_plan_read_blocks():
    modbus_cfg = {"byte_order": "big", "word_order": "little"}
    defaults = dict.fromkeys(
        ["register_type", "data_type", "count", "scale", "units", "byte_order", "word_order"]
    )
    points = [
        _Point({**defaults, **reg}, modbus_cfg, 1, component["component_id"])
        for component in PLC["components"]
        for reg in component["registers"]
    ]
    assert len(points) == 9
    # coils 0 and 2 are split by a 1-address gap; floats at 28672-28675 are contiguous
    assert len(plan_read_blocks(points, max_gap=0)) == 8
    assert len(plan_read_blocks(points, max_gap=10)) == 6
    for block in plan_read_blocks(points, max_gap=100000):
        assert block.count <= modbus._MAX_READ_COUNT[block.table]
        for point in block.points:
            assert block.start <= point.offset and point.offset + point.size <= block.end


# CONFIGURATION
# =======================================================


def test_example_configs_are_valid():
    for config in (PLC, GENERIC, ModbusTcpDataAdapter.CONFIG_EXAMPLE):
        ModbusTcpDataAdapter.SCHEMA.model_validate(config)
    assert ModbusTcpDataAdapter.CONFIG_EXAMPLE == GENERIC


INVALID_REGISTERS = [
    {"name": "x", "address": "400001", "data_type": "bool"},
    {"name": "x", "address": "000001", "data_type": "float32"},
    {"name": "x", "address": 0, "register_type": "coil", "data_type": "int16"},
    {"name": "x", "address": 0, "register_type": "holding_register", "data_type": "bool"},
    {"name": "x", "address": "400001", "data_type": "string"},
    {"name": "x", "address": 1234},
    {"name": "x", "address": "465536", "data_type": "float32"},
]


@pytest.mark.parametrize("register", INVALID_REGISTERS)
@pytest.mark.parametrize("location", ["registers", "attribute_registers"])
def test_schema_rejects_invalid_register(register, location):
    # The data adapter app validates configs with SCHEMA.model_validate() before connecting.
    config = copy.deepcopy(GENERIC)
    if location == "registers":
        config["components"][0]["registers"] = [register]
    else:
        config["attribute_registers"] = [register]
    with pytest.raises(ValidationError, match="Register 'x'"):
        ModbusTcpDataAdapter.SCHEMA.model_validate(config)


@pytest.mark.parametrize("register", INVALID_REGISTERS)
def test_invalid_register_rejected(fake_client, register):
    fake_client(GENERIC, GENERIC_VALUES)
    config = copy.deepcopy(GENERIC)
    config["components"][0]["registers"] = [register]
    with pytest.raises(ConfigError):
        ModbusTcpDataAdapter(config)


def test_duplicates_and_missing_keys_rejected(fake_client):
    fake_client(GENERIC, GENERIC_VALUES)
    config = copy.deepcopy(GENERIC)
    config["components"][0]["registers"].append(dict(config["components"][0]["registers"][0]))
    with pytest.raises(ConfigError):
        ModbusTcpDataAdapter(config)

    config = copy.deepcopy(GENERIC)
    config["components"][1]["component_id"] = config["components"][0]["component_id"]
    with pytest.raises(ConfigError):
        ModbusTcpDataAdapter(config)

    for key in ("trial_id", "modbus", "components"):
        config = copy.deepcopy(GENERIC)
        del config[key]
        with pytest.raises(ConfigError):
            ModbusTcpDataAdapter(config)


# ADAPTER BEHAVIOR
# =======================================================


def test_data_and_attributes(fake_client):
    adapter = plc_adapter(fake_client)
    adapter.get_data()

    assert adapter.component_ids == [c["component_id"] for c in PLC["components"]]
    for component in PLC["components"]:
        data = adapter.data[component["component_id"]]
        for reg in component["registers"]:
            assert data[reg["name"]] == PLC_VALUES[reg["name"]], reg["name"]
            assert type(data[reg["name"]]) is type(PLC_VALUES[reg["name"]]), reg["name"]

        attributes = adapter.attributes[component["component_id"]]
        assert attributes["trial_id"] == PLC["trial_id"]
        assert attributes["model"] == "PLC-1"
        assert attributes["serial_number"] == "PLC-TEST-0001"
        assert attributes["firmware_minor"] == 41
        assert attributes["modbus"] == {"host": "192.0.2.10", "port": 502, "unit_id": 1}

    oven = adapter.attributes["line-1/oven"]
    assert oven["max_temperature"] == 250
    assert oven["units"] == {
        "temperature_setpoint": "degC",
        "temperature": "degC",
        "pressure": "psi",
    }
    assert "max_temperature" not in adapter.attributes["line-1/door"]
    assert set(adapter.last_updated) == set(adapter.component_ids)


def test_generic_example(fake_client):
    fake_client(GENERIC, GENERIC_VALUES)
    adapter = ModbusTcpDataAdapter(copy.deepcopy(GENERIC))
    adapter.get_data()

    assert adapter.data["machine-a/spindle"] == {
        "running": 1,
        "fault": 0,
        "speed": 1500.5,
        "temperature": pytest.approx(-23.5),
    }
    assert adapter.data["machine-a/power_meter"] == {"power": 12.25, "energy_total": 123456789}
    assert adapter.attributes["machine-a/spindle"]["serial_number"] == "SN-12345"
    assert adapter.attributes["machine-a/spindle"]["location"] == "cell-1"
    assert "location" not in adapter.attributes["machine-a/power_meter"]

    # power meter reads go to its unit_id override, spindle reads to the default unit
    units = {(table, address): unit_id for table, address, _, unit_id in adapter._client.calls}
    assert units[("input_register", 100)] == 2
    assert units[("input_register", 0)] == 1


def test_unit_id_keyword_detected_from_client(fake_client):
    fake_client(GENERIC, GENERIC_VALUES, FakeRenamedKwargClient)
    adapter = ModbusTcpDataAdapter(copy.deepcopy(GENERIC))
    adapter.get_data()
    assert adapter._unit_id_kwarg == "unit"
    assert adapter.data["machine-a/power_meter"] == {"power": 12.25, "energy_total": 123456789}
    units = {(table, address): unit_id for table, address, _, unit_id in adapter._client.calls}
    assert units[("input_register", 100)] == 2


def test_unit_id_keyword_of_installed_pymodbus():
    from pymodbus.client import ModbusTcpClient

    client = ModbusTcpClient("127.0.0.1")
    assert modbus._get_unit_id_kwarg(client) in client.read_coils.__code__.co_varnames


def test_wrong_word_order_gives_wrong_values(fake_client):
    adapter = plc_adapter(fake_client, word_order="big")
    adapter.get_data()
    assert adapter.data["line-1/oven"]["temperature_setpoint"] != 180.0


def test_failed_reads_skip_only_affected_registers(fake_client):
    adapter = plc_adapter(fake_client)
    FakeModbusTcpClient.fail_tables = {"holding_register"}
    FakeModbusTcpClient.raise_tables = {"input_register"}
    adapter.get_data()

    assert adapter.data["line-1/status"] == {"error_flag": 0}  # error code is an input register
    assert adapter.data["line-1/door"] == {"door_open": 1}  # open time is a holding register
    assert adapter.data["line-1/oven"] == {"heater_on": 1, "fan_on": 0}


def test_unreachable_server(fake_client):
    fake_client(PLC, PLC_VALUES)
    FakeModbusTcpClient.reachable = False
    with pytest.raises(ConnectionError):
        ModbusTcpDataAdapter(copy.deepcopy(PLC))


def test_connection_lost_and_restored(fake_client):
    adapter = plc_adapter(fake_client)
    adapter._client.connected = False
    FakeModbusTcpClient.reachable = False
    adapter.get_data()  # must not raise
    assert all(not adapter.data[c] for c in adapter.component_ids)

    FakeModbusTcpClient.reachable = True
    adapter.get_data()
    assert adapter.data["line-1/door"] == {"door_open": 1, "open_time": 500}


def test_single_client_read_only(fake_client):
    adapter = plc_adapter(fake_client)
    for _ in range(3):
        adapter.get_data()
        adapter.clear_data_buffer()
    assert len(FakeModbusTcpClient.instances) == 1
    # 4 attribute registers merged into 3 reads at startup + 8 reads per poll
    assert len(adapter._client.calls) == 3 + 3 * 8

    adapter.disconnect()
    assert not FakeModbusTcpClient.instances[0].connected
