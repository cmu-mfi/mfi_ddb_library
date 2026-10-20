import inspect
import struct
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field, model_validator
from pymodbus.client import ModbusTcpClient

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError

# Modicon address prefix -> Modbus table
_MODICON_TABLES = {
    "0": "coil",
    "1": "discrete_input",
    "3": "input_register",
    "4": "holding_register",
}

_BIT_TABLES = ("coil", "discrete_input")

_READ_FUNCTIONS = {
    "coil": "read_coils",
    "discrete_input": "read_discrete_inputs",
    "input_register": "read_input_registers",
    "holding_register": "read_holding_registers",
}

# Maximum quantity per read request allowed by the Modbus specification
_MAX_READ_COUNT = {
    "coil": 2000,
    "discrete_input": 2000,
    "input_register": 125,
    "holding_register": 125,
}

# data_type -> (number of 16-bit registers, struct format)
_REGISTER_TYPES = {
    "int16": (1, "h"),
    "uint16": (1, "H"),
    "int32": (2, "i"),
    "uint32": (2, "I"),
    "float32": (2, "f"),
    "int64": (4, "q"),
    "uint64": (4, "Q"),
    "float64": (4, "d"),
}

_TableType = Literal["coil", "discrete_input", "input_register", "holding_register"]
_DataType = Literal[
    "bool", "int16", "uint16", "int32", "uint32", "float32", "int64", "uint64", "float64", "string"
]
_Order = Literal["big", "little"]


class _SCHEMA:
    class _MODBUS(BaseModel):
        host: str = Field(..., description="IP address or hostname of the Modbus TCP server")
        port: int = Field(502, description="TCP port of the Modbus server (default: 502)")
        framer: Literal["socket", "rtu"] = Field(
            "socket",
            description="Framing on the TCP connection: 'socket' (standard Modbus TCP) "
            "or 'rtu' (Modbus RTU over TCP). (default: socket)",
        )
        unit_id: int = Field(1, description="Default unit ID of the device (default: 1)")
        timeout: float = Field(3.0, description="Request timeout in seconds (default: 3)")
        retries: int = Field(3, description="Retries per request (default: 3)")
        byte_order: _Order = Field(
            "big", description="Byte order within each 16-bit register (default: big)"
        )
        word_order: _Order = Field(
            "big",
            description="Order of 16-bit words in 32/64-bit values. 'little' = low word first. "
            "(default: big)",
        )
        max_read_gap: int = Field(
            0,
            description="Max unused addresses allowed between two registers to merge them into "
            "one read request. 0 merges only contiguous addresses. (default: 0)",
        )

    class _REGISTER(BaseModel):
        name: str = Field(..., description="Key used for the value in the streamed data")
        address: Union[int, str] = Field(
            ...,
            description="Modicon address (e.g. '400001', '016394'; quote addresses starting "
            "with 0) or, if register_type is set, the 0-based protocol offset",
        )
        register_type: Optional[_TableType] = Field(
            None,
            description="(optional) One of 'coil', 'discrete_input', 'input_register', "
            "'holding_register'. If omitted, address is parsed as a Modicon address.",
        )
        data_type: Optional[_DataType] = Field(
            None,
            description="(optional) bool, int16, uint16, int32, uint32, float32, int64, "
            "uint64, float64, string. (default: bool for coils/discrete inputs, "
            "uint16 for registers)",
        )
        count: Optional[int] = Field(
            None, description="(optional) Number of registers for data_type 'string'"
        )
        scale: Optional[float] = Field(
            None, description="(optional) Multiplier applied to numeric values"
        )
        units: Optional[str] = Field(None, description="(optional) Engineering units")
        byte_order: Optional[_Order] = Field(
            None, description="(optional) Override modbus.byte_order for this register"
        )
        word_order: Optional[_Order] = Field(
            None, description="(optional) Override modbus.word_order for this register"
        )

        @model_validator(mode="after")
        def _check_register(self):
            # Validate address/data_type/count at schema time so configs are rejected
            # before the adapter connects (e.g. by the data adapter app).
            try:
                resolve_register(self.model_dump())
            except ConfigError as e:
                raise ValueError(e.message) from None
            return self

    class _COMPONENT(BaseModel):
        component_id: str = Field(..., description="Identifier for the component")
        unit_id: Optional[int] = Field(
            None, description="(optional) Override modbus.unit_id for this component"
        )
        trial_id: Optional[str] = Field(None, description="(optional) Trial ID for the component")
        attributes: Dict[str, Any] = Field(
            {}, description="(optional) Static metadata for the component"
        )
        attribute_registers: List["_REGISTER"] = Field(
            [],
            description="(optional) Registers read once at startup and added to the "
            "component attributes",
        )
        registers: List["_REGISTER"] = Field(
            ..., description="Registers polled on every get_data() call"
        )

    class SCHEMA(BaseModel):
        trial_id: str = Field(
            ..., description="Trial ID for the system. No spaces or special characters allowed."
        )
        modbus: "_MODBUS" = Field(..., description="Configuration for the Modbus connection")
        attributes: Dict[str, Any] = Field(
            {}, description="(optional) Static metadata added to every component"
        )
        attribute_registers: List["_REGISTER"] = Field(
            [],
            description="(optional) Registers read once at startup and added to the attributes "
            "of every component",
        )
        components: List["_COMPONENT"] = Field(
            ..., description="List of components, each with the registers to poll"
        )


class _Point:
    """A register resolved from the config: where it lives and how to decode it."""

    def __init__(self, reg: dict, modbus_cfg: dict, unit_id: int, component_id: str) -> None:
        self.name = reg["name"]
        self.component_id = component_id
        self.unit_id = unit_id
        self.table, self.offset, self.data_type, self.size = resolve_register(reg)
        self.scale = reg.get("scale")
        self.units = reg.get("units")
        self.byte_order = reg.get("byte_order") or modbus_cfg["byte_order"]
        self.word_order = reg.get("word_order") or modbus_cfg["word_order"]

    def decode(self, values: list) -> Union[int, float, str]:
        if self.data_type == "bool":
            return int(bool(values[0]))
        value = decode_registers(values, self.data_type, self.byte_order, self.word_order)
        if self.scale is not None and self.data_type != "string":
            value = value * self.scale
        return value


class _ReadBlock:
    """A single read request covering one or more points in the same table."""

    def __init__(self, point: _Point) -> None:
        self.unit_id = point.unit_id
        self.table = point.table
        self.start = point.offset
        self.end = point.offset + point.size
        self.points = [point]

    @property
    def count(self) -> int:
        return self.end - self.start


def parse_address(address: Union[int, str], register_type: Optional[str] = None) -> Tuple[str, int]:
    """
    Resolve a configured address into (table, 0-based offset).

    * With register_type: address is the 0-based protocol offset.
    * Without register_type: address is a 5 or 6 digit Modicon address, where the first digit
      selects the table (0=coil, 1=discrete input, 3=input register, 4=holding register) and
      the remaining digits are the 1-based register number. e.g. '428757' -> holding reg 28756.
    """
    if register_type is not None:
        try:
            offset = int(address)
        except (TypeError, ValueError):
            raise ConfigError(f"Address '{address}' must be an integer offset.") from None
        if not 0 <= offset <= 65535:
            raise ConfigError(f"Address offset {offset} out of range 0-65535.")
        return register_type, offset

    if isinstance(address, int):
        # A 6-digit integer cannot have lost a leading zero, so it is unambiguous.
        if not 100000 <= address <= 499999:
            raise ConfigError(
                f"Address {address} is ambiguous. Write Modicon addresses as quoted strings "
                "(e.g. '016394') so leading zeros are kept, or set 'register_type'."
            )
        address = str(address)

    address = str(address).strip()
    if not address.isdigit() or len(address) not in (5, 6):
        raise ConfigError(f"Address '{address}' is not a 5 or 6 digit Modicon address.")
    table = _MODICON_TABLES.get(address[0])
    if table is None:
        raise ConfigError(
            f"Address '{address}' has an unknown table prefix '{address[0]}' (expected 0,1,3,4)."
        )
    offset = int(address[1:]) - 1
    if not 0 <= offset <= 65535:
        raise ConfigError(f"Address '{address}' is out of range.")
    return table, offset


def resolve_register(reg: dict) -> Tuple[str, int, str, int]:
    """
    Validate a register config and return (table, 0-based offset, data_type, size), where size
    is the number of coils/discrete inputs or 16-bit registers it occupies.
    """
    name = reg["name"]
    try:
        table, offset = parse_address(reg["address"], reg.get("register_type"))
    except ConfigError as e:
        raise ConfigError(f"Register '{name}': {e.message}") from None

    is_bit_table = table in _BIT_TABLES
    data_type = reg.get("data_type") or ("bool" if is_bit_table else "uint16")
    if is_bit_table != (data_type == "bool"):
        raise ConfigError(
            f"Register '{name}': data_type '{data_type}' is not valid for a {table}. "
            "Use 'bool' for coils/discrete inputs only."
        )

    if data_type == "string":
        if not reg.get("count") or reg["count"] < 1:
            raise ConfigError(f"Register '{name}': data_type 'string' requires 'count'.")
        size = reg["count"]
    elif data_type == "bool":
        size = 1
    else:
        size = _REGISTER_TYPES[data_type][0]

    if offset + size > 65536:
        raise ConfigError(f"Register '{name}': address range exceeds 65535.")
    return table, offset, data_type, size


def decode_registers(
    registers: list, data_type: str, byte_order: str = "big", word_order: str = "big"
) -> Union[int, float, str]:
    """Decode a list of 16-bit register values into a Python value."""
    word_fmt = ">H" if byte_order == "big" else "<H"

    if data_type == "string":
        raw = b"".join(struct.pack(word_fmt, r) for r in registers)
        return raw.split(b"\x00", 1)[0].decode("ascii", errors="replace").strip()

    words = list(registers) if word_order == "big" else list(reversed(registers))
    raw = b"".join(struct.pack(word_fmt, w) for w in words)
    return struct.unpack(">" + _REGISTER_TYPES[data_type][1], raw)[0]


def plan_read_blocks(points: List[_Point], max_gap: int = 0) -> List[_ReadBlock]:
    """Group points into as few read requests as possible."""
    blocks = []
    points = sorted(points, key=lambda p: (p.unit_id, p.table, p.offset))
    for point in points:
        block = blocks[-1] if blocks else None
        if (
            block is not None
            and block.unit_id == point.unit_id
            and block.table == point.table
            and point.offset <= block.end + max_gap
            and max(block.end, point.offset + point.size) - block.start
            <= _MAX_READ_COUNT[point.table]
        ):
            block.end = max(block.end, point.offset + point.size)
            block.points.append(point)
        else:
            blocks.append(_ReadBlock(point))
    return blocks


def _get_unit_id_kwarg(client) -> str:
    """
    Name of the keyword argument that selects the unit ID in the client's read methods.
    pymodbus has renamed it across versions (it is `device_id` since 3.10), so it is read from
    the method signature instead of being hard-coded.
    """
    for name, param in inspect.signature(client.read_coils).parameters.items():
        if name in ("address", "count", "no_response_expected"):
            continue
        if param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
            return name
    return "device_id"


def _get_framer(name: str):
    try:
        from pymodbus import FramerType  # pymodbus >= 3.7
    except ImportError:
        from pymodbus import Framer as FramerType  # pymodbus 3.6
    return FramerType.RTU if name == "rtu" else FramerType.SOCKET


class ModbusDataAdapter(BaseDataAdapter):
    NAME = "Modbus"

    CONFIG_HELP = {
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "modbus": {
            "host": "IP address or hostname of the Modbus TCP server",
            "port": "(optional) TCP port of the Modbus server (default: 502)",
            "framer": "(optional) 'socket' (standard Modbus TCP) or 'rtu' (Modbus RTU over TCP)"
            " (default: socket)",
            "unit_id": "(optional) Default unit ID of the device (default: 1)",
            "timeout": "(optional) Request timeout in seconds (default: 3)",
            "retries": "(optional) Retries per request (default: 3)",
            "byte_order": "(optional) 'big' or 'little' byte order within a register "
            "(default: big)",
            "word_order": "(optional) 'big' or 'little' word order for 32/64-bit values. "
            "'little' = low word first (default: big)",
            "max_read_gap": "(optional) Max unused addresses between registers to merge them "
            "into one read request (default: 0)",
        },
        "attributes": "(optional) Static metadata added to every component",
        "attribute_registers": "(optional) Registers read once at startup and added to the "
        "attributes of every component. Same keys as 'registers'.",
        "components": [
            {
                "component_id": "Identifier for the component",
                "unit_id": "(optional) Override modbus.unit_id for this component",
                "trial_id": "(optional) Trial ID for the component",
                "attributes": "(optional) Static metadata for the component",
                "attribute_registers": "(optional) Registers read once at startup and added to "
                "the component attributes. Same keys as 'registers'.",
                "registers": [
                    {
                        "name": "Key used for the value in the streamed data",
                        "address": "Modicon address as a string (e.g. '400001', '016394') or, "
                        "if register_type is set, the 0-based protocol offset",
                        "register_type": "(optional) coil, discrete_input, input_register, "
                        "holding_register",
                        "data_type": "(optional) bool, int16, uint16, int32, uint32, float32, "
                        "int64, uint64, float64, string (default: bool for bits, uint16 for "
                        "registers)",
                        "count": "(optional) Number of registers for data_type 'string'",
                        "scale": "(optional) Multiplier applied to numeric values",
                        "units": "(optional) Engineering units, added to attributes",
                        "byte_order": "(optional) Override modbus.byte_order",
                        "word_order": "(optional) Override modbus.word_order",
                    }
                ],
            }
        ],
    }

    CONFIG_EXAMPLE = {
        "adapter_name": "my_modbus_adapter",
        "trial_id": "trial_001",
        "modbus": {
            "host": "192.168.1.50",
            "port": 502,
            "framer": "socket",
            "unit_id": 1,
            "timeout": 3,
            "retries": 3,
            "byte_order": "big",
            "word_order": "big",
            "max_read_gap": 0,
        },
        "attributes": {"manufacturer": "Example Corp", "model": "Example Machine"},
        "attribute_registers": [
            {"name": "serial_number", "address": "400101", "data_type": "string", "count": 8},
            {"name": "firmware_version", "address": "400110", "data_type": "uint16"},
        ],
        "components": [
            {
                "component_id": "machine-a/spindle",
                "attributes": {"location": "cell-1"},
                "registers": [
                    {"name": "running", "address": "000001"},
                    {"name": "fault", "address": "100001"},
                    {"name": "speed", "address": "300001", "data_type": "float32", "units": "rpm"},
                    {
                        "name": "temperature",
                        "address": "400001",
                        "data_type": "int16",
                        "scale": 0.1,
                        "units": "degC",
                    },
                ],
            },
            {
                "component_id": "machine-a/power_meter",
                "unit_id": 2,
                "registers": [
                    {
                        "name": "power",
                        "register_type": "input_register",
                        "address": 100,
                        "data_type": "float32",
                        "word_order": "little",
                        "units": "kW",
                    },
                    {
                        "name": "energy_total",
                        "register_type": "holding_register",
                        "address": 200,
                        "data_type": "uint32",
                        "units": "Wh",
                    },
                ],
            },
        ],
    }

    RECOMMENDED_TOPIC_FAMILY = "historian"

    SELF_UPDATE = False

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict):
        try:
            config = ModbusDataAdapter.SCHEMA(**config).model_dump()
        except Exception as e:
            raise ConfigError(f"Invalid configuration: {e}") from None

        super().__init__(config)

        modbus_cfg = self.cfg["modbus"]
        default_unit = modbus_cfg["unit_id"]

        # RESOLVE REGISTERS FROM THE CONFIG
        data_points = []
        attribute_points = []
        for component in self.cfg["components"]:
            component_id = component["component_id"]
            if component_id in self.component_ids:
                raise ConfigError(f"Duplicate component_id '{component_id}'.")
            unit_id = component["unit_id"] if component["unit_id"] is not None else default_unit

            for regs, points in (
                (component["registers"], data_points),
                (
                    self.cfg["attribute_registers"] + component["attribute_registers"],
                    attribute_points,
                ),
            ):
                names = [reg["name"] for reg in regs]
                duplicates = {name for name in names if names.count(name) > 1}
                if duplicates:
                    raise ConfigError(
                        f"Duplicate register names {sorted(duplicates)} in component "
                        f"'{component_id}'."
                    )
                points.extend(_Point(reg, modbus_cfg, unit_id, component_id) for reg in regs)

            self.component_ids.append(component_id)
            self._data[component_id] = {}
            self.attributes[component_id] = {
                "trial_id": component["trial_id"] or self.cfg["trial_id"],
                **self.cfg["attributes"],
                **component["attributes"],
                "modbus": {
                    "host": modbus_cfg["host"],
                    "port": modbus_cfg["port"],
                    "unit_id": unit_id,
                },
            }
            units = {reg["name"]: reg["units"] for reg in component["registers"] if reg["units"]}
            if units:
                self.attributes[component_id]["units"] = units

        max_gap = modbus_cfg["max_read_gap"]
        self._data_blocks = plan_read_blocks(data_points, max_gap)
        attribute_blocks = plan_read_blocks(attribute_points, max_gap)

        # CONNECT TO THE MODBUS SERVER
        self._client = ModbusTcpClient(
            modbus_cfg["host"],
            port=modbus_cfg["port"],
            framer=_get_framer(modbus_cfg["framer"]),
            timeout=modbus_cfg["timeout"],
            retries=modbus_cfg["retries"],
        )
        self._unit_id_kwarg = _get_unit_id_kwarg(self._client)
        self.__connect()

        # READ ATTRIBUTE REGISTERS ONCE
        for component_id, values in self.__read_blocks(attribute_blocks).items():
            self.attributes[component_id].update(values)

        print(
            f"ModbusDataAdapter initialized: {len(self.component_ids)} components, "
            f"{len(data_points)} registers in {len(self._data_blocks)} read requests"
        )

    def disconnect(self):
        self._client.close()
        print("Disconnected from Modbus server")
        return super().disconnect()

    def get_data(self):
        if not self._client.connected and not self._client.connect():
            self.logger.warning(
                f"Modbus server {self.cfg['modbus']['host']}:{self.cfg['modbus']['port']} "
                "is not reachable. Skipping read."
            )
            return

        current_time = time.time()
        for component_id, values in self.__read_blocks(self._data_blocks).items():
            self._data[component_id].update(values)
            self.last_updated[component_id] = current_time

    def __connect(self):
        host, port = self.cfg["modbus"]["host"], self.cfg["modbus"]["port"]
        print(f"Connecting to Modbus server {host}:{port} ...")
        if not self._client.connect():
            raise ConnectionError(f"Could not connect to Modbus server {host}:{port}.")
        print(f"Connected to Modbus server {host}:{port}")

    def __read_blocks(self, blocks: List[_ReadBlock]) -> Dict[str, dict]:
        """Read each block and return decoded values grouped by component_id."""
        results = {}
        for block in blocks:
            read = getattr(self._client, _READ_FUNCTIONS[block.table])
            try:
                response = read(
                    block.start, count=block.count, **{self._unit_id_kwarg: block.unit_id}
                )
            except Exception as e:
                self.logger.warning(
                    f"Read of {block.count} {block.table}(s) at {block.start} failed: {e}"
                )
                continue
            if response.isError():
                self.logger.warning(
                    f"Read of {block.count} {block.table}(s) at {block.start} returned "
                    f"an error: {response}"
                )
                continue

            values = response.bits if block.table in _BIT_TABLES else response.registers
            for point in block.points:
                start = point.offset - block.start
                try:
                    value = point.decode(values[start : start + point.size])
                except Exception as e:
                    self.logger.warning(f"Could not decode register '{point.name}': {e}")
                    continue
                results.setdefault(point.component_id, {})[point.name] = value
        return results
