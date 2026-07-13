import time
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, model_validator
from pymodbus.client import ModbusTcpClient
from pymodbus.client.mixin import ModbusClientMixin
from pymodbus.exceptions import ModbusException

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError

_BIT_REGISTER_TYPES = {"coil", "discrete_input"}

_DATATYPE_REGISTER_COUNT = {
    "uint16": 1,
    "int16": 1,
    "uint32": 2,
    "int32": 2,
    "float32": 2,
}

_DATATYPE_MAP = {
    "uint16": ModbusClientMixin.DATATYPE.UINT16,
    "int16": ModbusClientMixin.DATATYPE.INT16,
    "uint32": ModbusClientMixin.DATATYPE.UINT32,
    "int32": ModbusClientMixin.DATATYPE.INT32,
    "float32": ModbusClientMixin.DATATYPE.FLOAT32,
}


class _SCHEMA:
    class _TAG(BaseModel):
        component_id: str = Field(..., description="Identifier for the component this tag belongs to")
        register_type: Literal["coil", "discrete_input", "input_register", "holding_register"] = Field(..., description="Modbus register type to read")
        address: int = Field(..., description="Starting register/coil address (0-based)")
        key: Optional[str] = Field(None, description="Data key name to store the value under (defaults to '<register_type>_<address>')")
        data_type: Literal["bool", "uint16", "int16", "uint32", "int32", "float32"] = Field("uint16", description="How to decode the raw value. Must be 'bool' for 'coil'/'discrete_input' and must not be 'bool' for 'input_register'/'holding_register' (default: 'uint16' for register types).")
        device_id: Optional[int] = Field(1, description="Modbus device/slave id (default: 1)")
        trial_id: Optional[str] = Field(None, description="Trial ID for the component (optional)")

        @model_validator(mode="after")
        def _check_data_type(self):
            is_bit_type = self.register_type in ("coil", "discrete_input")
            if is_bit_type and self.data_type != "bool":
                raise ValueError("'data_type' must be 'bool' for 'coil'/'discrete_input' register types.")
            if not is_bit_type and self.data_type == "bool":
                raise ValueError("'data_type' must not be 'bool' for 'input_register'/'holding_register' register types; choose a numeric type.")
            return self

    class _MODBUS(BaseModel):
        host: str = Field(..., description="Modbus TCP server host/IP address")
        port: Optional[int] = Field(502, description="Modbus TCP server port (default: 502)")
        timeout: Optional[float] = Field(5.0, description="Connection timeout in seconds (default: 5.0)")
        word_order: Optional[Literal["big", "little"]] = Field("big", description="Register (word) order for multi-register values like 'uint32'/'float32' (default: big)")

    class SCHEMA(BaseModel):
        modbus: "_MODBUS" = Field(..., description="Configuration for the Modbus TCP server connection")
        trial_id: str = Field(..., description="Trial ID for the system. No spaces or special characters allowed.")
        tags: List["_TAG"] = Field(..., description="List of Modbus tags to poll.")


class ModbusDataAdapter(BaseDataAdapter):
    """
    Modbus has no native push/subscribe mechanism (unlike MQTT or OPC UA
    subscriptions) - every value is retrieved by the master explicitly polling
    a register address. get_data() issues the reads; there is no callback path.
    """

    NAME = "Modbus"

    CONFIG_HELP = {
        "modbus": {
            "host": "Modbus TCP server host/IP address",
            "port": "(optional) Modbus TCP server port (default: 502)",
            "timeout": "(optional) Connection timeout in seconds (default: 5.0)",
            "word_order": "(optional) Register (word) order for multi-register values like 'uint32'/'float32' (default: big)",
        },
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "tags": "List of Modbus tags to poll. Each tag needs 'component_id', 'register_type' ('coil'/'discrete_input'/'input_register'/'holding_register') and 'address'. Optionally 'key', 'data_type', 'device_id' and 'trial_id' can be provided.",
    }

    CONFIG_EXAMPLE = {
        "modbus": {
            "host": "192.168.1.10",
            "port": 502,
        },
        "trial_id": "trial_001",
        "tags": [
            {
                "component_id": "machine-a",
                "register_type": "holding_register",
                "address": 0,
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

    RECOMMENDED_TOPIC_FAMILY = "historian"

    SELF_UPDATE = False  # Modbus has no push/subscribe mechanism; must be polled.

    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict):
        super().__init__(config)

        self.cfg = config
        modbus_cfg = self.cfg["modbus"]
        self.word_order = modbus_cfg.get("word_order", "big")

        self.client = ModbusTcpClient(
            modbus_cfg["host"],
            port=modbus_cfg.get("port", 502),
            timeout=modbus_cfg.get("timeout", 5.0),
        )
        if not self.client.connect():
            raise ConfigError(f"Could not connect to Modbus server {modbus_cfg['host']}:{modbus_cfg.get('port', 502)}")

        # POPULATE COMPONENT IDS, ATTRIBUTES AND TAG LIST
        current_time = time.time()
        self._tags = []  # (component_id, key, tag_cfg)
        for tag_cfg in self.cfg["tags"]:
            component_id = tag_cfg["component_id"]
            trial_id = tag_cfg.get("trial_id", self.cfg["trial_id"])

            if component_id not in self.component_ids:
                self.component_ids.append(component_id)
                self._data[component_id] = {}
                self.attributes[component_id] = {"trial_id": trial_id}
                self.last_updated[component_id] = current_time

            key = tag_cfg.get("key") or f"{tag_cfg['register_type']}_{tag_cfg['address']}"
            self._tags.append((component_id, key, tag_cfg))

        # POPULATE INITIAL (BIRTH) DATA
        self.get_data()

        print("ModbusDataAdapter initialized")

    def disconnect(self):
        try:
            self.client.close()
            print("Disconnected from Modbus server")
        except Exception as e:
            print(f"Error while disconnecting from Modbus server: {e}")
        return super().disconnect()

    def get_data(self):
        current_time = time.time()
        for component_id, key, tag_cfg in self._tags:
            value = self.__read_tag(tag_cfg)
            if value is None:
                continue
            self._data[component_id][key] = value
            self.last_updated[component_id] = current_time

    def __read_tag(self, tag_cfg: dict):
        register_type = tag_cfg["register_type"]
        address = tag_cfg["address"]
        device_id = tag_cfg.get("device_id", 1)

        try:
            if register_type == "coil":
                response = self.client.read_coils(address, count=1, device_id=device_id)
            elif register_type == "discrete_input":
                response = self.client.read_discrete_inputs(address, count=1, device_id=device_id)
            else:
                count = _DATATYPE_REGISTER_COUNT[tag_cfg.get("data_type", "uint16")]
                if register_type == "input_register":
                    response = self.client.read_input_registers(address, count=count, device_id=device_id)
                else:
                    response = self.client.read_holding_registers(address, count=count, device_id=device_id)
        except ModbusException as e:
            self.logger.warning(f"Error reading Modbus {register_type} at address {address} (device {device_id}): {e}")
            return None

        if response.isError():
            self.logger.warning(f"Error reading Modbus {register_type} at address {address} (device {device_id}): {response}")
            return None

        if register_type in _BIT_REGISTER_TYPES:
            return bool(response.bits[0])

        data_type = _DATATYPE_MAP[tag_cfg.get("data_type", "uint16")]
        return self.client.convert_from_registers(response.registers, data_type, word_order=self.word_order)
