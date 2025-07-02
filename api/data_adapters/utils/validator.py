from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from mfi_ddb.data_adapters.mqtt      import MqttDataAdapter
from mfi_ddb.data_adapters.local_files import LocalFilesDataAdapter
from mfi_ddb.data_adapters.ros       import RosDataAdapter

ADAPTER_MAP = {
    'mtconnect': MTconnectDataAdapter,
    'mqtt':      MqttDataAdapter,
    'file':      LocalFilesDataAdapter,
    'ros':       RosDataAdapter,
}

def run_validation(config: dict, protocol: str) -> bool:
    """
    Instantiate the adapter with the provided config.
    Raises ValueError with details if construction fails.
    """
    Adapter = ADAPTER_MAP.get(protocol)
    if not Adapter:
        raise ValueError(f"Unknown protocol: {protocol}")
    try:
        # constructor itself performs config checks and may connect
        Adapter(config)
        return True
    except Exception as e:
        raise ValueError(f"Invalid {protocol} config: {e}")