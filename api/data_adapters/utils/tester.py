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

def run_test(config: dict, protocol: str) -> bool:
    """
    Instantiate adapter and, if available, call its `test_connection` method.
    Otherwise assume constructor validation suffices.
    """
    Adapter = ADAPTER_MAP.get(protocol)
    if not Adapter:
        raise ValueError(f"Unknown protocol: {protocol}")
    adapter = Adapter(config)
    # optional test_connection hook
    if hasattr(adapter, 'test_connection'):
        return adapter.test_connection()
    return True