"""MTConnect Data Adapter Module.

This module provides a data adapter for interfacing with MTConnect agents.
MTConnect is a manufacturing industry standard for data exchange between
manufacturing equipment and software applications.

The MTconnectDataAdapter class handles:
- Connecting to MTConnect agents via HTTP
- Retrieving device probe information to understand device structure
- Polling current and sample data from MTConnect devices
- Converting MTConnect XML data into a structured format for streaming
- Managing component IDs, attributes, and data buffers

Typical usage:
    config = {
        'mtconnect': {
            'agent_ip': '192.168.1.100',
            'agent_url': 'http://192.168.1.100:5000',
            'device_name': 'CNC_Machine',
            'trial_id': 'trial_001'
        }
    }
    adapter = MTconnectDataAdapter(config)
    adapter.get_data()  # Retrieves current device data
"""

import time

import omegaconf
import requests
import xmltodict
from omegaconf import OmegaConf
from ping3 import ping
from pydantic import BaseModel, Field

from mfi_ddb.data_adapters.base import BaseDataAdapter


class _SCHEMA(BaseModel):
    class _MTCONNECT(BaseModel):
        agent_ip: str = Field(..., description="IP address of the MTConnect agent")
        agent_url: str = Field(..., description="URL of the MTConnect agent")
        device_name: str = Field(..., description="Name of the device to be used in the data object")
        trial_id: str = Field(..., description="Trial ID for the MTConnect device. No spaces or special characters allowed.")    
    class SCHEMA(BaseModel):
        mtconnect: "_MTCONNECT" = Field(..., description="Configuration for the MTConnect agent connection")

class MTconnectDataAdapter(BaseDataAdapter):
    """Data adapter for MTConnect manufacturing devices.
    
    This adapter connects to an MTConnect agent (a server that exposes device data
    via HTTP) and retrieves manufacturing device information. It handles:
    
    - Device connectivity verification via ICMP ping
    - Device probe to discover component structure and data items
    - Continuous data polling from MTConnect current/sample endpoints
    - XML-to-dict conversion and data structuring
    - Automatic type conversion for numeric values
    
    The adapter organizes data by component IDs (e.g., "MachineName/ComponentID")
    and maintains attributes and timestamps for each component.
    
    Attributes:
        NAME (str): Identifier for this adapter type
        CONFIG_HELP (dict): Help text for configuration parameters
        CONFIG_EXAMPLE (dict): Example configuration structure
        RECOMMENDED_TOPIC_FAMILY (str): Suggested topic family for streaming ("historian")
        SELF_UPDATE (bool): Whether adapter auto-updates (False - requires polling)
    """
    
    NAME = "MTConnect"
    
    CONFIG_HELP = {
        "mtconnect": {
            "agent_ip": "IP address of the MTConnect agent",
            "agent_url": "URL of the MTConnect agent",
            "device_name": "Name of the device to be used in the data object",
            "trial_id": "Trial ID for the MTConnect device. No spaces or special characters allowed.",
        }
    }
    
    CONFIG_EXAMPLE = {
        "mtconnect": {
            "agent_ip": "192.168.1.1",
            "agent_url": "http://192.168.1.1:5000",
            "device_name": "MTConnectDevice",
            "trial_id": "trial_001"
        }
    }
    
    RECOMMENDED_TOPIC_FAMILY = "historian"
    
    SELF_UPDATE = False
    
    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        """
        Schema for the MTConnect data adapter configuration.
        """
        pass

    def __init__(self, config: dict):
        """Initialize the MTConnect data adapter.
        
        Performs the following initialization steps:
        1. Validates connection to the MTConnect agent via ping
        2. Sends a probe request to discover device structure
        3. Extracts device and component information
        4. Populates component IDs, attributes, and data buffers
        
        Args:
            config (dict): Configuration dictionary containing MTConnect agent
                connection details. Must include 'mtconnect' key with:
                - agent_ip: IP address of the MTConnect agent
                - agent_url: Full URL to the MTConnect agent
                - device_name: Name identifier for the device
                - trial_id: Trial identifier for data organization
                - mtconnect_uuid (optional): Specific device UUID if multiple devices exist
                - timeout (optional): Connection timeout in seconds (default: 5)
        
        Raises:
            ConnectionError: If MTConnect agent is not reachable within timeout period
        """
        super().__init__()
        
        self.cfg = config
        self.device_name = self.cfg['mtconnect']['device_name']
        # CHECK IF MTCONNECT AGENT IS ACTIVE
        self.__connect()
        
        # POPULATE COMPONENT IDS AND ATTRIBUTES
        # Note: By default, only first device, from the list of MTConnect devices, is used.
        #       If multiple devices exist, specify config['mtconnect']['mtconnect_uuid'] to use a specific device.
        probe_data_raw = self.__request_agent('probe')
        device_data = probe_data_raw.MTConnectDevices.Devices.Device
        if isinstance(device_data, omegaconf.listconfig.ListConfig):
            if 'mtconnect_uuid' in self.cfg['mtconnect'].keys():
                device_id = self.cfg['mtconnect']['mtconnect_uuid']
                for device in device_data:
                    if device['@uuid'] == device_id:
                        device_data = device
                        break
            else:
                device_data = device_data[0]
                
        current_time = time.time()
            
        # Populate device attributes
        device_attributes = {}
        device_attributes['trial_id'] = self.cfg['mtconnect']['trial_id']
        for key in device_data.keys():
            if not isinstance(device_data[key], omegaconf.dictconfig.DictConfig):
                device_attributes[key] = device_data[key]
            elif key == 'Description':
                for sub_key in device_data[key].keys():
                    if not isinstance(device_data[key][sub_key], omegaconf.dictconfig.DictConfig):
                        device_attributes[sub_key] = device_data[key][sub_key]

        # Populate component and data attributes
        components_data = device_data
        component_data = self.__get_probe_components(components_data)
        for component in component_data:
            component_id = f'{self.device_name}/{component["@id"]}'
            self.last_updated[component_id] = current_time
            self.component_ids.append(component_id)
            self.attributes[component_id] = device_attributes.copy()
            self._data[component_id] = {}
            data_list = component.DataItems.DataItem
            if not isinstance(data_list, omegaconf.listconfig.ListConfig):
                data_list = [data_list]
            for data_item in data_list:
                for key in data_item.keys():
                    self._data[component_id][f'{data_item["@id"]}/{key}'] = str(data_item[key])                
                    
    def get_data(self):
        """Retrieve current data from the MTConnect agent.
        
        Queries the MTConnect 'current' endpoint to get the latest snapshot
        of all data items across all device components. This provides a
        complete picture of the device state at the current moment.
        
        The retrieved data is parsed and stored in the internal data buffer,
        organized by component IDs. This method is typically called for
        initial data population or one-time queries.
        """
        raw_data = self.__request_agent('current')
        devices = raw_data.MTConnectStreams.Streams.DeviceStream
        device = devices[0] if isinstance(devices, omegaconf.listconfig.ListConfig) else devices
        
        component_stream = device.ComponentStream
        if not isinstance(component_stream, omegaconf.listconfig.ListConfig):
            component_stream = [component_stream]
            
        self.__populate_data(component_stream)
    
    def update_data(self):
        """Update data by polling the MTConnect sample endpoint.
        
        Queries the MTConnect 'sample' endpoint to retrieve time-series data
        samples. This is used for continuous monitoring and is typically called
        in a polling loop to capture device data changes over time.
        
        The retrieved samples are parsed and update the internal data buffer,
        with timestamps tracking when each component was last updated.
        """
        raw_data = self.__request_agent('sample')
        devices = raw_data.MTConnectStreams.Streams.DeviceStream
        device = devices[0] if isinstance(devices, omegaconf.listconfig.ListConfig) else devices
        
        component_stream = device.ComponentStream
        if not isinstance(component_stream, omegaconf.listconfig.ListConfig):
            component_stream = [component_stream]
        
        self.__populate_data(component_stream)
        
        
    def __connect(self):
        """Verify connectivity to the MTConnect agent.
        
        Uses ICMP ping to check if the MTConnect agent is reachable on the network.
        Retries for a configurable timeout period (default 5 seconds) before failing.
        
        The while loop condition ensures both conditions must be true to continue waiting:
        - response is None (no successful ping yet)
        - time_elapsed < timeout (haven't exceeded timeout period)
        
        This prevents infinite waiting and ensures timely failure detection.
        
        Raises:
            ConnectionError: If the agent doesn't respond within the timeout period
        
        Note:
            Prints status messages to console during connection attempts
        """
        # Ping to see if MTConnect agent is active -----------------------------------
        print("Checking if MTConnect agent is active ...")
        ip = self.cfg['mtconnect']['agent_ip']
        
        response = ping(ip)
        timeout = self.cfg['mtconnect'].get('timeout', 5) if 'timeout' in self.cfg['mtconnect'] else 5
        
        start_time = time.time()
        time_elapsed = 0
        while response is None and time_elapsed < timeout:
            print("MTConnect agent is not active. Waiting ...")
            time.sleep(1)
            response = ping(ip)
            time_elapsed = time.time() - start_time
            
        if response is None:
            raise ConnectionError(f"MTConnect agent at {ip} is not responding after {timeout} seconds.")
        
        print(f"MTConnect agent at {ip} is active")    
        
    def __request_agent(self, ext: str):
        """Send HTTP request to the MTConnect agent.
        
        Constructs and sends a GET request to the MTConnect agent, appending
        the specified endpoint extension to the base URL. Common extensions are:
        - 'probe': Get device structure and capabilities
        - 'current': Get current snapshot of all data items
        - 'sample': Get time-series samples of data
        
        Args:
            ext (str): Endpoint extension to append to the agent URL
                (e.g., 'probe', 'current', 'sample')
        
        Returns:
            OmegaConf: Parsed MTConnect XML response as an OmegaConf object,
                providing dict-like access to the structured data
        """
        URL = self.cfg['mtconnect']['agent_url']
        response = requests.get(URL + ext)
        val = xmltodict.parse(response.text, encoding='utf-8')
        val = OmegaConf.create(val)        
        return val
            
    def __get_probe_components(self, data, component_data=None):
        """Recursively extract components with DataItems from probe response.
        
        Traverses the hierarchical structure of the MTConnect probe response
        to find all components that contain DataItems (actual data points).
        This recursive function handles nested components at any depth.
        
        Args:
            data: MTConnect probe data structure (dict or list)
            component_data (list, optional): Accumulator list for found components.
                Defaults to None (creates new list).
        
        Returns:
            list: All components that have DataItems defined, which represent
                the actual data sources within the device
        """
        if component_data is None:
            component_data = []
        
        if isinstance(data, omegaconf.dictconfig.DictConfig):
            if 'DataItems' in data.keys():
                component_data.append(data)
            for key in data.keys():
                self.__get_probe_components(data[key], component_data)
        elif isinstance(data, omegaconf.listconfig.ListConfig):
            for item in data:
                self.__get_probe_components(item, component_data)
        
        return component_data

    def __populate_data(self, raw_data):
        """Parse and store MTConnect stream data into internal data buffer.
        
        Processes component stream data from MTConnect current/sample responses.
        Recursively extracts key-value pairs from the hierarchical data structure
        and stores them in the component-specific data buffers.
        
        The method handles:
        - Events and Samples data from each component
        - Nested data structures (dicts and lists)
        - Special key substitutions (e.g., '#text' becomes 'value')
        - Automatic type conversion for numeric values
        - Timestamp tracking for each component update
        
        Args:
            raw_data (list): List of ComponentStream objects from MTConnect response,
                containing Events and/or Samples data for each component
        
        Note:
            Updates self._data and self.last_updated for each affected component
        """
        
        current_time = time.time()
        
        for component_data in raw_data:
            component_id = f'{self.device_name}/{component_data["@componentId"]}'
            
            def extract_key_value(data_item, data_item_key):
                if isinstance(data_item, omegaconf.dictconfig.DictConfig):
                    for key in data_item.keys():
                        substitute_key = key
                        if key == '#text':
                            substitute_key = 'value'
                        extract_key_value(data_item[key], f'{data_item_key}/{substitute_key}')
                elif isinstance(data_item, omegaconf.listconfig.ListConfig):
                        for i, item in enumerate(data_item):
                            extract_key_value(item, f'{data_item_key}_{i}')
                else:
                    try:
                        self._data[component_id][data_item_key] = self.__autotype(data_item)           
                    except KeyError:
                        print("WARNING: KeyError: ", data_item_key, " not found in data buffer for component ", component_id)
                    self.last_updated[component_id] = current_time
            
            for key in component_data.keys():
                if key in ['Events', 'Samples']:
                    for data_item_list in component_data[key].values():
                        if not isinstance(data_item_list, omegaconf.listconfig.ListConfig):
                            data_item_list = [data_item_list]                            
                        for data_item in data_item_list:
                            data_item_id = data_item['@dataItemId']
                            extract_key_value(data_item, data_item_id)                                                         

    def __autotype(self, value):
        """Automatically convert string values to appropriate numeric types.
        
        Attempts to convert the input value to int first, then float,
        falling back to the original value if neither conversion succeeds.
        This ensures numeric data is stored in proper types for analysis.
        
        Args:
            value: Input value (typically string from XML parsing)
        
        Returns:
            int, float, or original value: Converted value in the most
                appropriate type (int preferred over float over string)
        """
        try:
            return int(value)
        except:
            try:
                return float(value)
            except:
                return value
