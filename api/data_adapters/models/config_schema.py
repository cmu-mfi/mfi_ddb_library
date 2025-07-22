from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

# MTConnect adapter configuration
class MTConnectConfig(BaseModel):
    agent_ip: str  # Agent IP address
    agent_url: str  # Agent URL
    stream_rate: float = Field(1.0, gt=0)  # Polling rate in seconds (>0)
    device_name: Optional[str] = None  # Optional device identifier
    trial_id: Optional[str] = None  # Optional trial ID

# MQTT broker connection settings
class MQTTConfig(BaseModel):
    broker_address: str  # Broker host/IP
    broker_port: int  # Broker port
    enterprise: str  # Organization name
    site: str  # Site/facility name
    username: Optional[str] = None  # Auth username
    password: Optional[str] = None  # Auth password
    tls_enabled: bool = False  # Enable TLS encryption
    debug: bool = False  # Enable debug logging

class Topic(BaseModel):
    topic: str
    component_id: str
    trial_id: Optional[str] = None
class MQTTAdapConfig(BaseModel):
    broker_address: str  # Adapter host/IP
    broker_port: int  # Adapter port
    username: Optional[str] = None  # Auth username
    password: Optional[str] = None  # Auth password
    trial_id: str
    queue_size: int = Field(..., gt=0, description="max number of messages to buffer")
    topics: List[Topic]
# Common system metadata
class SystemConfig(BaseModel):
    name: str  # System name
    trial_id: str  # Trial/experiment ID
    description: Optional[str] = None  # Optional description
    type: Optional[str] = None  # Optional system type

# File system adapter config
class FileConfig(BaseModel):
    watch_dir: List[str]  # Directories to monitor
    wait_before_read: Optional[int] = 1  # Seconds after file creation
    buffer_size: Optional[int] = 10  # File buffer count
    system: SystemConfig  # System metadata

# ROS device attributes
class ROSDeviceAttributes(BaseModel):
    description: str  # Device description
    type: str  # Device type (e.g., "Robot")
    version: float  # Device version
    trial_id: Optional[str] = None  # Optional trial ID override

# ROS device configuration
class ROSDevice(BaseModel):
    namespace: str  # ROS namespace for the device
    rostopics: List[str]  # List of ROS topics to subscribe to
    attributes: ROSDeviceAttributes  # Device metadata

# ROS adapter configuration (live ROS connections)
class ROSConfig(BaseModel):
    trial_id: str  # Trial/experiment identifier
    set_ros_callback: bool = False  # Enable ROS callback mode
    devices: Dict[str, ROSDevice]  # Named device configurations

# ROS Files adapter configuration (bag file processing)
class ROSFilesConfig(BaseModel):
    trial_id: str  # Trial/experiment identifier
    set_ros_callback: bool = False  # Enable ROS callback mode
    devices: Dict[str, ROSDevice]  # Named device configurations
    

# Main config model (supports all adapters)
class FullConfig(BaseModel):
    # Nested configurations
    mtconnect: Optional[MTConnectConfig] = None
    mqtt: Optional[MQTTConfig] = None
    file: Optional[FileConfig] = None 
    ros: Optional[ROSConfig] = None
    ros_files: Optional[ROSFilesConfig] = None
    

    topic_family: Optional[str] = None