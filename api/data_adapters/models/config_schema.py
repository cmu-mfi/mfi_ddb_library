from pydantic import BaseModel, Field
from typing import Optional, List

class MTConnectConfig(BaseModel):
    agent_ip: str
    agent_url: str
    stream_rate: float = Field(1.0, gt=0)
    device_name: Optional[str]
    trial_id: Optional[str]

class MQTTConfig(BaseModel):
    broker_address: str
    broker_port: int
    enterprise: str
    site: str
    username: Optional[str] = None
    password: Optional[str] = None
    tls_enabled: bool = False
    debug: bool = False

from pydantic import BaseModel, Field
from typing import Optional, List

class MTConnectConfig(BaseModel):
    agent_ip: str
    agent_url: str
    stream_rate: float = Field(1.0, gt=0)
    device_name: Optional[str]
    trial_id: Optional[str]

class MQTTConfig(BaseModel):
    broker_address: str
    broker_port: int
    enterprise: str
    site: str
    topic_family: str
    username: Optional[str]
    password: Optional[str]
    tls_enabled: bool = False
    debug: bool = False

class SystemConfig(BaseModel):
    name: str
    trial_id: str
    description: Optional[str] = None
    type: Optional[str] = None

class FileConfig(BaseModel):
    watch_dir: List[str]
    stream_rate: float = Field(1.0, gt=0)
    wait_before_read: Optional[int] = 1
    buffer_size: Optional[int] = 10
    system: SystemConfig

class ROSConfig(BaseModel):
    ros_topic: str
    ros_master_uri: str
    stream_rate: float = Field(1.0, gt=0)

class FullConfig(BaseModel):
    mtconnect: Optional[MTConnectConfig] = None
    mqtt: Optional[MQTTConfig] = None
    file: Optional[FileConfig] = None
    ros: Optional[ROSConfig] = None
    
    # Top-level fields for flat Local Files structure
    watch_dir: Optional[List[str]] = None
    system: Optional[SystemConfig] = None
    wait_before_read: Optional[int] = None
    buffer_size: Optional[int] = None
    topic_family: Optional[str] = None  # ✅ Add topic_family at root level