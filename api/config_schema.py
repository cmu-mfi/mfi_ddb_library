from pydantic import BaseModel, AnyUrl, IPvAnyAddress, Field
from typing import Optional

class MTConnectConfig(BaseModel):
    agent_ip:    IPvAnyAddress
    agent_url:   AnyUrl
    trial_id:    str
    stream_rate: int = Field(..., gt=0)
    device_name: str

class MQTTConfig(BaseModel):
    broker_address: IPvAnyAddress
    broker_port:    int = Field(..., ge=1, le=65535)
    enterprise:     str
    site:           Optional[str]
    username:       str
    password:       str
    tls_enabled:    bool
    debug:          bool

class FullConfig(BaseModel):
    mtconnect: MTConnectConfig
    mqtt:      MQTTConfig
    topic_family: str 
