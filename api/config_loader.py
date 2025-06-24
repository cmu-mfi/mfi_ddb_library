import yaml
from .config_schema import MTConnectConfig, MQTTConfig

def load_ref_mtconnect(path: str = "ref_configs/mtconnect.yaml") -> MTConnectConfig:
    raw = yaml.safe_load(open(path))
    # if YAML is:
    # mtconnect:
    #   agent_ip: …
    #   …
    # we need the nested dict:
    data = raw.get("mtconnect", raw)
    print(data)
    return MTConnectConfig(**data)

def load_ref_mqtt(path: str = "ref_configs/mqtt.yaml") -> MQTTConfig:
    raw = yaml.safe_load(open(path))
    # if YAML is:
    # topic_family: historian
    # mqtt:
    #   broker_address: …
    #   …
    # we want just the mqtt: block
    data = raw.get("mqtt", raw)
    return MQTTConfig(**data)
