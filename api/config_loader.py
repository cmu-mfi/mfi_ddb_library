import yaml
from api.config_schema import MTConnectConfig, MQTTConfig


def load_ref_mtconnect(path: str = "ref_configs/mtconnect.yaml") -> MTConnectConfig:
    raw = yaml.safe_load(open(path))
    data = raw.get("mtconnect", raw)
    return MTConnectConfig(**data)


def load_ref_mqtt(path: str = "ref_configs/mqtt.yaml") -> MQTTConfig:
    raw = yaml.safe_load(open(path))
    data = raw.get("mqtt", raw)
    return MQTTConfig(**data)