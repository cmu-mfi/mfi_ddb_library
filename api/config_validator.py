from .config_schema import MTConnectConfig, MQTTConfig

def validate_mtconnect(user: MTConnectConfig, ref: MTConnectConfig):
    if user.device_name != ref.device_name:
        raise ValueError(f"device_name must be {ref.device_name!r}")
    if user.trial_id    != ref.trial_id:
        raise ValueError(f"trial_id must be {ref.trial_id!r}")

def validate_mqtt(user: MQTTConfig, ref: MQTTConfig):
    if user.enterprise != ref.enterprise:
        raise ValueError("enterprise not recognized")
    if user.site       != ref.site:
        raise ValueError(f"site must be {ref.site!r}")
    # enforce your secret password from the ref
    user.password = ref.password
