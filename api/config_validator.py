from api.config_schema import MTConnectConfig, MQTTConfig


def validate_mtconnect(user: MTConnectConfig, ref: MTConnectConfig):
    if user.device_name != ref.device_name:
        raise ValueError(f"device_name must be {ref.device_name!r}")
    if user.trial_id != ref.trial_id:
        raise ValueError(f"trial_id must be {ref.trial_id!r}")


def validate_mqtt(user: MQTTConfig, ref: MQTTConfig):
    if user.enterprise != ref.enterprise:
        raise ValueError(f"enterprise must be {ref.enterprise!r}")
    if user.site and user.site != ref.site:
        raise ValueError(f"site must be {ref.site!r}")
    # enforce secret password from ref
    user.password = ref.password