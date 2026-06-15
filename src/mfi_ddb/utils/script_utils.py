import json
import time
import copy

def get_topic_from_config(cfg: dict) -> str:
    """
    Get the topic from the config file.
    """
    topic = f"mfi-v{str(float(cfg['version']))}-{cfg['topic_family']}"
    
    if cfg['topic_family'] != "historian":
        topic = f"{topic}/{cfg['enterprise']}"
        if 'site' in cfg.keys() and cfg['site']:
            topic = f"{topic}/{cfg['site']}"
            if 'area' in cfg.keys() and cfg['area']:
                topic = f"{topic}/{cfg['area']}"
                if 'device' in cfg.keys() and cfg['device']:
                    topic = f"{topic}/{cfg['device']}"           
    else:
        NotImplementedError("Topic for historian is not implemented yet")
        
    return f"{topic}/#"

def get_blob_json_payload_from_dict(data: dict, file_name: str, trial_id:str) -> dict:
    """
    Convert a dictionary to a JSON payload for blob data.
    """
    if not isinstance(data, dict):
        raise TypeError("Data must be a dictionary")

    # check if any value was bytes type
    def drop_bytes_values(d: dict) -> dict:
        for key, value in list(d.items()):
            if isinstance(value, bytes):
                del d[key]
            elif isinstance(value, dict):
                drop_bytes_values(value)
        return d

    data = drop_bytes_values(data)
    json_data = json.dumps(data, indent=4)
    json_bytes = json_data.encode('utf-8')
    
    payload = {
        "file_name": file_name,
        "file_type": '.json',
        "size": json_bytes.__sizeof__(),
        "timestamp": int(time.time()),
        "file": json_bytes,
        "trial_id": trial_id if trial_id else "",
    }
                
    return payload

def redact_cfg(cfg: dict) -> dict:
    """
    Return a copy of cfg with password redacted.
    """
    safe_cfg = copy.deepcopy(cfg)

    if 'mqtt' in safe_cfg.keys():
        if 'password' in safe_cfg['mqtt'].keys():
            safe_cfg['mqtt']['password'] = '***'

    return safe_cfg