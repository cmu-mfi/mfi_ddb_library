import json
import time

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