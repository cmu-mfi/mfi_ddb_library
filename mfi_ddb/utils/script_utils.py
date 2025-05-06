

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