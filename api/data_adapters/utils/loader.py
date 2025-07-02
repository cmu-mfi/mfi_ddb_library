import yaml
from fastapi import UploadFile, HTTPException
from ..models.config_schema import FullConfig

async def load_config(file: UploadFile = None, text: str = None) -> FullConfig:
    if file:
        text = (await file.read()).decode()
    if not text:
        raise HTTPException(400, "No configuration provided")
    try:
        raw = yaml.safe_load(text)
    except Exception as e:
        raise HTTPException(400, f"YAML parse error: {e}")
    try:
        return FullConfig(**raw)
    except Exception as e:
        raise HTTPException(400, f"Config schema error: {e}")