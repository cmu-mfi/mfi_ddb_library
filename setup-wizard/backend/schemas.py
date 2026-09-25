from pathlib import Path, PurePosixPath
import sys
from urllib.parse import urlsplit
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, model_validator

ASSET_ROOT = Path(getattr(sys, '_MEIPASS', Path(__file__).resolve().parents[2]))
DOCKER_DIR = ASSET_ROOT / 'docker'
MODULE_DIRS = {'kv': 'kv-psql', 'ts': 'timescale', 'blob': 'blob',
               'rws': 'metadata-rws', 'aveva': 'aveva', 'daa': 'data-adapter-app'}
CONFIG_FILES = {
    str(path.relative_to(DOCKER_DIR)): module
    for module, directory in MODULE_DIRS.items()
    for path in (DOCKER_DIR / directory).glob('*.yaml')
    if not path.name.startswith('compose.')
}


def validate_document(value, template, path):
    """Reject missing/unknown fields and type changes rather than silently dropping data."""
    if isinstance(template, dict):
        if not isinstance(value, dict) or value.keys() != template.keys():
            raise ValueError(f'{path}: fields must match the configuration YAML')
        for key in template:
            validate_document(value[key], template[key], f'{path}.{key}')
    elif isinstance(template, list):
        if not isinstance(value, list) or any(type(item) is not str for item in value):
            raise ValueError(f'{path}: expected a list of strings')
    elif template is None:
        if value is not None and not isinstance(value, str):
            raise ValueError(f'{path}: expected a string or null')
    elif type(value) is not type(template):
        raise ValueError(f'{path}: expected {type(template).__name__}')
    elif type(template) is int and not 1 <= value <= 65535:
        raise ValueError(f'{path}: port must be between 1 and 65535')


LOCAL_DATABASES = {
    'kv': ('kv-psql', 'postgres', 'kv-psql-db', 'database'),
    'ts': ('timescale', 'timescaledb', 'timescaledb-db', 'dbname'),
}


def local_database_settings(configs, module):
    directory, section, service, database_key = LOCAL_DATABASES[module]
    connections = [configs[f'{directory}/{kind}-config.yaml'][section] for kind in ('connector', 'dws')]
    local = [connection for connection in connections if connection['host'] in (service, f'mfi-{service}')]
    if not local:
        return None
    settings = {key: local[0][key] for key in ('port', 'user', 'password', database_key)}
    if any(any(connection[key] != value for key, value in settings.items()) for connection in local[1:]):
        raise ValueError(f'{directory}: connector and DWS settings for the same local database must agree on port, user, password, and database name')
    if not settings['user'] or not settings['password'] or not settings[database_key]:
        raise ValueError(f'{directory}: local database user, password, and database name cannot be empty')
    return settings


def validate_runtime_settings(payload):
    for module in LOCAL_DATABASES:
        if module in payload.selectedServices:
            local_database_settings(payload.configs, module)
    if 'kv' in payload.selectedServices:
        port = payload.configs['kv-psql/dws-config.yaml']['dws']['port']
        reserved = {8000, 5431}
        module_ports = {'infra': {1883, 8083, 18083}, 'ts': {5432, 50052},
                        'blob': {50053}, 'rws': {5430, 8002}, 'daa': {8001, 3001}, 'aveva': {50054}}
        for module in payload.selectedServices:
            reserved.update(module_ports.get(module, set()))
        if port in reserved:
            raise ValueError(f'KV DWS port {port} conflicts with another selected service or the setup backend')
    if 'blob' in payload.selectedServices:
        paths = [payload.configs['blob/connector-config.yaml']['config']['save_directory'],
                 payload.configs['blob/dws-config.yaml']['config']['blob_dir']]
        for path in paths:
            parsed = PurePosixPath(path)
            if not parsed.is_absolute() or '..' in parsed.parts or str(parsed) == '/' or parsed == PurePosixPath('/app'):
                raise ValueError('Blob storage directories must be absolute container paths outside / and /app')
        index = PurePosixPath(payload.configs['blob/dws-config.yaml']['config']['index_path'])
        if index != PurePosixPath(paths[1]) / 'index.jsonl':
            raise ValueError('Blob index_path must be blob_dir/index.jsonl because the connector writes index.jsonl')
    if 'rws' in payload.selectedServices:
        ports = {}
        for module, names, port in [
            ('kv', ('kv-psql-dws', 'mfi-kv-psql-dws'), payload.configs.get('kv-psql/dws-config.yaml', {}).get('dws', {}).get('port', 50051)),
            ('ts', ('timescaledb-dws', 'mfi-timescaledb-dws'), 50051),
            ('blob', ('blob-dws', 'mfi-blob-dws'), 50051),
            ('aveva', ('aveva-pi-dws', 'mfi-aveva-pi-dws'), 50051),
        ]:
            if module in payload.selectedServices:
                ports.update(dict.fromkeys(names, port))
        for name, route in payload.configs['metadata-rws/rws-dws-endpoints.yaml']['services'].items():
            parsed = urlsplit(route['url'])
            if parsed.hostname in ports and parsed.port != ports[parsed.hostname]:
                raise ValueError(f'{name}.url must use internal port {ports[parsed.hostname]} for {parsed.hostname}')


class MasterConfigPayload(BaseModel):
    model_config = ConfigDict(extra='forbid')
    selectedServices: list[Literal['infra', 'kv', 'ts', 'blob', 'rws', 'daa', 'aveva']]
    configs: dict[str, dict[str, Any]]

    @model_validator(mode='after')
    def validate_configs(self):
        if not self.selectedServices:
            raise ValueError('Select at least one service')
        expected = {name for name, module in CONFIG_FILES.items() if module in self.selectedServices}
        if self.configs.keys() != expected:
            raise ValueError('Configuration files must match the selected services')
        for name, document in self.configs.items():
            template = yaml.safe_load((DOCKER_DIR / name).read_text())
            validate_document(document, template, name)
        validate_runtime_settings(self)
        return self
