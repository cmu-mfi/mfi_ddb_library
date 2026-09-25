from copy import deepcopy
from pathlib import Path
import shutil

import yaml
from schemas import ASSET_ROOT, DOCKER_DIR, MODULE_DIRS, LOCAL_DATABASES, local_database_settings


def write_runtime_configs(config_dir: Path, payload) -> None:
    """Serialize submitted documents without merging or replacing their values."""
    for filename, document in payload.configs.items():
        target = config_dir / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(yaml.safe_dump(document, sort_keys=False, allow_unicode=True))
    # These files have no wizard inputs; retain their checked-in defaults.
    if 'rws' in payload.selectedServices:
        for source in (DOCKER_DIR / 'metadata-rws').glob('*.ini'):
            shutil.copyfile(source, config_dir / 'metadata-rws' / source.name)


def generate_master_compose(runtime_dir: Path, payload) -> None:
    """Use the existing Docker service definitions and mount the submitted configs."""
    base = yaml.safe_load((DOCKER_DIR / 'docker-compose.yaml').read_text())
    result = {'name': base['name'], 'networks': base['networks'], 'services': {}}
    sources = []
    if 'infra' in payload.selectedServices:
        sources.append(('infra', DOCKER_DIR, base['services']))
    for module in payload.selectedServices:
        if module == 'infra':
            continue
        directory = DOCKER_DIR / MODULE_DIRS[module]
        compose = next(directory.glob('compose.*.yaml'))
        sources.append((module, directory, yaml.safe_load(compose.read_text())['services']))

    for module, directory, services in sources:
        for name, original in services.items():
            service = deepcopy(original)
            service.pop('build', None)  # Published images are used by the installer.
            service['profiles'] = [module]
            mounts = []
            for mount in service.get('volumes', []):
                source, target, *mode = mount.split(':')
                resolved = (directory / source).resolve()
                if resolved.is_relative_to(DOCKER_DIR.resolve() / '.data'):
                    relative = resolved.relative_to(DOCKER_DIR.resolve() / '.data')
                    host = runtime_dir / '.data' / relative
                    host.mkdir(parents=True, exist_ok=True)
                elif resolved.is_relative_to(DOCKER_DIR.resolve()):
                    host = runtime_dir / 'runtime_configs' / resolved.relative_to(DOCKER_DIR.resolve())
                else:
                    # Timescale's initialization SQL is packaged alongside the templates.
                    host = ASSET_ROOT / resolved.relative_to(ASSET_ROOT.resolve())
                if module == 'blob' and target == '/data/blob_storage':
                    file = 'connector' if name == 'blob-connector' else 'dws'
                    field = 'save_directory' if file == 'connector' else 'blob_dir'
                    target = payload.configs[f'blob/{file}-config.yaml']['config'][field]
                mounts.append({'type': 'bind', 'source': str(host), 'target': target,
                               'read_only': 'ro' in mode})
            if mounts:
                service['volumes'] = mounts
            result['services'][name] = service

    for module, (_, _, service_name, database_key) in LOCAL_DATABASES.items():
        if module not in payload.selectedServices:
            continue
        settings = local_database_settings(payload.configs, module)
        if settings is None:
            continue
        database = result['services'][service_name]
        database['environment'] = {
            'POSTGRES_USER': settings['user'], 'POSTGRES_PASSWORD': settings['password'],
            'POSTGRES_DB': settings[database_key],
        }
        database['command'] = ['postgres', '-p', str(settings['port'])]
        host_port = database['ports'][0].split(':')[0]
        database['ports'] = [f"{host_port}:{settings['port']}"]
        database['healthcheck']['test'] = [
            'CMD-SHELL', f'pg_isready -h 127.0.0.1 -p {settings["port"]} -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
        ]
    if 'kv' in payload.selectedServices:
        port = payload.configs['kv-psql/dws-config.yaml']['dws']['port']
        result['services']['kv-psql-dws']['ports'] = [f'{port}:{port}']
    if 'rws' in payload.selectedServices:
        # The setup backend occupies host port 8000 throughout deployment.
        result['services']['rws-app']['ports'] = ['8002:8000']

    # An unselected module can be provided externally, so do not retain its dependency.
    for service in result['services'].values():
        dependencies = service.get('depends_on')
        if dependencies is not None:
            service['depends_on'] = ({key: value for key, value in dependencies.items() if key in result['services']}
                                     if isinstance(dependencies, dict)
                                     else [key for key in dependencies if key in result['services']])
            if not service['depends_on']:
                del service['depends_on']
    # Compose interpolates dollar signs even inside YAML-quoted strings.
    def escape(value):
        if isinstance(value, str):
            return value.replace('$', '$$')
        if isinstance(value, list):
            return [escape(item) for item in value]
        if isinstance(value, dict):
            return {key: escape(item) for key, item in value.items()}
        return value
    (runtime_dir / 'docker-compose.yaml').write_text(yaml.safe_dump(escape(result), sort_keys=False))


def validate_existing_databases(runtime_dir: Path, payload) -> None:
    """Postgres initialization variables cannot change credentials on an existing volume."""
    compose_path = runtime_dir / 'docker-compose.yaml'
    if not compose_path.exists():
        return
    old_services = yaml.safe_load(compose_path.read_text()).get('services', {})
    for module, (_, _, service, database_key) in LOCAL_DATABASES.items():
        storage = 'kv_psql_storage' if module == 'kv' else 'timescale_storage'
        if module not in payload.selectedServices or not (runtime_dir / '.data' / storage / 'PG_VERSION').exists():
            continue
        settings = local_database_settings(payload.configs, module)
        if settings is None:
            continue
        previous = old_services.get(service, {}).get('environment', {})
        if isinstance(previous, list):
            previous = dict(entry.split('=', 1) for entry in previous)
        desired = {'POSTGRES_USER': settings['user'], 'POSTGRES_PASSWORD': settings['password'],
                   'POSTGRES_DB': settings[database_key]}
        if any(previous.get(key, '').replace('$$', '$') != value for key, value in desired.items()):
            raise ValueError(f'{service} already has initialized data. Keep its existing credentials and database name, or migrate the database before changing them. No data has been deleted.')
