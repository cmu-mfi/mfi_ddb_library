import copy
import itertools
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from schemas import CONFIG_FILES, DOCKER_DIR, MasterConfigPayload
from generators import generate_master_compose, write_runtime_configs, validate_existing_databases
from main import containers_ready


def payload_for(modules):
    return MasterConfigPayload(selectedServices=modules, configs={
        name: yaml.safe_load((DOCKER_DIR / name).read_text())
        for name, module in CONFIG_FILES.items() if module in modules
    })


class RuntimeTests(unittest.TestCase):
    def test_local_database_credentials_ports_and_dollar_escaping(self):
        payload = payload_for(['infra', 'kv', 'ts', 'blob', 'rws'])
        for directory, section, dbkey in [('kv-psql', 'postgres', 'database'), ('timescale', 'timescaledb', 'dbname')]:
            for kind in ('connector', 'dws'):
                payload.configs[f'{directory}/{kind}-config.yaml'][section].update(
                    user='custom user', password='secret$not_env:#', port=5544, **{dbkey: 'custom db'})
        payload.configs['kv-psql/dws-config.yaml']['dws']['port'] = 50111
        payload.configs['metadata-rws/rws-dws-endpoints.yaml']['services']['kv_service']['url'] = 'http://kv-psql-dws:50111'
        payload = MasterConfigPayload.model_validate(payload.model_dump())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_runtime_configs(root / 'runtime_configs', payload)
            generate_master_compose(root, payload)
            compose = yaml.safe_load((root / 'docker-compose.yaml').read_text())
            for name in ['kv-psql-db', 'timescaledb-db']:
                db = compose['services'][name]
                self.assertEqual(db['environment']['POSTGRES_PASSWORD'], 'secret$$not_env:#')
                self.assertEqual(db['environment']['POSTGRES_USER'], 'custom user')
                self.assertEqual(db['command'], ['postgres', '-p', '5544'])
                self.assertIn('"$$POSTGRES_USER"', db['healthcheck']['test'][1])
                self.assertTrue(db['ports'][0].endswith(':5544'))
            self.assertEqual(compose['services']['kv-psql-dws']['ports'], ['50111:50111'])
            self.assertEqual(compose['services']['blob-dws']['ports'], ['50053:50051'])
            self.assertEqual(compose['services']['timescaledb-dws']['ports'], ['50052:50051'])
            self.assertEqual(compose['services']['rws-app']['ports'], ['8002:8000'])
            if shutil.which('docker'):
                resolved = json.loads(subprocess.check_output([
                    'docker', 'compose', '-f', str(root / 'docker-compose.yaml'), '--profile', '*', 'config', '--format', 'json']))
                # Compose's config output retains escaped dollars for safe re-use as a Compose file.
                self.assertEqual(resolved['services']['kv-psql-db']['environment']['POSTGRES_PASSWORD'].replace('$$', '$'), 'secret$not_env:#')

    def test_conflicting_local_settings_rejected_external_settings_preserved(self):
        payload = payload_for(['kv']).model_dump()
        payload['configs']['kv-psql/dws-config.yaml']['postgres']['password'] = 'different'
        with self.assertRaises(ValidationError):
            MasterConfigPayload.model_validate(payload)
        payload['configs']['kv-psql/dws-config.yaml']['postgres']['host'] = 'external.example'
        accepted = MasterConfigPayload.model_validate(payload)
        self.assertEqual(accepted.configs['kv-psql/dws-config.yaml']['postgres']['password'], 'different')

    def test_existing_database_credentials_cannot_be_silently_changed(self):
        payload = payload_for(['kv'])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            generate_master_compose(root, payload)
            (root / '.data/kv_psql_storage/PG_VERSION').write_text('15')
            validate_existing_databases(root, payload)
            for kind in ('connector', 'dws'):
                payload.configs[f'kv-psql/{kind}-config.yaml']['postgres']['password'] = 'new-password'
            with self.assertRaisesRegex(ValueError, 'already has initialized data'):
                validate_existing_databases(root, payload)
            self.assertTrue((root / '.data/kv_psql_storage/PG_VERSION').exists())

    def test_blob_mounts_follow_both_paths(self):
        payload = payload_for(['blob'])
        payload.configs['blob/connector-config.yaml']['config']['save_directory'] = '/write/blobs'
        payload.configs['blob/dws-config.yaml']['config'].update(blob_dir='/read/blobs', index_path='/read/blobs/index.jsonl')
        with tempfile.TemporaryDirectory() as tmp:
            generate_master_compose(Path(tmp), payload)
            services = yaml.safe_load((Path(tmp) / 'docker-compose.yaml').read_text())['services']
            writer = services['blob-connector']['volumes'][0]
            reader = services['blob-dws']['volumes'][0]
            self.assertEqual(writer['source'], reader['source'])
            self.assertEqual(writer['target'], '/write/blobs')
            self.assertEqual(reader['target'], '/read/blobs')
            self.assertTrue(reader['read_only'])

    def test_internal_route_mismatch_rejected(self):
        payload = payload_for(['kv', 'rws']).model_dump()
        payload['configs']['kv-psql/dws-config.yaml']['dws']['port'] = 50111
        with self.assertRaisesRegex(ValidationError, 'internal port 50111'):
            MasterConfigPayload.model_validate(payload)

    @unittest.skipUnless(shutil.which('docker'), 'Docker CLI unavailable')
    def test_all_module_combinations_pass_compose_validation(self):
        modules = ['infra', 'kv', 'ts', 'blob', 'rws', 'daa', 'aveva']
        with tempfile.TemporaryDirectory() as tmp:
            for count in range(1, len(modules) + 1):
                for selected in itertools.combinations(modules, count):
                    with self.subTest(selected=selected):
                        root = Path(tmp)
                        payload = payload_for(list(selected))
                        write_runtime_configs(root / 'runtime_configs', payload)
                        generate_master_compose(root, payload)
                        result = subprocess.run(['docker', 'compose', '-f', str(root / 'docker-compose.yaml'),
                                                 '--profile', '*', 'config', '--quiet'], capture_output=True, text=True)
                        self.assertEqual(result.returncode, 0, result.stderr)


class MonitorTests(unittest.TestCase):
    def test_missing_exited_unhealthy_and_starting_are_not_success(self):
        expected = {'db', 'web'}
        good = [{'Service': 'db', 'State': 'running', 'Health': 'healthy'}, {'Service': 'web', 'State': 'running', 'Health': ''}]
        self.assertTrue(containers_ready(json.dumps(good), expected))
        self.assertTrue(containers_ready('\n'.join(map(json.dumps, good)), expected))
        for output in ['', '[]', '{}', 'not JSON', json.dumps(good[:1])]:
            self.assertFalse(containers_ready(output, expected))
        for state, health in [('exited', ''), ('restarting', ''), ('running', 'unhealthy'), ('running', 'starting')]:
            bad = copy.deepcopy(good)
            bad[0].update(State=state, Health=health)
            self.assertFalse(containers_ready(json.dumps(bad), expected))
