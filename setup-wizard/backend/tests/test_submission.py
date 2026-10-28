"""Run: python -m unittest discover -s setup-wizard/backend/tests -v"""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main

UI_DIR = Path(__file__).resolve().parents[2] / 'ui'


class SubmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Exercise the actual frontend serializer with a distinct edit in every field.
        script = """
          import { YAML_CONFIG_BLUEPRINT as blueprint } from './src/config/yamlConfig.js';
          import { buildConfigPayload } from './src/services/configPayload.js';
          const values = {};
          for (const config of Object.values(blueprint)) {
            for (const field of config.fields) {
              values[field.key] = field.type === 'number' ? field.default + 1
                : field.type === 'boolean' ? !field.default
                : field.type === 'list' ? ['topic/#', 'second: value']
                : field.default === null ? null
                : `edited ${field.key}: # ' \\n ü $value`;
            }
          }
          const selected = Object.fromEntries(['infra','kv','ts','blob','rws','daa','aveva'].map(key => [key,true]));
          console.log(JSON.stringify(buildConfigPayload(values, selected)));
        """
        cls.payload = json.loads(subprocess.check_output(['node', '--input-type=module', '-e', script], cwd=UI_DIR))
        cls.payload['configs']['kv-psql/dws-config.yaml']['dws']['port'] = 50111
        cls.payload['configs']['blob/connector-config.yaml']['config']['save_directory'] = '/data/edited-connector'
        cls.payload['configs']['blob/dws-config.yaml']['config'].update(
            blob_dir='/data/edited-reader', index_path='/data/edited-reader/index.jsonl')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime = Path(self.temp.name)
        self.config_dir = self.runtime / 'runtime_configs'
        for name, value in [('RUNTIME_DIR', self.runtime), ('CONFIG_DIR', self.config_dir)]:
            patcher = patch.object(main, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = TestClient(main.app)

    def test_every_frontend_value_survives_http_and_yaml(self):
        response = self.client.post('/api/deploy', json=self.payload)
        self.assertEqual(response.status_code, 201)
        for filename, expected in self.payload['configs'].items():
            self.assertEqual(yaml.safe_load((self.config_dir / filename).read_text()), expected)
        compose = yaml.safe_load((self.runtime / 'docker-compose.yaml').read_text())
        mounted = {mount['source'] for service in compose['services'].values() for mount in service.get('volumes', [])}
        for filename in self.payload['configs']:
            self.assertIn(str(self.config_dir / filename), mounted)
        self.assertIn('aveva-pi-dws', compose['services'])
        self.assertIn('data-adapter-backend', compose['services'])

    def test_single_module_and_empty_values(self):
        payload = copy.deepcopy(self.payload)
        payload['selectedServices'] = ['blob']
        payload['configs'] = {key: value for key, value in payload['configs'].items() if key.startswith('blob/')}
        connector = payload['configs']['blob/connector-config.yaml']
        connector['mqtt']['username'] = ''
        connector['mqtt']['tls_enabled'] = False
        connector['config']['topic']['site'] = ''
        self.assertEqual(self.client.post('/api/deploy', json=payload).status_code, 201)
        self.assertEqual(yaml.safe_load((self.config_dir / 'blob/connector-config.yaml').read_text()), connector)
        compose = yaml.safe_load((self.runtime / 'docker-compose.yaml').read_text())
        self.assertEqual(set(compose['services']), {'blob-connector', 'blob-dws'})
        self.assertNotIn('depends_on', compose['services']['blob-connector'])

    def test_invalid_payloads_do_not_write_files(self):
        invalid = []
        missing = copy.deepcopy(self.payload)
        del missing['configs']['aveva/config.yaml']['password']
        invalid.append(missing)
        unknown = copy.deepcopy(self.payload)
        unknown['configs']['../../escape.yaml'] = {}
        invalid.append(unknown)
        bad_type = copy.deepcopy(self.payload)
        bad_type['configs']['blob/connector-config.yaml']['mqtt']['tls_enabled'] = 'false'
        invalid.append(bad_type)
        invalid.append({'selectedServices': [], 'configs': {}})
        for payload in invalid:
            self.assertEqual(self.client.post('/api/deploy', json=payload).status_code, 422)
        self.assertFalse(self.config_dir.exists())

    def test_service_without_config_files(self):
        self.assertEqual(self.client.post('/api/deploy', json={'selectedServices': ['infra'], 'configs': {}}).status_code, 201)


if __name__ == '__main__':
    unittest.main()
