"""Verify that the installer backend needs only its own packaged files."""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


BACKEND = Path(__file__).resolve().parents[1]


class StandaloneTests(unittest.TestCase):
    def test_generation_outside_repository_and_from_packaged_assets(self):
        for packaged in (False, True):
            with self.subTest(packaged=packaged), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                backend = root / 'backend'
                backend.mkdir()
                for filename in ('schemas.py', 'generators.py'):
                    shutil.copyfile(BACKEND / filename, backend / filename)
                assets = root / 'bundle' if packaged else backend
                shutil.copytree(BACKEND / 'templates', assets / 'templates')
                script = '''
import sys
from pathlib import Path
import yaml
if sys.argv[1] != 'source':
    sys._MEIPASS = sys.argv[1]
from schemas import TEMPLATE_DIR, CONFIG_FILES, MasterConfigPayload
from generators import write_runtime_configs, generate_master_compose
assert len(CONFIG_FILES) == 8
payload = MasterConfigPayload(selectedServices=['infra','kv','ts','blob','rws','daa','aveva'],
    configs={name: yaml.safe_load((TEMPLATE_DIR / name).read_text()) for name in CONFIG_FILES})
runtime = Path(sys.argv[2])
runtime.mkdir()
write_runtime_configs(runtime / 'runtime_configs', payload)
generate_master_compose(runtime, payload)
compose = yaml.safe_load((runtime / 'docker-compose.yaml').read_text())
assert len(compose['services']) == 15
for service in compose['services'].values():
    assert 'build' not in service
    for mount in service.get('volumes', []):
        source = Path(mount['source'])
        assert source.is_relative_to(runtime), source
        assert source.exists(), source
assert (runtime / 'runtime_configs/timescale/init_schema.sql').is_file()
'''
                result = subprocess.run([sys.executable, '-c', script,
                                         str(assets) if packaged else 'source', str(root / 'runtime')],
                                        cwd=backend, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_packaging_spec_bundles_only_wizard_owned_assets(self):
        captured = {}

        class AnalysisStub:
            def __init__(self, scripts, **kwargs):
                captured.update(scripts=scripts, **kwargs)
                self.pure = self.scripts = self.binaries = self.datas = []

        env = {'SPECPATH': str(BACKEND), 'Analysis': AnalysisStub,
               'PYZ': lambda *args, **kwargs: None, 'EXE': lambda *args, **kwargs: None,
               'COLLECT': lambda *args, **kwargs: None}
        exec(compile((BACKEND / 'main.spec').read_text(), 'main.spec', 'exec'), env)
        self.assertEqual(captured['datas'], [(str(BACKEND / 'templates'), 'templates')])
        for path, _ in captured['datas']:
            self.assertTrue(Path(path).is_relative_to(BACKEND))
            self.assertTrue(Path(path).exists())
