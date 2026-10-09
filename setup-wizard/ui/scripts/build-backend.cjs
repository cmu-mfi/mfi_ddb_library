const { spawnSync } = require('node:child_process');
const path = require('node:path');

const python = process.platform === 'win32' ? 'python' : 'python3';
const result = spawnSync(python, ['-m', 'PyInstaller', '--noconfirm', 'main.spec'], {
  cwd: path.resolve(__dirname, '../../backend'),
  stdio: 'inherit',
});

if (result.error) console.error(`Backend build failed: ${result.error.message}`);
process.exit(result.status ?? 1);
