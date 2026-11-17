#!/usr/bin/env bash
# Self-contained source installer for Linux and macOS.
set -euo pipefail

module_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
backend="$module_dir/data_adapter_app/backend"
frontend="$module_dir/data_adapter_app/frontend"
tools_dir="$module_dir/.tools"
state="$tools_dir/install.json"
action="${1:-install}"
case "$action" in
    install|check|start) if [ "$#" -gt 0 ]; then shift; fi ;;
    -*) action=install ;;
    *) echo 'Use setup.sh [install|start|check].' >&2; exit 2 ;;
esac
skip_packages=0
backend_port=8000
frontend_port=3000
while [ "$#" -gt 0 ]; do
    case "$1" in
        --help|-h)
            echo 'Usage: bash setup.sh [install|start|check] [options]'
            echo '  install (default): install prerequisites/dependencies and build the UI.'
            echo '  start: run the installed API and UI; Ctrl+C stops both.'
            echo '  check: verify the installed environment without downloading anything.'
            echo '  Options: --no-system-packages, --backend-port PORT, --frontend-port PORT'
            exit 0 ;;
        --no-system-packages) skip_packages=1; shift ;;
        --backend-port|--frontend-port)
            [ "$#" -ge 2 ] || { echo "Missing port after $1" >&2; exit 2; }
            [[ "$2" =~ ^[0-9]{1,5}$ ]] && [ "$((10#$2))" -ge 1 ] && [ "$((10#$2))" -le 65535 ] || {
                echo "Invalid port: $2" >&2; exit 2;
            }
            if [ "$1" = --backend-port ]; then backend_port="$((10#$2))"; else frontend_port="$((10#$2))"; fi
            shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

case "$(uname -s)" in
    Darwin) node_os=darwin ;;
    Linux) node_os=linux ;;
    *) echo 'Use setup.bat on Windows.' >&2; exit 1 ;;
esac
case "$(uname -m)" in
    x86_64) node_arch=x64 ;;
    arm64|aarch64) node_arch=arm64 ;;
    *) echo 'An x86-64 or ARM64 machine is required.' >&2; exit 1 ;;
esac
export UV_PYTHON_INSTALL_DIR="$tools_dir/python" UV_CACHE_DIR="$tools_dir/cache/uv" UV_NO_CONFIG=1
export npm_config_cache="$tools_dir/cache/npm" VITE_API_URL=/api
uv_bin="$tools_dir/uv/uv"

install_packages() {
    [ "$skip_packages" = 0 ] || { echo "Missing OS packages: $*. Rerun without --no-system-packages." >&2; exit 1; }
    local elevate=()
    if [ "$(id -u)" != 0 ]; then
        command -v sudo >/dev/null || { echo 'Ask an administrator to install the listed OS packages.' >&2; exit 1; }
        elevate=(sudo)
    fi
    "${elevate[@]}" "$@"
}

download() {
    if command -v curl >/dev/null 2>&1; then
        curl --fail --show-error --location --proto '=https' --proto-redir '=https' --retry 3 --connect-timeout 30 --output "$2" "$1"
    else
        wget --https-only --tries=3 --timeout=30 -O "$2" "$1"
    fi
}

if [ "$action" = install ]; then
    # Minimal Linux images may not have download/unpack tools or CA certificates.
    if ! command -v tar >/dev/null || ! command -v gzip >/dev/null || \
       { ! command -v curl >/dev/null && ! command -v wget >/dev/null; } || \
       { [ "$node_os" = linux ] && [ ! -s /etc/ssl/certs/ca-certificates.crt ] && [ ! -s /etc/pki/tls/certs/ca-bundle.crt ]; }; then
        if command -v apt-get >/dev/null; then
            install_packages apt-get update
            install_packages apt-get install -y ca-certificates curl tar gzip
        elif command -v dnf >/dev/null; then install_packages dnf install -y ca-certificates curl tar gzip
        elif command -v zypper >/dev/null; then install_packages zypper --non-interactive install ca-certificates curl tar gzip
        elif command -v pacman >/dev/null; then install_packages pacman -S --needed --noconfirm ca-certificates curl tar gzip
        else echo 'Install CA certificates, curl (or wget), tar and gzip, then retry.' >&2; exit 1
        fi
    fi
    mkdir -p "$tools_dir"
    installer="$(mktemp "$tools_dir/uv-install.XXXXXX")"
    trap 'rm -f -- "$installer"' EXIT
    download https://astral.sh/uv/install.sh "$installer"
    UV_UNMANAGED_INSTALL="$tools_dir/uv" sh "$installer"
    "$uv_bin" python install 3.12 --upgrade --no-bin --no-registry
    rm -f -- "$installer"
    trap - EXIT
fi
[ -x "$uv_bin" ] || { echo 'Run setup.sh install first.' >&2; exit 1; }
managed_python="$("$uv_bin" python find --managed-python --no-python-downloads 3.12)"

if [ "$action" = install ]; then
    "$managed_python" - <<'PY'
import os, platform, re, sys
if sys.platform == 'darwin' and tuple(map(int, platform.mac_ver()[0].split('.')[:2])) < (13, 5):
    raise SystemExit('macOS 13.5 or newer is required.')
if sys.platform == 'linux':
    try:
        libc = os.confstr('CS_GNU_LIBC_VERSION') or ''
    except ValueError:
        libc = ''
    if not libc.startswith('glibc ') or tuple(map(int, libc.split()[-1].split('.'))) < (2, 31):
        raise SystemExit('Use glibc >= 2.31 Linux; Alpine/musl is unsupported.')
    kernel = tuple(map(int, re.match(r'(\d+)\.(\d+)', platform.release()).groups()))
    if kernel < (4, 18):
        raise SystemExit('Linux kernel 4.18 or newer is required.')
PY
    if [ "$node_os" = linux ] && ! "$managed_python" - <<'PY'
import ctypes
for library in ('libGL.so.1', 'libEGL.so.1', 'libgomp.so.1', 'libglib-2.0.so.0', 'libstdc++.so.6', 'libatomic.so.1'):
    try:
        ctypes.CDLL(library)
    except OSError:
        raise SystemExit(1)
PY
    then
        if command -v apt-get >/dev/null; then
            install_packages apt-get update
            glib=libglib2.0-0
            if apt-cache show libglib2.0-0t64 >/dev/null 2>&1; then glib=libglib2.0-0t64; fi
            install_packages apt-get install -y libgl1 libegl1 libgomp1 "$glib" libstdc++6 libatomic1
        elif command -v dnf >/dev/null; then install_packages dnf install -y mesa-libGL mesa-libEGL libgomp glib2 libstdc++ libatomic
        elif command -v zypper >/dev/null; then install_packages zypper --non-interactive install Mesa-libGL1 libEGL1 libgomp1 libglib-2_0-0 libstdc++6 libatomic1
        elif command -v pacman >/dev/null; then install_packages pacman -S --needed --noconfirm libglvnd gcc-libs glib2
        else echo 'Install OpenGL/EGL, OpenMP, GLib, C++ and atomic runtimes, then retry.' >&2; exit 1
        fi
    fi

    download https://nodejs.org/dist/index.json "$tools_dir/node-releases.json"
    node_version="$("$managed_python" - "$tools_dir/node-releases.json" <<'PY'
import json, re, sys
versions = [r['version'] for r in json.load(open(sys.argv[1])) if r.get('lts') and re.fullmatch(r'v\d+\.\d+\.\d+', r['version'])]
print(max(versions, key=lambda v: tuple(map(int, v[1:].split('.')))))
PY
)"
    node_root="node-$node_version-$node_os-$node_arch"
    node_bin="$tools_dir/node/$node_root/bin/node"
    if [ ! -x "$node_bin" ] || [ ! -f "$tools_dir/node/$node_root/lib/node_modules/npm/bin/npm-cli.js" ]; then
        archive="$tools_dir/$node_root.tar.gz"
        download "https://nodejs.org/dist/$node_version/$node_root.tar.gz" "$archive"
        download "https://nodejs.org/dist/$node_version/SHASUMS256.txt" "$tools_dir/node-checksums.txt"
        "$managed_python" - "$archive" "$tools_dir/node-checksums.txt" "$tools_dir/node" "$node_root" <<'PY'
import hashlib, pathlib, sys, tarfile
archive, sums, destination, root = map(pathlib.Path, sys.argv[1:])
expected = next(line.split()[0] for line in sums.read_text().splitlines() if line.split()[-1].lstrip('*') == archive.name)
with archive.open('rb') as source:
    if hashlib.file_digest(source, 'sha256').hexdigest() != expected:
        raise SystemExit('Node archive checksum mismatch; rerun setup.')
with tarfile.open(archive) as bundle:
    if any(pathlib.PurePosixPath(item.name).parts[0] != str(root) for item in bundle.getmembers()):
        raise SystemExit('Unexpected Node archive contents.')
    bundle.extractall(destination, filter='data')
PY
    fi
    [ "$("$node_bin" --version)" = "$node_version" ] || { echo 'Damaged Node runtime; move its .tools/node folder aside and rerun.' >&2; exit 1; }
    python_version="$("$managed_python" -c 'import platform; print(platform.python_version())')"
    venv_python="$tools_dir/venvs/$python_version/bin/python"
    rm -f -- "$state"
    "$uv_bin" venv --allow-existing --python "$managed_python" "$tools_dir/venvs/$python_version"
    printf '%s\n' 'open3d!=0.20.0; sys_platform == "darwin"' > "$tools_dir/constraints.txt"
    "$uv_bin" pip install --python "$venv_python" --only-binary numpy,pandas,open3d,opencv-python-headless,grpcio,grpcio-tools,pydantic-core,scipy,scikit-learn \
        --constraint "$tools_dir/constraints.txt" --requirements "$backend/requirements.txt"
else
    [ -f "$state" ] || { echo 'Run setup.sh install first.' >&2; exit 1; }
    installed_path() {
        "$managed_python" - "$state" "$module_dir" "$1" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[2])
path = root / json.load(open(sys.argv[1]))[sys.argv[3]]
if not path.resolve().is_relative_to((root / '.tools').resolve()) or not path.is_file():
    raise SystemExit('Installation is missing or moved; rerun setup.sh install.')
print(path)
PY
    }
    venv_python="$(installed_path python)"
    node_bin="$(installed_path node)"
fi
export PATH="$(dirname "$node_bin"):$PATH"
npm_cli="$(dirname "$(dirname "$node_bin")")/lib/node_modules/npm/bin/npm-cli.js"
if [ "$action" = install ]; then
    (cd "$frontend" && "$node_bin" "$npm_cli" ci --include=dev --include=optional --engine-strict --no-audit --no-fund && "$node_bin" "$npm_cli" run build)
fi

if [ "$action" != start ]; then
    "$uv_bin" pip check --python "$venv_python" --offline
    "$node_bin" --version
    "$node_bin" "$npm_cli" --version
    [ -f "$frontend/dist/index.html" ] && [ -f "$frontend/node_modules/vite/bin/vite.js" ] || { echo 'Frontend build is missing; rerun install.' >&2; exit 1; }
    "$venv_python" - "$backend" <<'PY'
import os, sys, tempfile
with tempfile.TemporaryDirectory() as temporary:
    os.chdir(temporary)
    sys.path.insert(0, sys.argv[1])
    import sqlite3, cv2, open3d
    from app.main import app
    from app.services.adapter_factory import AdapterFactory
    assert app.routes and AdapterFactory.discover_adapters()
print('Backend dependencies and frontend build are ready.')
PY
    if [ "$action" = install ]; then
        "$managed_python" - "$state" "$module_dir" "$venv_python" "$node_bin" <<'PY'
import json, pathlib, sys
state, root, python, node = map(pathlib.Path, sys.argv[1:])
temporary = state.with_suffix('.tmp')
temporary.write_text(json.dumps({'schema': 1, 'python': str(python.relative_to(root)), 'node': str(node.relative_to(root))}, indent=2) + '\n')
temporary.replace(state)
PY
        printf '\nInstallation complete. Start the app with:\n  bash "%s/setup.sh" start\n' "$module_dir"
    fi
    exit 0
fi

"$venv_python" - "$backend_port" "$frontend_port" <<'PY'
import socket, sys
ports = list(map(int, sys.argv[1:]))
if len(set(ports)) != 2:
    raise SystemExit('Backend and frontend ports must be different.')
for port in ports:
    with socket.socket() as sock:
        try:
            sock.bind(('127.0.0.1', port))
        except OSError:
            raise SystemExit(f'Port {port} is occupied; choose another port.')
PY
export MFI_DAA_BACKEND_URL="http://127.0.0.1:$backend_port"
(cd "$backend" && exec "$venv_python" -m uvicorn app.main:app --host 127.0.0.1 --port "$backend_port") &
backend_pid=$!
(cd "$frontend" && exec "$node_bin" node_modules/vite/bin/vite.js preview --host 127.0.0.1 --port "$frontend_port" --strictPort) &
frontend_pid=$!
cleanup() {
    kill "$backend_pid" "$frontend_pid" 2>/dev/null || true
    for process in "$backend_pid" "$frontend_pid"; do
        for attempt in 1 2 3 4 5; do
            kill -0 "$process" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$process" 2>/dev/null; then kill -9 "$process" 2>/dev/null || true; fi
        wait "$process" 2>/dev/null || true
    done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '\nUI: http://127.0.0.1:%s\nAPI: http://127.0.0.1:%s/docs\nPress Ctrl+C to stop both.\n' "$frontend_port" "$backend_port"
while kill -0 "$backend_pid" 2>/dev/null && kill -0 "$frontend_pid" 2>/dev/null; do sleep 1; done
echo 'A server stopped; shutting down the app.' >&2
exit 1
