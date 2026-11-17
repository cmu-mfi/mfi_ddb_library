# Data Adapter App

The [Data Adapter Application](data_adapter_app/README.md) is a FastAPI backend and React/Vite frontend for configuring and monitoring `mfi_ddb` adapters.

## Install and build from source

Download and extract this repository, or clone it if you already have Git. From the repository root, run the command for your OS. Python, Node.js, npm, uv, and Docker do **not** need to be installed beforehand.

**Linux / macOS:**

```bash
bash mfi_ddb_data_adapter/setup.sh
```

**Windows**, from Command Prompt or PowerShell (or double-click `setup.bat`):

```bat
.\mfi_ddb_data_adapter\setup.bat
```

Both scripts are self-contained; there are no separate installer helpers or test scripts. The batch file uses Windows' built-in PowerShell without changing its execution policy. Both entry points work from paths containing spaces and locate the application relative to the script, independently of the working directory.

The installer:

1. Checks the OS and CPU architecture and installs missing OS runtime packages where supported.
2. Downloads the latest **uv** into `.tools/uv`, without changing your shell profile, global PATH, or default Python/Node.
3. Uses uv to install the latest available **Python 3.12 patch**, then creates an isolated environment in `.tools/venvs/<python-version>`.
4. Reads the official Node release index, selects the newest stable **LTS** release, downloads Node and bundled npm for your platform, and verifies the archive against its published SHA-256 checksum before extracting it into `.tools/node`.
5. Installs `data_adapter_app/backend/requirements.txt`, including `mfi_ddb` from PyPI and all its transitive dependencies, with the compatibility constraint embedded in the installer. Native numerical/vision packages use binary wheels rather than requiring a local compiler.
6. Runs `npm ci --include=dev --include=optional --engine-strict` against the committed frontend lockfile, then `npm run build` to create `frontend/dist`.
7. Checks Python dependency consistency, SQLite, OpenCV/Open3D imports, backend imports, adapter discovery, Node/npm, and the frontend build. It records a successful installation only after these checks pass.

**Why Python 3.12?** Python has no LTS release channel. The current `mfi_ddb` package declares `requires-python = ">=3.8,<3.13"`; installing the newest Python release would violate that requirement. The installer deliberately uses the latest compatible 3.12 patch known to the freshly downloaded uv release. Node uses its actual LTS channel. See [uv's Python documentation](https://docs.astral.sh/uv/guides/install-python/) and [Node's release policy](https://nodejs.org/en/about/previous-releases).

The script installs only this application and its dependencies. It does not install the setup wizard, database nodes, retrieval API, or Docker.

On macOS the installer excludes Open3D 0.20.0 because its wheel links to an external Homebrew `libusb` rather than bundling it ([upstream issue](https://github.com/isl-org/Open3D/issues/7586)). The compatible 0.19 wheel avoids adding Homebrew/Xcode as application prerequisites.

## Run and verify

**Linux / macOS:**

```bash
bash mfi_ddb_data_adapter/setup.sh start
# Later, verify the existing installation without downloading anything:
bash mfi_ddb_data_adapter/setup.sh check
```

**Windows:**

```bat
.\mfi_ddb_data_adapter\setup.bat start
.\mfi_ddb_data_adapter\setup.bat check
```

Open **http://127.0.0.1:3000** for the UI or **http://127.0.0.1:8000/docs** for the API. `start` runs Uvicorn and Vite's preview server for the built UI, forwards `/api` to the backend, and stops both when you press **Ctrl+C**. If one server fails, the other is stopped too. Vite preview is for local use; deployment as a production service needs a production static server/reverse proxy and service configuration.

Occupied ports produce an error. Choose alternatives with `start --backend-port 8001 --frontend-port 3001` on either OS; the frontend proxy follows the chosen backend port automatically. Both servers bind to `127.0.0.1`.

The backend runs from `data_adapter_app/backend`, so its connection database defaults to `backend/data/connections.db` and its logs to `backend/logs`. Set `MFI_DAA_DB_PATH` before `start` to use a different database. Installation checks do not restore saved connections or connect to devices.

## Supported systems and prerequisites

| OS | Architecture and baseline | OS packages handled automatically |
| --- | --- | --- |
| Linux | x86-64 or ARM64, glibc >= 2.31, kernel >= 4.18; use a supported distro such as Ubuntu 22.04+ or Debian 12+ | CA certificates, curl (or existing wget), tar/gzip if missing; OpenGL/EGL, OpenMP, GLib, C++ and atomic runtimes if missing. Supports apt, dnf, zypper and pacman. |
| macOS | Intel or Apple Silicon, macOS 13.5+ | Uses the OS's Bash, curl, tar/gzip and system libraries. Homebrew and Xcode are not needed for the supplied binary dependencies. |
| Windows | x86-64 Windows 10/11 or Server 2016+, 64-bit Windows PowerShell 5.1+ | Downloads and verifies Microsoft's signed Visual C++ v14 x64 Redistributable if a sufficiently recent runtime is missing. |

Linux may request your sudo password **only** for missing OS packages; Windows may show UAC for the C++ runtime. Do not run the entire setup as root/administrator. To prohibit OS package installation, pass `install --no-system-packages`; missing prerequisites then produce an error. On systems with no supported package manager, install the named missing libraries using your OS's package manager and rerun.

32-bit systems, Windows ARM64 and Alpine/musl are not supported by this dependency set. Node's platform requirements and availability of Open3D wheels constrain the supported systems; see [Node's platform matrix](https://github.com/nodejs/node/blob/v26.x/BUILDING.md#platform-list) and [Open3D installation](https://www.open3d.org/docs/release/getting_started.html).

Internet access is required for installation: the OS package repositories, `astral.sh`, GitHub release downloads, `nodejs.org`, PyPI/package files, the npm registry, and (Windows) Microsoft downloads. Allow several GB of free disk space for Open3D and the other dependencies, environments, downloads and caches. A normal system CA trust store is required; HTTPS verification is never disabled.

MQTT streaming needs a reachable broker configured in the app. ROS/ROS2 adapters additionally need the appropriate external ROS environment and compatible Python bindings. Those device/protocol services are not prerequisites for building or opening the app and are not provisioned by this installer. It does not install a broker or a ROS distribution.

## Reruns and troubleshooting

- Rerun the install command after an interrupted download or dependency failure. It refreshes uv, selects the current compatible Python patch and Node LTS, reuses complete local runtime downloads, reinstalls frontend dependencies from the lockfile, and rebuilds. Existing Python packages that still satisfy the requirements can be reused.
- Python environments, runtime versions and caches are kept inside the ignored `.tools` directory. Existing system runtimes and a developer's `backend/.venv` are left alone. Successful older runtime versions may remain in `.tools` after an update.
- If you move the checkout or copy it to another OS, rerun setup; Python environments are not portable. For a clean reinstall, stop the app and remove this module's `.tools` directory and `frontend/node_modules`, then run setup again. The connection database and logs are outside `.tools`.
- A checksum mismatch, unsupported platform, missing native wheel, failed dependency install, or failed build exits nonzero. There is no fallback to an unsupported Python version or unverified Node archive. Dependency errors include the failing command.
- If Windows reports that a restart is required after the C++ runtime install, restart and rerun setup.
- `setup.sh --help` / `setup.bat --help` works without downloading prerequisites.
