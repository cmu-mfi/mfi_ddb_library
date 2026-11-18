@echo off
setlocal
rem Self-contained installer: Windows' built-in PowerShell runs the payload below.
set "MFI_DAA_SETUP_FILE=%~f0"
set "MFI_DAA_SETUP_ARGS=%*"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$text=[IO.File]::ReadAllText($env:MFI_DAA_SETUP_FILE); $marker='# POWERSHELL'+' BEGIN'; & ([ScriptBlock]::Create($text.Substring($text.IndexOf($marker)+$marker.Length)))"
set "setupExit=%errorlevel%"
if "%~1"=="" pause
exit /b %setupExit%
# POWERSHELL BEGIN
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$moduleDir = Split-Path -Parent $env:MFI_DAA_SETUP_FILE
$toolsDir = Join-Path $moduleDir '.tools'
$backend = Join-Path $moduleDir 'data_adapter_app\backend'
$frontend = Join-Path $moduleDir 'data_adapter_app\frontend'
$stateFile = Join-Path $toolsDir 'install.json'
$setupArgs = @($env:MFI_DAA_SETUP_ARGS -split '\s+' | Where-Object { $_ })
$action = 'install'
if ($setupArgs.Count -gt 0 -and !$setupArgs[0].StartsWith('-')) {
    $action = $setupArgs[0]
    $setupArgs = @($setupArgs | Select-Object -Skip 1)
}
$skipPackages = $false
$backendPort = 8000
$frontendPort = 3000

function Run([string]$command, [string[]]$commandArgs) {
    & $command @commandArgs
    if ($LASTEXITCODE -ne 0) { throw "Command failed (exit $LASTEXITCODE): $command" }
}
function Download([string]$url, [string]$destination) {
    Invoke-WebRequest $url -UseBasicParsing -OutFile $destination
}

try {
    for ($index = 0; $index -lt $setupArgs.Count; $index++) {
        switch ($setupArgs[$index]) {
            { $_ -in @('--help', '-h') } {
                Write-Host 'Usage: setup.bat [install|start|check] [options]'
                Write-Host '  install (default): install prerequisites/dependencies and build the UI.'
                Write-Host '  start: run the installed API and UI; Ctrl+C stops both.'
                Write-Host '  check: verify the installed environment without downloads.'
                Write-Host '  Options: --no-system-packages, --backend-port PORT, --frontend-port PORT'
                exit 0
            }
            '--no-system-packages' { $skipPackages = $true }
            { $_ -in @('--backend-port', '--frontend-port') } {
                $option = $_
                $index++
                if ($index -ge $setupArgs.Count -or $setupArgs[$index] -notmatch '^\d{1,5}$') { throw "Missing or invalid port after $option" }
                $number = [int]$setupArgs[$index]
                if ($number -lt 1 -or $number -gt 65535) { throw 'Ports must be between 1 and 65535.' }
                if ($option -eq '--backend-port') { $backendPort = $number } else { $frontendPort = $number }
            }
            default { throw "Unknown option: $($setupArgs[$index])" }
        }
    }
    if ($action -notin @('install', 'start', 'check')) { throw 'Use setup.bat [install|start|check].' }
    if ($env:OS -ne 'Windows_NT' -or [IntPtr]::Size -ne 8 -or $env:PROCESSOR_ARCHITECTURE -ne 'AMD64') {
        throw 'Use x86-64 Windows 10/11 with 64-bit PowerShell. Use setup.sh on Linux/macOS.'
    }
    $ProgressPreference = 'SilentlyContinue'
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    $env:UV_PYTHON_INSTALL_DIR = Join-Path $toolsDir 'python'
    $env:UV_CACHE_DIR = Join-Path $toolsDir 'cache\uv'
    $env:UV_NO_CONFIG = '1'
    $env:npm_config_cache = Join-Path $toolsDir 'cache\npm'
    $env:VITE_API_URL = '/api'
    $uv = Join-Path $toolsDir 'uv\uv.exe'

    if ($action -eq 'install') {
        New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null
        $runtime = Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64' -ErrorAction SilentlyContinue
        if (!$runtime -or $runtime.Installed -ne 1 -or [version]($runtime.Version -replace '^v', '') -lt [version]'14.40') {
            if ($skipPackages) { throw 'Install Microsoft Visual C++ v14 Redistributable (x64), then retry.' }
            $redist = Join-Path $toolsDir 'vc_redist.x64.exe'
            Download 'https://aka.ms/vc14/vc_redist.x64.exe' $redist
            $signature = Get-AuthenticodeSignature $redist
            if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notmatch 'O=Microsoft Corporation') {
                throw 'The Microsoft C++ runtime signature could not be verified.'
            }
            Write-Host 'Installing Microsoft C++ runtime (Windows may request administrator access)...'
            $process = Start-Process $redist -ArgumentList '/install', '/quiet', '/norestart' -Verb RunAs -Wait -PassThru
            Remove-Item $redist -Force
            if ($process.ExitCode -eq 3010) { throw 'Restart Windows to finish the C++ runtime installation, then rerun setup.' }
            if ($process.ExitCode -notin @(0, 1638)) { throw "C++ runtime installation failed (exit $($process.ExitCode))." }
        }
        $installer = Join-Path $toolsDir 'uv-install.ps1'
        Download 'https://astral.sh/uv/install.ps1' $installer
        $env:UV_UNMANAGED_INSTALL = Join-Path $toolsDir 'uv'
        Run 'powershell.exe' @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $installer)
        Remove-Item $installer -Force
        Run $uv @('python', 'install', '3.12', '--upgrade', '--no-bin', '--no-registry')
    }
    if (!(Test-Path $uv)) { throw 'Run setup.bat install first.' }
    $managedPython = & $uv python find --managed-python --no-python-downloads 3.12
    if ($LASTEXITCODE -ne 0) { throw 'Managed Python is missing; rerun setup.bat install.' }

    if ($action -eq 'install') {
        $releases = Invoke-RestMethod 'https://nodejs.org/dist/index.json'
        $release = $releases |
            Where-Object { $_.lts -and $_.version -match '^v\d+\.\d+\.\d+$' } |
            Sort-Object { [version]$_.version.Substring(1) } -Descending | Select-Object -First 1
        if (!$release) { throw 'No stable Node LTS release was found.' }
        $nodeRoot = 'node-' + $release.version + '-win-x64'
        $nodeDir = Join-Path $toolsDir 'node'
        $node = Join-Path $nodeDir ($nodeRoot + '\node.exe')
        if (!(Test-Path $node) -or !(Test-Path (Join-Path $nodeDir ($nodeRoot + '\node_modules\npm\bin\npm-cli.js')))) {
            $archiveName = $nodeRoot + '.zip'
            $archive = Join-Path $toolsDir $archiveName
            $baseUrl = 'https://nodejs.org/dist/' + $release.version + '/'
            Download ($baseUrl + $archiveName) $archive
            $sums = (Invoke-WebRequest ($baseUrl + 'SHASUMS256.txt') -UseBasicParsing).Content
            $line = @($sums -split '\r?\n' | Where-Object { $_ -match ('\s+\*?' + [regex]::Escape($archiveName) + '$') })
            if ($line.Count -ne 1 -or ($line[0] -split '\s+')[0] -notmatch '^[0-9a-fA-F]{64}$') { throw 'Node checksum is missing.' }
            $expected = ($line[0] -split '\s+')[0]
            if ((Get-FileHash $archive -Algorithm SHA256).Hash -ne $expected) { throw 'Node checksum mismatch; rerun setup.' }
            New-Item -ItemType Directory -Force -Path $nodeDir | Out-Null
            Expand-Archive -LiteralPath $archive -DestinationPath $nodeDir -Force
        }
        $actual = & $node --version
        if ($LASTEXITCODE -ne 0 -or $actual -ne $release.version) { throw 'Damaged Node runtime; move its .tools\node folder aside and rerun.' }
        $pythonVersion = & $managedPython -c 'import platform; print(platform.python_version())'
        if ($LASTEXITCODE -ne 0) { throw 'Could not determine the Python version.' }
        $venvDir = Join-Path $toolsDir ('venvs\' + $pythonVersion)
        $python = Join-Path $venvDir 'Scripts\python.exe'
        Remove-Item $stateFile -Force -ErrorAction SilentlyContinue
        Run $uv @('venv', '--allow-existing', '--python', $managedPython, $venvDir)
        Run $uv @('pip', 'install', '--python', $python, '--only-binary',
            'numpy,pandas,open3d,opencv-python-headless,grpcio,grpcio-tools,pydantic-core,scipy,scikit-learn',
            '--requirements', (Join-Path $backend 'requirements.txt'))
    } else {
        if (!(Test-Path $stateFile)) { throw 'Run setup.bat install first.' }
        $state = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json
        $python = [IO.Path]::GetFullPath((Join-Path $moduleDir $state.python))
        $node = [IO.Path]::GetFullPath((Join-Path $moduleDir $state.node))
        foreach ($file in @($python, $node)) {
            if (!$file.StartsWith($toolsDir + '\', [StringComparison]::OrdinalIgnoreCase) -or !(Test-Path $file)) {
                throw 'Installation is missing or moved; rerun setup.bat install.'
            }
        }
    }
    $env:PATH = (Split-Path -Parent $node) + ';' + $env:PATH
    $npmCli = Join-Path (Split-Path -Parent $node) 'node_modules\npm\bin\npm-cli.js'
    if ($action -eq 'install') {
        Push-Location $frontend
        try {
            Run $node @($npmCli, 'ci', '--include=dev', '--include=optional', '--engine-strict', '--no-audit', '--no-fund')
            Run $node @($npmCli, 'run', 'build')
        } finally { Pop-Location }
    }
    if ($action -ne 'start') {
        Run $uv @('pip', 'check', '--python', $python, '--offline')
        Run $node @('--version')
        Run $node @($npmCli, '--version')
        if (!(Test-Path (Join-Path $frontend 'dist\index.html')) -or !(Test-Path (Join-Path $frontend 'node_modules\vite\bin\vite.js'))) {
            throw 'The frontend build is missing; rerun setup.bat install.'
        }
        $temporary = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
        New-Item -ItemType Directory -Path $temporary | Out-Null
        Push-Location $temporary
        try {
            Run $python @('-c', 'import sys, sqlite3, cv2, open3d; sys.path.insert(0, sys.argv[1]); from app.main import app; from app.services.adapter_factory import AdapterFactory; assert app.routes and AdapterFactory.discover_adapters(); print(''Backend dependencies and frontend build are ready.'')', $backend)
        } finally { Pop-Location; Remove-Item $temporary -Recurse -Force }
        if ($action -eq 'install') {
            $state = @{schema=1; python=$python.Substring($moduleDir.Length + 1); node=$node.Substring($moduleDir.Length + 1)}
            $temporary = $stateFile + '.tmp'
            [IO.File]::WriteAllText($temporary, ($state | ConvertTo-Json))
            Move-Item $temporary $stateFile -Force
            Write-Host "`nInstallation complete. Start the app with:`n  setup.bat start"
        }
        exit 0
    }

    if ($backendPort -eq $frontendPort) { throw 'Backend and frontend ports must be different.' }
    foreach ($port in @($backendPort, $frontendPort)) {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
        try { $listener.Start() } catch { throw "Port $port is occupied; choose another port." } finally { $listener.Stop() }
    }
    $env:MFI_DAA_BACKEND_URL = "http://127.0.0.1:$backendPort"
    $processes = @()
    try {
        $processes += Start-Process -FilePath $python -WorkingDirectory $backend -NoNewWindow -PassThru -ArgumentList @('-m','uvicorn','app.main:app','--host','127.0.0.1','--port',"$backendPort")
        $vite = '"' + (Join-Path $frontend 'node_modules\vite\bin\vite.js') + '"'
        $processes += Start-Process -FilePath $node -WorkingDirectory $frontend -NoNewWindow -PassThru -ArgumentList @($vite,'preview','--host','127.0.0.1','--port',"$frontendPort",'--strictPort')
        Write-Host "`nUI: http://127.0.0.1:$frontendPort`nAPI: http://127.0.0.1:$backendPort/docs`nPress Ctrl+C to stop both."
        while ($true) {
            foreach ($process in $processes) { if ($process.HasExited) { throw 'A server stopped; shutting down the app.' } }
            Start-Sleep -Milliseconds 500
        }
    } finally {
        foreach ($process in $processes) {
            if (!$process.HasExited) { & taskkill.exe /PID $process.Id /T /F | Out-Null }
        }
    }
} catch {
    [Console]::Error.WriteLine("Setup failed: $($_.Exception.Message)")
    exit 1
}
