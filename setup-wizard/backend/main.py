# uvicorn main:app --reload --port 8000
import asyncio
import os
import json

import yaml
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pathlib import Path
import uvicorn

from schemas import MasterConfigPayload
from generators import write_runtime_configs, generate_master_compose, validate_existing_databases

app = FastAPI(title="CMU MFI Generation Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cross-platform safe path initialization
RUNTIME_DIR = Path.home() / ".mfi_ddb_runtime"
CONFIG_DIR = RUNTIME_DIR / "runtime_configs"
COMPOSE_FILE_PATH = RUNTIME_DIR / "docker-compose.yaml"

@app.post("/api/deploy", status_code=status.HTTP_201_CREATED)
async def deploy_pipeline(payload: MasterConfigPayload, request: Request):
    """
    Step 1: Staging Configurations.
    Assembles configuration files and writes out the docker-compose file.
    Returns IMMEDIATELY to flip the frontend to the progress screen.
    """
    try:
        validate_existing_databases(RUNTIME_DIR, payload)

        # 1. Initialize workspaces
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        
        # 2. Write out configuration parameters (.yaml and .ini files)
        write_runtime_configs(CONFIG_DIR, payload)
        
        # 3. Generate the master docker-compose configuration
        generate_master_compose(RUNTIME_DIR, payload)

        dashboard_url = None
        compose = yaml.safe_load((RUNTIME_DIR / 'docker-compose.yaml').read_text())
        frontend = compose['services'].get('data-adapter-frontend')
        if frontend:
            # Use the published frontend port and the browser-accessible deployment host.
            binding = frontend['ports'][0].rsplit(':', 2)
            host = request.url.hostname
            if len(binding) == 3 and binding[0] not in ('0.0.0.0', '[::]', '::'):
                host = binding[0].strip('[]')
            if ':' in host:
                host = f'[{host}]'
            dashboard_url = f'http://{host}:{binding[-2]}/'
        
        # --- BLOCKING STEP 4 REMOVED ---
        # The actual container pull/spin-up execution is handed off entirely 
        # to the /api/deploy/stream SSE endpoint below.

        return {
            "status": "success",
            "message": "DDB custom configurations staged successfully. Handing off execution loop to stream.",
            "workspace": str(RUNTIME_DIR),
            "dashboard_url": dashboard_url,
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pipeline assembly configuration error: {str(e)}"
        )
    
def containers_ready(output: str, expected: set[str]) -> bool:
    """Compose versions emit either a JSON array or one JSON object per line."""
    try:
        try:
            rows = json.loads(output)
            if isinstance(rows, dict):
                rows = [rows]
        except json.JSONDecodeError:
            rows = [json.loads(line) for line in output.splitlines() if line.strip()]
        if not isinstance(rows, list) or not rows:
            return False
        active = {row['Service']: row for row in rows if row.get('Service') in expected}
        return set(active) == expected and all(
            row.get('State') == 'running' and row.get('Health', '') in ('', 'healthy')
            for row in active.values()
        )
    except (ValueError, TypeError, KeyError, AttributeError):
        return False


@app.get("/api/deploy/stream")
async def stream_deployment_logs(services: str = ""):
    async def generate_logs():
        compose_str_path = str(COMPOSE_FILE_PATH)
        if not os.path.exists(compose_str_path):
            yield f"data: [ERROR] Configuration must be staged before deployment.\n\n"
            return
        definition = yaml.safe_load(COMPOSE_FILE_PATH.read_text())
        expected = set(definition.get('services', {}))
        staged_profiles = {profile for service in definition['services'].values() for profile in service.get('profiles', [])}
        active_profiles = set(filter(None, (s.strip() for s in services.split(','))))
        if not expected or active_profiles != staged_profiles:
            yield "data: [ERROR] Service selection does not match the staged configuration. Submit the form again.\n\n"
            return
        cmd = ["docker", "compose", "-f", compose_str_path]
        for profile in sorted(active_profiles):
            cmd.extend(["--profile", profile])
        try:
            # Validate the exact generated file before touching containers.
            check = await asyncio.create_subprocess_exec(
                *cmd, 'config', '--quiet', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            output, _ = await check.communicate()
            if check.returncode:
                yield 'data: [ERROR] Invalid Compose configuration: ' + output.decode().replace('\n', ' ') + '\n\n'
                return
            yield "data: [SYSTEM] Starting selected services...\n\n"
            process = await asyncio.create_subprocess_exec(
                *cmd, 'up', '-d', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            while line := await process.stdout.readline():
                yield f"data: {line.decode().strip()}\n\n"
            await process.wait()
            if process.returncode:
                yield f"data: [ERROR] Docker Compose exited with code {process.returncode}.\n\n"
                return
            stable_checks = 0
            for _ in range(20):
                check = await asyncio.create_subprocess_exec(
                    *cmd, 'ps', '--all', '--format', 'json',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
                output, _ = await check.communicate()
                ready = check.returncode == 0 and containers_ready(output.decode(), expected)
                stable_checks = stable_checks + 1 if ready else 0
                if stable_checks >= 5:
                    yield "data: [SYSTEM] All selected containers are running and available health checks passed.\n\n"
                    yield "data: [DEPLOYMENT_COMPLETE]\n\n"
                    return
                yield "data: [SYSTEM] Waiting for all selected containers to remain ready...\n\n"
                await asyncio.sleep(1)
            yield "data: [ERROR] Selected containers did not reach a stable running state. Check Docker container logs.\n\n"
        except Exception as exc:
            yield 'data: [ERROR] Deployment failed: ' + str(exc).replace('\n', ' ') + '\n\n'
    return StreamingResponse(generate_logs(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(
        app, 
        host="127.0.0.1", 
        port=8000, 
        reload=False,
        workers=1
    )
