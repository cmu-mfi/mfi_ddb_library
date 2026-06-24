# uvicorn main:app --reload --port 8000
import asyncio
import os
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pathlib import Path

from schemas import MasterConfigPayload
from generators import write_runtime_configs, generate_master_compose

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
async def deploy_pipeline(payload: MasterConfigPayload):
    """
    Step 1: Staging Configurations.
    Assembles configuration files and writes out the docker-compose file.
    Returns IMMEDIATELY to flip the frontend to the progress screen.
    """
    try:
        # 1. Initialize workspaces
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        
        # 2. Write out configuration parameters (.yaml and .ini files)
        write_runtime_configs(CONFIG_DIR, payload)
        
        # 3. Generate the master docker-compose configuration
        generate_master_compose(RUNTIME_DIR, payload)
        
        # --- BLOCKING STEP 4 REMOVED ---
        # The actual container pull/spin-up execution is handed off entirely 
        # to the /api/deploy/stream SSE endpoint below.

        return {
            "status": "success",
            "message": "DDB custom configurations staged successfully. Handing off execution loop to stream.",
            "workspace": str(RUNTIME_DIR)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pipeline assembly configuration error: {str(e)}"
        )
    
@app.get("/api/deploy/stream")
async def stream_deployment_logs(services: str = ""): # Read the incoming query line
    """
    Step 2: Real-time Orchestration Loop.
    Instructs Docker to boot specific configuration profiles requested by Step 1.
    """
    async def generate_logs():
        compose_str_path = str(COMPOSE_FILE_PATH)

        if not os.path.exists(compose_str_path):
            yield f"data: [ERROR] Layout manifest not found at {compose_str_path}. Staging must be run first.\n\n"
            return

        # Parse out active layout selections. (e.g. services="infra,kv")
        active_profiles = [s.strip() for s in services.split(",") if s.strip()]

        # Build command dynamically including active compose profiles
        cmd = ["docker", "compose", "-f", compose_str_path]
        
        for profile in active_profiles:
            cmd.extend(["--profile", profile])
            
        cmd.extend(["up", "-d"])
        
        yield f"data: [SYSTEM] Orchestrating profiles: {active_profiles}\n\n"
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )

            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8").strip()
                if text:
                    yield f"data: {text}\n\n"
                
            await process.wait()

            if process.returncode == 0:
                yield "data: [DEPLOYMENT_COMPLETE]\n\n"
            else:
                yield f"data: [ERROR] Docker engine returned abnormal code termination: {process.returncode}\n\n"
        except Exception as e:
            yield f"data: [ERROR] Runtime exception executing subprocess loop: {str(e)}\n\n"

    return StreamingResponse(generate_logs(), media_type="text/event-stream")