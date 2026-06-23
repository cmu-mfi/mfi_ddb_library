import React, { useState, useEffect, useRef } from 'react';
import { hostPlatform } from '../../services/api';

export default function Step3Monitor({ selectedServices, formValues, prevStep, onFinished }) {
  const [logs, setLogs] = useState([]);
  const [isDeploying, setIsDeploying] = useState(false);
  const [deploySuccess, setDeploySuccess] = useState(null);
  
  // Progress states for visual orchestration feedback
  const [progress, setProgress] = useState(0);
  const [currentPhase, setCurrentPhase] = useState("Initializing...");
  
  const terminalEndRef = useRef(null);

  // Auto-scroll anchor system
  useEffect(() => {
    terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const triggerDeploymentPipeline = () => {
    setIsDeploying(true);
    setDeploySuccess(null);
    setProgress(5);
    setCurrentPhase("Arming orchestration sequence...");
    
    setLogs([
      "[SYSTEM] Orchestration sequence armed...",
      "[INFO] Filtering user selected network topology configurations...",
    ]);

    const selectedList = Object.keys(selectedServices).filter(key => selectedServices[key]);
    setLogs(prev => [...prev, `[INFO] Modules targeted for compilation: [${selectedList.join(', ')}]`]);
    setLogs(prev => [...prev, "[INFO] Connecting stream pipelines directly to daemon sockets..."]);

    // Create an object to remember which index in our logs array belongs to which Docker Layer ID
    const layerIndices = {};

    const pipelinePayload = {
      selectedServices: selectedList,
      configs: formValues
    };

    const closeActiveStream = hostPlatform.streamPipelineLogs(
      pipelinePayload,
      
      // Callback 1: Smart Log Line Appended
      (incomingLine) => {
        const line = incomingLine.trim();
        if (!line) return;

        // Regular expression matching Docker layer updates: e.g., "c30732a3612c Downloading..." or "0c7392fd90b8 Extracting..."
        const dockerLayerMatch = line.match(/^([a-f0-9]{12})\s+(Downloading|Extracting|Waiting|Download complete|Pull complete|Already exists)/i);

        if (dockerLayerMatch) {
          const layerId = dockerLayerMatch[1]; // e.g. "c30732a3612c"
          const status = dockerLayerMatch[2];
          
          // Phase tracking based on active layer download states
          if (status === "Downloading") {
            setCurrentPhase("Downloading container image layers...");
            setProgress((prev) => Math.max(prev, 35));
          } else if (status === "Extracting") {
            setCurrentPhase("Extracting layer file systems...");
            setProgress((prev) => Math.max(prev, 65));
          }

          setLogs((prev) => {
            const newLogs = [...prev];
            
            if (layerId in layerIndices) {
              // We've seen this layer before! Overwrite its previous line to prevent flooding
              const targetIndex = layerIndices[layerId];
              newLogs[targetIndex] = line;
            } else {
              // First time seeing this layer. Push it and remember its index position
              newLogs.push(line);
              layerIndices[layerId] = newLogs.length - 1;
            }
            return newLogs;
          });
        } else {
          // Check for structural milestones to push progress metrics smoothly forward
          if (line.toLowerCase().includes("pulling")) {
            setCurrentPhase("Contacting Docker Hub registry repositories...");
            setProgress(20);
          } else if (line.toLowerCase().includes("creating network")) {
            setCurrentPhase("Configuring software virtual network switches...");
            setProgress(80);
          } else if (line.toLowerCase().includes("creating") && line.toLowerCase().includes("done")) {
            setCurrentPhase("Spawning system container environments...");
            setProgress(90);
          }

          // Standard system output (e.g., "Creating network", "Container starting")
          setLogs((prev) => [...prev, line]);
        }
      },
      
      // Callback 2: Compilation Complete Hook
      () => {
        setIsDeploying(false);
        setDeploySuccess(true);
        setProgress(100);
        setCurrentPhase("DDB Pipeline Operational!");
        setLogs((prev) => [...prev, "\n[SUCCESS] Docker Compose stack updated flawlessly! All services operational."]);
      },
      
      // Callback 3: Crash Hook
      (errorMsg) => {
        setIsDeploying(false);
        setDeploySuccess(false);
        setCurrentPhase("Deployment failed.");
        setLogs((prev) => [
          ...prev, 
          `[CRITICAL ERROR] Host container runtime deployment failure.`,
          `[ERROR] Reason: ${errorMsg}`
        ]);
      }
    );

    return closeActiveStream;
  };

  useEffect(() => {
    const streamCleanup = triggerDeploymentPipeline();
    return () => {
      if (streamCleanup) streamCleanup();
    };
  }, []);

  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between h-full animate-fade">
      
      {/* 1. TITLE CONTAINER */}
      <div className="flex-none pb-2">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Orchestration Progress Monitor</h2>
        <p className="text-base text-neutral-500 mt-1">Real-time compilation loop working directly against the host machine container environment layers.</p>
      </div>

      {/* DYNAMIC PROGRESS BAR ACCORDION WRAPPER */}
      <div className="flex items-center justify-center gap-3 my-1 bg-neutral-50 border border-neutral-200 px-2 py-1 rounded shadow-sm">

        <div className="w-full bg-neutral-200 h-2.5 rounded-full overflow-hidden shadow-inner">
          <div 
            className={`h-full transition-all duration-500 ease-out ${
              deploySuccess === false ? 'bg-rose-500' :
              progress === 100 ? 'bg-emerald-500' : 'bg-cmu-red'
            }`}
            style={{ width: `${progress}%` }}
          />
        </div>

        <div className="flex text-xs font-semibold text-neutral-700 font-sans">
          <span className="text-sm font-mono text-neutral-900"> {progress}%</span>
        </div>

      </div>

      {/* 2. BODY AREA (SCROLLABLE LOG TERMINAL) */}
      <div className="flex-1 overflow-y-auto  min-h-0 custom-scrollbar py-1">
        <div className="w-full bg-neutral-950  p-5 font-mono text-xs space-y-2 h-full min-h-[300px] max-h-[420px] overflow-y-auto shadow-inner select-text">
          {logs.map((line, index) => {
            let colorClass = "text-sky-400";
            
            if (line.includes("[SUCCESS]")) colorClass = "text-emerald-400 font-semibold";
            else if (line.includes("[CRITICAL ERROR]") || line.includes("[ERROR]") || line.toLowerCase().includes("error response from daemon")) colorClass = "text-rose-500 font-semibold";
            else if (line.includes("[SYSTEM]")) colorClass = "text-neutral-500";
            else if (line.includes("[INFO]")) colorClass = "text-amber-400/90";
            else if (line.includes("Pulling") || line.includes("Extracting") || line.includes("Downloading")) colorClass = "text-neutral-500 animate-pulse";
            
            return <div key={index} className={`whitespace-pre-wrap leading-relaxed ${colorClass}`}>{line}</div>;
          })}
          
          {isDeploying && (
            <div className="flex items-center gap-2 text-amber-500 animate-pulse mt-3 pt-2 border-t border-neutral-900">
              <span className="inline-block animate-spin text-sm">⏳</span>
              <span>Host engine compiling active stack profiles... Please hold...</span>
            </div>
          )}
          
          <div ref={terminalEndRef} />
        </div>
      </div>

      {/* 3. BUTTONS ROW */}
      <div className="flex-none flex justify-between pt-4 border-t border-neutral-200 mt-2 bg-white">
        <button 
          onClick={prevStep}
          disabled={isDeploying}
          className="px-5 py-2.5 cursor-pointer bg-neutral-100 hover:bg-neutral-200 disabled:opacity-40 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition shadow-sm"
        >
          ← Adjust Configurations
        </button>

        {deploySuccess === true && (
          <button
            onClick={onFinished}
            className="px-6 py-2.5 cursor-pointer bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-sm rounded-lg shadow-sm transition transform active:scale-95 animate-fade"
          >
            Continue to Dashboard →
          </button>
        )}
        
        {deploySuccess === false && (
          <button 
            onClick={triggerDeploymentPipeline} 
            className="px-5 py-2.5 cursor-pointer bg-cmu-red hover:bg-cmu-red-hover text-white font-medium text-sm rounded-lg shadow-sm transition"
          >
            Retry Deployment ↻
          </button>
        )}
      </div>
    </div>
  );
}