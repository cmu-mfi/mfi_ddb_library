import React, { useState, useEffect, useRef } from 'react';
import { hostPlatform } from '../../services/api';

export default function Step3Monitor({ selectedServices, formValues, prevStep, onFinished }) {
  const [logs, setLogs] = useState([]);
  const [isDeploying, setIsDeploying] = useState(false);
  const [deploySuccess, setDeploySuccess] = useState(null);
  const terminalEndRef = useRef(null);

  // Auto-scroll anchor system
  useEffect(() => {
    terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

const triggerDeploymentPipeline = () => {
    setIsDeploying(true);
    setDeploySuccess(null);
    
    setLogs([
      "[SYSTEM] Orchestration sequence armed...",
      "[INFO] Filtering user selected network topology configurations...",
    ]);

    const selectedList = Object.keys(selectedServices).filter(key => selectedServices[key]);
    setLogs(prev => [...prev, `[INFO] Modules targeted for compilation: [${selectedList.join(', ')}]`]);
    setLogs(prev => [...prev, "[INFO] Connecting stream pipelines directly to daemon sockets..."]);

    const pipelinePayload = {
      selectedServices: selectedList,
      configs: formValues
    };

    // Invoke our cross-platform streaming engine client
    const closeActiveStream = hostPlatform.streamPipelineLogs(
      pipelinePayload,
      
      // Callback 1: Smart Log Chunk Appended
      (incomingLine) => {
        // Check if the incoming string has a Carriage Return (\r)
        if (incomingLine.includes('\r')) {
          // Clean up the text by extracting the very last string segment after the last \r
          const parts = incomingLine.split('\r');
          const cleanText = parts[parts.length - 1].trim();

          if (cleanText) {
            setLogs((prev) => {
              // Create a shallow copy of the logs history array
              const newLogs = [...prev];
              // Overwrite the last item in the log list with the new live progress status
              if (newLogs.length > 0) {
                newLogs[newLogs.length - 1] = cleanText;
              } else {
                newLogs.push(cleanText);
              }
              return newLogs;
            });
          }
        } else {
          // Standard line output (Newlines) -> Simply append to history normally
          setLogs((prev) => [...prev, incomingLine.trim()]);
        }
      },
      
      // Callback 2: Compilation Complete Hook
      () => {
        setIsDeploying(false);
        setDeploySuccess(true);
        setLogs((prev) => [...prev, "\n[SUCCESS] Docker Compose stack updated flawlessly! All services operational."]);
      },
      
      // Callback 3: Crash Hook
      (errorMsg) => {
        setIsDeploying(false);
        setDeploySuccess(false);
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
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Orchestration Progress Monitor</h2>
        <p className="text-base text-neutral-500 mt-1">Real-time compilation loop working directly against the host machine container environment layers.</p>
      </div>

      {/* 2. BODY AREA (SCROLLABLE LOG TERMINAL) */}
      <div className="flex-1 overflow-y-auto pr-2 min-h-0 custom-scrollbar py-2">
        <div className="w-full bg-neutral-950 border border-neutral-800 rounded-xl p-5 font-mono text-xs space-y-2 h-full min-h-[350px] max-h-[450px] overflow-y-auto shadow-inner select-text">
          {logs.map((line, index) => {
            let colorClass = "text-sky-400";
            
            if (line.startsWith("[SUCCESS]")) colorClass = "text-emerald-400 font-semibold";
            else if (line.startsWith("[CRITICAL ERROR]") || line.startsWith("[ERROR]") || line.toLowerCase().includes("error response from daemon")) colorClass = "text-rose-500 font-semibold";
            else if (line.startsWith("[SYSTEM]")) colorClass = "text-neutral-500";
            else if (line.startsWith("[INFO]")) colorClass = "text-amber-400/90";
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
            className="px-6 py-2.5 cursor-pointer bg-emerald-600 hover:bg-emerald-700 text-white font-bold text-sm rounded-lg shadow-sm transition transform active:scale-95"
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