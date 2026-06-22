import React, { useState, useEffect } from 'react';
import { hostPlatform } from '../../services/api';

export default function Step3Monitor({ selectedServices, formValues, prevStep }) {
  const [logs, setLogs] = useState([]);
  const [isDeploying, setIsDeploying] = useState(false);
  const [deploySuccess, setDeploySuccess] = useState(null);

  const triggerDeploymentPipeline = async () => {
    setIsDeploying(true);
    setDeploySuccess(null);
    setLogs([
      "[SYSTEM] Orchestration sequence armed...",
      "[INFO] Filtering user selected network topology configurations...",
    ]);

    const selectedList = Object.keys(selectedServices).filter(key => selectedServices[key]);
    setLogs(prev => [...prev, `[INFO] Modules targeted for compilation: [${selectedList.join(', ')}]`]);

    try {
      const result = await hostPlatform.launchPipeline({
        selectedServices: selectedList,
        configs: formValues
      });

      if (result.success) {
        setDeploySuccess(true);
        setLogs(prev => [...prev, "[SUCCESS] Docker Compose stack updated flawlessly!", "----------------------------------", result.log]);
      } else {
        setDeploySuccess(false);
        setLogs(prev => [...prev, "[CRITICAL ERROR] Host container runtime deployment failure.", "----------------------------------", result.log]);
      }
    } catch (err) {
      setDeploySuccess(false);
      setLogs(prev => [...prev, `[CRITICAL UI BREAK] Failed to route data through daemon API: ${err.message}`]);
    } finally {
      setIsDeploying(false);
    }
  };

  useEffect(() => {
    triggerDeploymentPipeline();
  }, []);

  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between h-full animate-fade">
      
      {/* 1. TITLE CONTAINER (UNIFIED) */}
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Orchestration Progress Monitor</h2>
        <p className="text-base text-neutral-500 mt-1">Real-time compilation loop working directly against the host machine container environment layers.</p>
      </div>

      {/* 2. BODY AREA (SCROLLABLE) */}
      <div className="flex-1 overflow-y-auto pr-2 min-h-0 custom-scrollbar py-2">
        <div className="w-full bg-neutral-950 border border-neutral-800 rounded-xl p-5 font-mono text-xs space-y-2 h-full min-h-[300px] overflow-y-auto shadow-inner">
          {logs.map((line, index) => {
            let colorClass = "text-sky-400";
            if (line.startsWith("[SUCCESS]")) colorClass = "text-emerald-400 font-semibold";
            if (line.startsWith("[CRITICAL ERROR]") || line.startsWith("[ERROR]")) colorClass = "text-rose-500 font-semibold";
            if (line.startsWith("[SYSTEM]")) colorClass = "text-neutral-500";
            
            return <div key={index} className={`whitespace-pre-wrap leading-relaxed ${colorClass}`}>{line}</div>;
          })}
          
          {isDeploying && (
            <div className="flex items-center gap-2 text-amber-500 animate-pulse mt-3">
              <span>⏳</span>
              <span>Host engine compiling active stack profiles... Please hold...</span>
            </div>
          )}
        </div>
      </div>

      {/* 3. BUTTONS ROW (FIXED ANCHOR) */}
      <div className="flex-none flex justify-between pt-4 border-t border-neutral-200 mt-2 bg-white">
        <button 
          onClick={prevStep}
          disabled={isDeploying}
          className="px-5 py-2.5 cursor-pointer bg-neutral-100 hover:bg-neutral-200 disabled:opacity-40 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition shadow-sm"
        >
          {/* px-5 py-2.5 bg-neutral-100 hover:bg-neutral-200 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition shadow-sm */}
          ← Adjust Configurations
        </button>

        {deploySuccess === true && (
          <div className="flex items-center gap-2 text-emerald-600 text-sm font-bold bg-emerald-50 border border-emerald-300 px-4 py-2 rounded-lg shadow-sm">
            ✓ DDB Pipeline Operational
          </div>
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