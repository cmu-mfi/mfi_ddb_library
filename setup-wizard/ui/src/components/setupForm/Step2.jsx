import React, { useState } from 'react';
import ServiceForm from './ServiceForm';
import { CONFIG_BLUEPRINT } from '../../config/config';
import { deployPipeline } from '../../services/api';

export default function Step2Configuration({ selectedServices, formValues, updateValue, prevStep, nextStep }) {
  const [isDeploying, setIsDeploying] = useState(false);
  const [errorMsg, setErrorMsg] = useState(null);

  const handleLaunchPipeline = async () => {
    setIsDeploying(true);
    setErrorMsg(null);

    try {
      // 1. POST the custom values to write text configurations to the host runtime filesystem
      const result = await deployPipeline(formValues);
      console.log('Configurations written successfully:', result);
      
      // 2. Advance directly to Step 3 (The Streaming Terminal Monitor)
      nextStep();
    } catch (err) {
      console.error('Configuration assembly execution error:', err);
      setErrorMsg(err.message || 'An unexpected server error occurred while writing configurations.');
    } finally {
      setIsDeploying(false);
    }
  };

  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between h-full bg-white text-neutral-900 animate-fade">
      
      {/* 1. TITLE CONTAINER (UNIFIED) */}
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Environment Variable Overrides</h2>
        <p className="text-base text-neutral-500 mt-1">
          Review and customize the environment parameters for all distributed systems below. Fields are pre-populated with default values, you can modify them as needed.
        </p>
      </div>

      {/* ERROR FEEDBACK BANNER */}
      {errorMsg && (
        <div className="flex-none mb-4 p-3 bg-rose-50 border border-rose-200 text-rose-700 text-sm rounded-lg font-medium animate-fade">
          ⚠️ {errorMsg}
        </div>
      )}

      {/* 2. BODY AREA (SCROLLABLE) */}
      <div className="flex-1 overflow-y-auto pr-2 space-y-4 min-h-0 custom-scrollbar py-2">
        {Object.keys(CONFIG_BLUEPRINT).map((key) => {
          return (
            <ServiceForm 
              key={key} 
              sectionKey={key} 
              configDef={CONFIG_BLUEPRINT[key]} 
              currentValues={formValues} 
              onValueChange={updateValue} 
            />
          );
        })}
      </div>

      {/* 3. BUTTONS ROW (FIXED ANCHOR) */}
      <div className="flex-none flex justify-between pt-4 border-t border-neutral-200 mt-2 bg-white">
        <button 
          onClick={prevStep}
          disabled={isDeploying}
          className="px-5 py-2.5 cursor-pointer bg-neutral-100 hover:bg-neutral-200 disabled:opacity-50 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition shadow-sm"
        >
          ← Back
        </button>
        
        <button 
          onClick={handleLaunchPipeline}
          disabled={isDeploying}
          className="px-6 py-2.5 cursor-pointer bg-cmu-red hover:bg-cmu-red-hover disabled:bg-neutral-400 text-white font-bold text-sm rounded-lg shadow-sm border border-transparent transition active:scale-[0.98] flex items-center gap-2"
        >
          {isDeploying ? (
            <>
              <svg className="animate-spin h-4 w-4 text-white" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
              </svg>
              Assembling Layout...
            </>
          ) : (
            'Launch DDB Pipeline'
          )}
        </button>
      </div>

    </div>
  );
}