import React from 'react';
import ServiceForm from './ServiceForm';
import { CONFIG_BLUEPRINT } from '../../config/config';

export default function Step2Configuration({ selectedServices, formValues, updateValue, prevStep, nextStep }) {
  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between h-full bg-white text-neutral-900 animate-fade">
      
      {/* 1. TITLE CONTAINER (UNIFIED) */}
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Environment Variable Overrides</h2>
        <p className="text-base text-neutral-500 mt-1">
          Review and customize the environment parameters for all distributed systems below.
        </p>
      </div>

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
          className="px-5 py-2.5 cursor-pointer bg-neutral-100 hover:bg-neutral-200 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition shadow-sm"
        >
          ← Back
        </button>
        <button 
          onClick={nextStep}
          className="px-6 py-2.5 cursor-pointer bg-cmu-red hover:bg-cmu-red-hover text-white font-bold text-sm rounded-lg shadow-sm border border-transparent transition active:scale-[0.98]"
        >
          Launch DDB Pipeline
        </button>
      </div>
    </div>
  );
}