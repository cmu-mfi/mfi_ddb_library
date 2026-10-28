import React from 'react';

export default function Step0Welcome({ nextStep }) {
  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between h-full animate-fade">
      
      {/* 1. TITLE CONTAINER (UNIFIED) */}
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-black text-neutral-900 tracking-tight">
          Welcome to the Digital Data Backbone Setup Wizard
        </h2>
        <p className="text-base text-neutral-600 leading-relaxed mt-1">
          This orchestration tool configures and provisions infrastructure environments for the Manufacturing Futures Institute (MFI).
        </p>
      </div>

      {/* 2. BODY AREA (SCROLLABLE) */}
      <div className="flex-1 overflow-y-auto pr-2 space-y-6 min-h-0 custom-scrollbar py-2">
        <div className="border border-neutral-300 bg-neutral-50 rounded-xl p-5 space-y-3 max-w-3xl">
          <h4 className="text-base font-bold uppercase tracking-wider text-neutral-700">Before you begin:</h4>
          <ul className="text-sm text-neutral-600 space-y-2.5 list-disc pl-4">
            <li>Ensure the target host machine has an active <span className="font-bold bg-neutral-200 text-red-500 px-1 py-0.5 rounded">Docker Daemon</span> instance running.</li>
            <li>Verify execution permissions are granted for container image allocation loops.</li>
            <li>Have your network configuration criteria and interface binding port offsets ready.</li>
          </ul>
        </div>
        
        <p className="text-sm text-neutral-500 max-w-3xl">
          The wizard will guide you through choosing system modules, configuring network environment parameters, and initializing deployment commands.
        </p>
      </div>

      {/* 3. BUTTONS ROW (FIXED ANCHOR) */}
      <div className="flex-none flex justify-end pt-4 border-t border-neutral-200 mt-2 bg-white">
        <button 
          onClick={nextStep}
          className="px-6 py-2.5 cursor-pointer bg-cmu-red hover:bg-cmu-red-hover text-white font-semibold text-sm rounded-lg shadow-sm transition active:scale-[0.98]"
        >
          Get Started →
        </button>
      </div>
    </div>
  );
}