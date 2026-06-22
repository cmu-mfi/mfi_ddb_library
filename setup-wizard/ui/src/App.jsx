import React, { useState } from 'react';
import { CONFIG_BLUEPRINT } from './config/config';
import Step0Welcome from './components/setupForm/Step0';
import Step1Selection from './components/setupForm/Step1';
import Step2Configuration from './components/setupForm/Step2';
import Step3Monitor from './components/setupForm/Step3';

export default function App() {
  const [step, setStep] = useState(0); 
  const [selectedServices, setSelectedServices] = useState({
    infra: true, kv: false, ts: false, blob: false, rws: false
  });
  
  const [formValues, setFormValues] = useState(() => {
    const initial = {};
    Object.values(CONFIG_BLUEPRINT).forEach(sec => {
      sec.fields.forEach(f => { initial[f.key] = f.default; });
    });
    return initial;
  });

  const toggleService = (id) => {
    setSelectedServices(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const updateValue = (key, value) => {
    setFormValues(prev => ({ ...prev, [key]: value }));
  };

  return (
    <div className="h-screen w-screen bg-neutral-100 text-neutral-800 flex flex-col font-sans antialiased overflow-hidden">
      
      {/* VIBRANT CMU BRAND TOP BANNER */}
      <header className="flex-none px-8 py-2 bg-cmu-red flex justify-between items-center shadow-md z-10">
        <div className="flex items-center gap-3">
          <img src="/ddb_logo.png" alt="CMU Logo" className="h-20 md:h-28 lg:h-32 w-auto bg-black p-3 rounded-2xl" />
          <div>
            <h1 className="text-xl md:text-2xl lg:text-3xl font-bold tracking-tight text-white leading-none">CMU MFI Digital Data Backbone</h1>
            <p className="text-xs md:text-sm lg:text-base text-red-100 font-medium uppercase tracking-wider mt-2">System Setup Wizard</p>
          </div>
        </div>
        {step > 0 && (
          <div className="px-3 py-1 bg-cmu-red-hover text-xs md:text-base font-semibold text-white rounded-lg border border-red-700/20">
            Stage {step} of 3
          </div>
        )}
      </header>

      {/* FIXED BOUNDS MAIN CONTENT WRAPPER */}
      <main className="flex-1 min-h-0 p-4 md:p-6 lg:p-8 flex flex-col justify-center items-center">
        {/* The consistent main bounding box */}
        <div className="bg-white rounded-xl border border-neutral-300 p-6 flex flex-col w-full max-w-5xl h-full shadow-sm min-h-0">
          
          {step === 0 && <Step0Welcome nextStep={() => setStep(1)} />}

          {step === 1 && (
            <Step1Selection 
              selectedServices={selectedServices} 
              toggleService={toggleService} 
              nextStep={() => setStep(2)} 
            />
          )}

          {step === 2 && (
            <Step2Configuration 
              selectedServices={selectedServices} 
              formValues={formValues} 
              updateValue={updateValue} 
              prevStep={() => setStep(1)} 
              nextStep={() => setStep(3)} 
            />
          )}

          {step === 3 && (
            <Step3Monitor 
              selectedServices={selectedServices} 
              formValues={formValues} 
              prevStep={() => setStep(2)} 
            />
          )}
          
        </div>
      </main>
    </div>
  );
}