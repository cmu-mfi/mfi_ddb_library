import { useState } from 'react';
import { getDefaultFormValues } from './services/configPayload.js';
import ImportConfigurations from './components/setupForm/ImportConfigurations';
import Step0Welcome from './components/setupForm/Step0';
import Step1Selection from './components/setupForm/Step1';
import Step2Configuration from './components/setupForm/Step2';
import Step3Monitor from './components/setupForm/Step3';

export default function App() {
  const [step, setStep] = useState(0); 
  const [dashboardUrl, setDashboardUrl] = useState(null);
  const [importedFileCount, setImportedFileCount] = useState(null);
  const [selectedServices, setSelectedServices] = useState({
    infra: false, kv: false, ts: false, blob: false, rws: false, daa: false, aveva: false
  });
  
  const [formValues, setFormValues] = useState(getDefaultFormValues);

  const applyImportedConfigurations = ({ formValues: imported, selectedServices: services, fileCount }) => {
    setFormValues({ ...getDefaultFormValues(), ...imported });
    setSelectedServices(prev => Object.fromEntries(Object.keys(prev).map(key => [key, !!services[key]])));
    setImportedFileCount(fileCount);
    setStep(1);
  };

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
          <img src={`${import.meta.env.BASE_URL}ddb_logo.png`} alt="CMU DDB Logo" className="h-20 md:h-28 lg:h-32 w-auto bg-black p-3 rounded-2xl" />
          <div>
            <h1 className="text-xl md:text-2xl lg:text-3xl font-bold tracking-tight text-white leading-none">CMU MFI Digital Data Backbone</h1>
            <p className="text-xs md:text-sm lg:text-base text-red-100 font-medium uppercase tracking-wider mt-2">System Setup Wizard</p>
          </div>
        </div>
        {step !== 0 && (
          <div className="px-3 py-1 bg-cmu-red-hover text-xs md:text-base font-semibold text-white rounded-lg border border-red-700/20">
            {step === 'import' ? 'Import Configurations' : `Stage ${step} of 3`}
          </div>
        )}
      </header>

      {/* FIXED BOUNDS MAIN CONTENT WRAPPER */}
      <main className="flex-1 min-h-0 p-4 md:p-6 lg:p-8 flex flex-col justify-center items-center">
        {/* The consistent main bounding box */}
        <div className="bg-white rounded-xl border border-neutral-300 p-6 flex flex-col w-full max-w-5xl h-full shadow-sm min-h-0">
          
          {step === 0 && <Step0Welcome nextStep={() => setStep('import')} />}

          {step === 'import' && (
            <ImportConfigurations
              onImport={applyImportedConfigurations}
              onSkip={() => {
                setImportedFileCount(null);
                setStep(1);
              }}
              prevStep={() => setStep(0)}
            />
          )}

          {step === 1 && importedFileCount !== null && (
            <p role="status" className="flex-none mb-4 p-3 rounded-lg border border-emerald-200 bg-emerald-50 text-emerald-800 text-sm">
              Imported {importedFileCount} YAML configuration {importedFileCount === 1 ? 'file' : 'files'}. Review your service selection, then continue to review the settings.
            </p>
          )}

          {step === 1 && (
            <Step1Selection 
              selectedServices={selectedServices} 
              toggleService={toggleService} 
              prevStep={() => setStep('import')}
              nextStep={() => setStep(2)} 
            />
          )}

          {step === 2 && (
            <Step2Configuration 
              selectedServices={selectedServices} 
              formValues={formValues} 
              updateValue={updateValue} 
              prevStep={() => setStep(1)} 
              nextStep={url => {
                setDashboardUrl(url);
                setStep(3);
              }}
            />
          )}

          {step === 3 && (
            <Step3Monitor 
              selectedServices={selectedServices} 
              formValues={formValues} 
              prevStep={() => setStep(2)} 
              dashboardUrl={dashboardUrl}
            />
          )}
          
        </div>
      </main>
    </div>
  );
}
