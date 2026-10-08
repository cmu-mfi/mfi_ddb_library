import { useRef, useState } from 'react';
import ServiceForm from './ServiceForm';
import { YAML_CONFIG_BLUEPRINT } from '../../config/yamlConfig';
import { deployPipeline } from '../../services/api';

export default function Step2Configuration({ selectedServices, formValues, updateValue, prevStep, nextStep }) {
  const modules = { infra: 'MQTT Broker', kv: 'Key-Value', ts: 'TimescaleDB', blob: 'Blob', rws: 'Metadata / RWS', daa: 'Data Adapter App', aveva: 'AVEVA PI' };
  const pages = Object.keys(modules).flatMap(module =>
    selectedServices[module]
      ? Object.entries(YAML_CONFIG_BLUEPRINT).filter(([, config]) => config.module === module)
      : []
  );
  const [pageIndex, setPageIndex] = useState(0);
  const currentIndex = Math.max(0, Math.min(pageIndex, pages.length - 1));
  const [pageKey, config] = pages[currentIndex] ?? [];
  const isLastPage = pages.length === 0 || currentIndex === pages.length - 1;
  const headingRef = useRef(null);
  const contentRef = useRef(null);

  const goToPage = index => {
    setPageIndex(index);
    setErrorMsg(null);
    contentRef.current?.scrollTo(0, 0);
    headingRef.current?.focus();
  };
  const [isDeploying, setIsDeploying] = useState(false);
  const [errorMsg, setErrorMsg] = useState(null);

  const handleLaunchPipeline = async () => {
    setIsDeploying(true);
    setErrorMsg(null);

    try {
      // 1. POST the custom values to write text configurations to the host runtime filesystem
      const result = await deployPipeline(formValues, selectedServices);
      console.log('Configurations written successfully:', result);
      
      // 2. Advance directly to Step 3 (The Streaming Terminal Monitor)
      nextStep(result.dashboard_url);
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
        <h2 ref={headingRef} tabIndex={-1} className="text-2xl font-bold text-neutral-900 tracking-tight">
          {config ? modules[config.module] : 'Configuration'}
        </h2>
        {config && (
          <p className="text-sm font-semibold text-neutral-600 mt-1" aria-live="polite">
            Configuration {currentIndex + 1} of {pages.length}
          </p>
        )}
        <p className="text-base text-neutral-500 mt-1">
          {config
            ? 'Review each configuration file in order. Use Next to continue or Back to revise your inputs. Your values are kept as you move between pages.'
            : 'The selected services have no configuration YAML inputs. You can continue or go back to change your selection.'}
        </p>
      </div>

      {/* ERROR FEEDBACK BANNER */}
      {errorMsg && (
        <div className="flex-none mb-4 p-3 bg-rose-50 border border-rose-200 text-rose-700 text-sm rounded-lg font-medium animate-fade">
          ⚠️ {errorMsg}
        </div>
      )}

      {/* 2. BODY AREA (SCROLLABLE) */}
      <div ref={contentRef} className="flex-1 overflow-y-auto pr-2 space-y-4 min-h-0 custom-scrollbar py-2">
        {config && <ServiceForm key={pageKey} configDef={config} currentValues={formValues} onValueChange={updateValue} />}
      </div>

      {/* 3. BUTTONS ROW (FIXED ANCHOR) */}
      <div className="flex-none flex justify-between pt-4 border-t border-neutral-200 mt-2 bg-white">
        <button 
          onClick={() => currentIndex === 0 ? prevStep() : goToPage(currentIndex - 1)}
          disabled={isDeploying}
          className="px-5 py-2.5 cursor-pointer bg-neutral-100 hover:bg-neutral-200 disabled:opacity-50 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition shadow-sm"
        >
          {currentIndex === 0 ? '← Service Selection' : '← Previous Configuration'}
        </button>
        
        <button 
          onClick={() => isLastPage ? handleLaunchPipeline() : goToPage(currentIndex + 1)}
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
            isLastPage ? 'Launch DDB Pipeline' : 'Next Configuration →'
          )}
        </button>
      </div>

    </div>
  );
}
