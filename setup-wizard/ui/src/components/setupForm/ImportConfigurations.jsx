import { useRef, useState } from 'react';
import { importConfigurations } from '../../services/configurationFiles.js';

export default function ImportConfigurations({ onImport, onSkip, prevStep }) {
  const folderInput = useRef(null);
  const zipInput = useRef(null);
  const [isImporting, setIsImporting] = useState(false);
  const [error, setError] = useState(null);

  const handleFiles = async (event, source) => {
    const files = Array.from(event.target.files || []);
    event.target.value = '';
    if (!files.length) return;
    setIsImporting(true);
    setError(null);
    try {
      onImport(await importConfigurations(files, source));
    } catch (err) {
      setError(err.message || 'Unable to import configurations. Choose another folder or ZIP.');
    } finally {
      setIsImporting(false);
    }
  };

  return (
    <div className="flex-1 flex flex-col min-h-0 justify-between animate-fade" aria-busy={isImporting}>
      <div className="flex-none pb-4">
        <h2 className="text-2xl font-bold text-neutral-900 tracking-tight">Import saved configurations?</h2>
        <p className="text-base text-neutral-500 mt-2">
          Choose a folder or ZIP containing your YAML configuration files. You can review and edit the settings before starting any services.
        </p>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto py-2 space-y-4">
        <div className="p-5 bg-neutral-50 rounded-xl border border-neutral-300 space-y-4">
          <p className="text-sm text-neutral-600">
            Use a ZIP exported by this wizard, or select its extracted folder. Keep the service folders and YAML filenames together.
          </p>
          <input ref={folderInput} type="file" webkitdirectory="" multiple hidden disabled={isImporting}
            aria-label="Configuration folder" onChange={event => handleFiles(event, 'folder')} />
          <input ref={zipInput} type="file" accept=".zip,application/zip" hidden disabled={isImporting}
            aria-label="Configuration ZIP" onChange={event => handleFiles(event, 'zip')} />
          <div className="flex flex-wrap gap-3">
            <button type="button" onClick={() => folderInput.current.click()} disabled={isImporting}
              className="px-5 py-2.5 cursor-pointer bg-white hover:bg-neutral-100 disabled:opacity-50 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-300 transition">
              Import Folder
            </button>
            <button type="button" onClick={() => zipInput.current.click()} disabled={isImporting}
              className="px-5 py-2.5 cursor-pointer bg-white hover:bg-neutral-100 disabled:opacity-50 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-300 transition">
              Upload ZIP
            </button>
          </div>
          {isImporting && <p role="status" className="text-sm text-neutral-600">Reading configurations...</p>}
        </div>
        {error && <p role="alert" className="p-3 bg-rose-50 border border-rose-200 text-rose-700 text-sm rounded-lg">{error}</p>}
      </div>

      <div className="flex-none flex flex-wrap justify-between gap-3 pt-4 border-t border-neutral-200 mt-2">
        <button type="button" onClick={prevStep} disabled={isImporting}
          className="px-5 py-2.5 cursor-pointer bg-neutral-100 hover:bg-neutral-200 disabled:opacity-50 text-neutral-800 font-bold text-sm rounded-lg border border-neutral-200 transition">
          ← Back
        </button>
        <button type="button" onClick={onSkip} disabled={isImporting}
          className="px-6 py-2.5 cursor-pointer bg-cmu-red hover:bg-cmu-red-hover disabled:opacity-50 text-white font-bold text-sm rounded-lg transition">
          No, configure manually →
        </button>
      </div>
    </div>
  );
}
