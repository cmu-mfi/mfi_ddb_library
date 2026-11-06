import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, ClipboardList, Loader2, Radio, Settings as SettingsIcon, Wrench } from "lucide-react";
import FormBuilder from "./components/FormBuilder";
import MetadataForm from "./components/MetadataForm";
import Settings from "./components/Settings";
import SubmissionHistory from "./components/SubmissionHistory";
import { fetchConfig, fetchHealth, fetchSubmissions } from "./api";
import logoMfi from "./images/logo_mfi.png";

const serverStatusCls = {
  online: "bg-green-600 text-white",
  offline: "bg-red-600 text-white",
  checking: "bg-gray-500 text-white",
};

export default function App() {
  const [config, setConfig] = useState(null);
  const [loadError, setLoadError] = useState(null);
  const [activeId, setActiveId] = useState(null);
  const [view, setView] = useState("forms"); // "forms" | "builder" | "settings"
  const [serverStatus, setServerStatus] = useState("checking");
  const [history, setHistory] = useState([]);

  const loadConfig = useCallback(async () => {
    const res = await fetchConfig();
    if (!res.ok) {
      setLoadError(res.data?.message || "Could not load the form configuration.");
      return;
    }
    setLoadError(null);
    // Keep the same object if nothing changed so in-progress input isn't reset.
    setConfig((prev) => (prev && JSON.stringify(prev) === JSON.stringify(res.data) ? prev : res.data));
    setActiveId((prev) => (res.data.forms.some((f) => f.id === prev) ? prev : res.data.forms[0]?.id ?? null));
  }, []);

  const loadHistory = useCallback(async () => {
    const res = await fetchSubmissions(20);
    if (res.ok && Array.isArray(res.data)) setHistory(res.data);
  }, []);

  useEffect(() => {
    loadConfig();
    loadHistory();
  }, [loadConfig, loadHistory]);

  useEffect(() => {
    const check = async () => setServerStatus((await fetchHealth()).ok ? "online" : "offline");
    check();
    const id = setInterval(check, 10000);
    return () => clearInterval(id);
  }, []);

  // Pick up config edits when the user returns to the tab.
  useEffect(() => {
    const onFocus = () => loadConfig();
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [loadConfig]);

  useEffect(() => {
    if (config?.app?.title) document.title = config.app.title;
  }, [config?.app?.title]);

  const activeForm = config?.forms.find((f) => f.id === activeId);
  const openForm = async (id) => {
    await loadConfig();
    setActiveId(id);
    setView("forms");
  };

  const viewBtn = (key, label, Icon) => (
    <button
      type="button"
      onClick={() => setView(key)}
      aria-pressed={view === key}
      className={`inline-flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
        view === key ? "bg-white text-gray-900 shadow-sm" : "text-gray-600 hover:text-gray-900"
      }`}
    >
      <Icon size={16} /> {label}
    </button>
  );

  return (
    <div className="min-h-screen bg-gray-100 pb-12">
      <header className="flex flex-wrap items-center justify-between gap-4 px-4 pt-6 sm:px-8 sm:pt-8">
        <a href="https://cmu-mfi.github.io/ddb/" target="_blank" rel="noreferrer">
          <img src={logoMfi} alt="Manufacturing Futures Institute" width={200} />
        </a>
        <div
          className={`flex items-center gap-1.5 rounded-full px-3 py-2 text-xs font-medium shadow-md ${serverStatusCls[serverStatus]}`}
        >
          <span className="h-2 w-2 shrink-0 rounded-full bg-current" />
          Server: {serverStatus}
        </div>
      </header>

      <main className="mx-auto max-w-5xl space-y-6 px-4 pt-6 sm:px-8">
        {!config && !loadError && (
          <div className="flex items-center justify-center gap-2 py-24 text-gray-500">
            <Loader2 className="animate-spin" size={18} /> Loading forms…
          </div>
        )}

        {loadError && (
          <div role="alert" className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
            <p className="font-medium">The form service is not available.</p>
            <p className="mt-1">{loadError}</p>
            <button type="button" className="btn-secondary mt-3 py-1.5" onClick={loadConfig}>
              Retry
            </button>
          </div>
        )}

        {config && (
          <>
            <div>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <h1 className="text-3xl font-semibold text-gray-800">{config.app.title}</h1>
                <div className="flex gap-1 rounded-lg bg-gray-200 p-1">
                  {viewBtn("forms", "Fill in forms", ClipboardList)}
                  {config.builder?.enabled && viewBtn("builder", "Form builder", Wrench)}
                  {viewBtn("settings", "Settings", SettingsIcon)}
                </div>
              </div>
              {config.app.subtitle && <p className="mt-1 text-gray-600">{config.app.subtitle}</p>}
              <p className="mt-3 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-gray-500">
                <Radio size={14} />
                Publishing via KV adapter to <code className="text-gray-700">{config.target.broker}</code>
                <span>·</span>
                <code className="break-all text-gray-700">
                  {(view === "forms" && activeForm?.topic_prefix) || config.target.topic_prefix}/…
                </code>
                {view === "forms" && activeForm?.site && (
                  <span className="rounded-full bg-blue-50 px-2 py-0.5 font-medium text-blue-800">
                    Site: {activeForm.site}
                  </span>
                )}
                {config.dry_run && (
                  <span className="rounded-full bg-amber-100 px-2 py-0.5 font-medium text-amber-800">
                    Dry run – nothing is published
                  </span>
                )}
              </p>
            </div>

            {(config.config_error || config.config_warnings?.length > 0) && (
              <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
                <p className="flex items-center gap-2 font-medium">
                  <AlertTriangle size={16} /> Configuration notes
                </p>
                <ul className="mt-2 list-disc space-y-1 pl-5">
                  {config.config_error && <li>{config.config_error} (showing the last valid configuration)</li>}
                  {config.config_warnings?.map((w, i) => (
                    <li key={i}>{w}</li>
                  ))}
                </ul>
              </div>
            )}

            {view === "settings" ? (
              <div className="rounded-lg bg-white p-6 shadow">
                <Settings onChanged={loadConfig} />
              </div>
            ) : view === "builder" ? (
              <div className="rounded-lg bg-white p-6 shadow">
                <FormBuilder
                  builder={config.builder}
                  defaultSite={config.target.default_site}
                  yamlForms={config.forms.filter((f) => f.source !== "ui")}
                  onChanged={loadConfig}
                  onOpenForm={openForm}
                />
              </div>
            ) : (
            <>
            <div className="rounded-lg bg-white shadow">
              {config.forms.length > 1 && (
                <nav className="flex gap-1 overflow-x-auto border-b border-gray-200 px-4 pt-3" role="tablist">
                  {config.forms.map((f) => (
                    <button
                      key={f.id}
                      type="button"
                      role="tab"
                      aria-selected={f.id === activeId}
                      onClick={() => setActiveId(f.id)}
                      className={`-mb-px whitespace-nowrap border-b-2 px-4 py-2.5 text-sm font-medium transition-colors ${
                        f.id === activeId
                          ? "border-blue-600 text-blue-700"
                          : "border-transparent text-gray-500 hover:text-gray-800"
                      }`}
                    >
                      {f.title}
                    </button>
                  ))}
                </nav>
              )}
              <div className="p-6">{activeForm && <MetadataForm form={activeForm} onSubmitted={loadHistory} />}</div>
            </div>

            {config.history_enabled && <SubmissionHistory items={history} onRefresh={loadHistory} />}
            </>
            )}
          </>
        )}
      </main>
    </div>
  );
}
