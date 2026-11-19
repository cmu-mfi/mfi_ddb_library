import { useEffect, useState } from "react";
import { Search, Terminal } from "lucide-react";
import ApiConsole from "./components/ApiConsole";
import TrialDetail from "./components/TrialDetail";
import TrialSearch, { initialSearch } from "./components/TrialSearch";
import { IdentityBar } from "./components/Common";
import { identityBlocked, loadIdentity } from "./lib/identity";
import { fetchHealth } from "./api";
import { fromLocalInput } from "./lib/time";
import logoMfi from "./images/logo_mfi.png";

const serverStatusCls = {
  online: "bg-green-600 text-white",
  offline: "bg-red-600 text-white",
  checking: "bg-gray-500 text-white",
};

export default function App() {
  const [identity, setIdentity] = useState(loadIdentity);
  const [view, setView] = useState("trials"); // "trials" | "console"
  const [search, setSearch] = useState(initialSearch);
  const [openTrial, setOpenTrial] = useState(null);
  const [serverStatus, setServerStatus] = useState("checking");
  const [identityStatus, setIdentityStatus] = useState({ state: "empty" });
  const blocked = identityBlocked(identityStatus);

  useEffect(() => {
    const check = async () => setServerStatus((await fetchHealth()).ok ? "online" : "offline");
    check();
    const id = setInterval(check, 10000);
    return () => clearInterval(id);
  }, []);

  const tab = (key, label, Icon) => (
    <button
      type="button"
      onClick={() => {
        setView(key);
        setOpenTrial(null);
      }}
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
          title="Retrieval API (RWS) health"
        >
          <span className="h-2 w-2 shrink-0 rounded-full bg-current" />
          Retrieval API: {serverStatus}
        </div>
      </header>

      <main className="mx-auto max-w-6xl space-y-6 px-4 pt-6 sm:px-8">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-semibold text-gray-800">MFI DDB Data Explorer</h1>
            <p className="mt-1 text-gray-600">Find a trial and download its data or files through the Retrieval API.</p>
          </div>
          <div className="flex flex-wrap gap-1 rounded-lg bg-gray-200 p-1">
            {tab("trials", "Trials", Search)}
            {tab("console", "API console", Terminal)}
          </div>
        </div>

        <IdentityBar identity={identity} onChange={setIdentity} onStatus={setIdentityStatus} />

        {view === "console" ? (
          <ApiConsole identity={identity} />
        ) : openTrial ? (
          <TrialDetail
            key={openTrial.uuid}
            trial={openTrial}
            searchRange={{ start: fromLocalInput(search.filters.start), end: fromLocalInput(search.filters.end) }}
            identity={identity}
            blocked={blocked}
            onBack={() => setOpenTrial(null)}
          />
        ) : (
          <TrialSearch identity={identity} blocked={blocked} state={search} setState={setSearch} onOpen={setOpenTrial} />
        )}
      </main>
    </div>
  );
}
