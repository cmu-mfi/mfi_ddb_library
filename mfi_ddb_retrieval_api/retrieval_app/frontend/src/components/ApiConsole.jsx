import { useEffect, useState } from "react";
import { Download, Loader2, Play, RotateCcw } from "lucide-react";
import { downloadTrialFiles, listEndpoints, request } from "../api";
import { download, formatBytes } from "../lib/data";
import { CopyButton, ErrorBox } from "./Common";

const ENDPOINTS = [
  { id: "type0", method: "GET", label: "Type 0 - list endpoints" },
  { id: "type1", method: "POST", label: "Type 1 - search trials" },
  { id: "type2", method: "POST", label: "Type 2 - get trial data" },
  { id: "type3", method: "POST", label: "Type 3 - search, and get data if one trial matches" },
  { id: "type4", method: "POST", label: "Type 4 - download files / bytes", file: true },
];

function template(id, identity) {
  const now = new Date();
  const dayAgo = new Date(now.getTime() - 24 * 3600e3);
  const who = { user_id: identity.user_id || "superadmin", user_domain: identity.user_domain || "superadmin" };
  if (id === "type1" || id === "type3") {
    return {
      time_start: dayAgo.toISOString(),
      time_end: now.toISOString(),
      ...who,
      trial_id: null,
      project_id: null,
      project_name: null,
      enterprise_id: null,
      site: null,
      device: null,
      search_terms: null,
      ...(id === "type3" ? { frequency: 0 } : {}),
    };
  }
  if (id === "type2") {
    return {
      trial_uuid: "<paste a trial uuid>",
      ...who,
      time_start: new Date(now.getTime() - 60e3).toISOString(),
      time_end: now.toISOString(),
      frequency: 0,
      data_format: "json",
    };
  }
  if (id === "type4") {
    return {
      trial_uuid: "<paste a trial uuid>",
      ...who,
      time_start: "birth",
      time_end: "now",
    };
  }
  return null;
}

export default function ApiConsole({ identity }) {
  const [endpoint, setEndpoint] = useState("type1");
  const [body, setBody] = useState(() => JSON.stringify(template("type1", identity), null, 2));
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState(null);
  const [parseError, setParseError] = useState(null);
  const [docs, setDocs] = useState(null);

  useEffect(() => {
    listEndpoints().then((r) => r.ok && setDocs(r.data?.endpoints || null));
  }, []);

  const ep = ENDPOINTS.find((e) => e.id === endpoint);

  const choose = (id) => {
    setEndpoint(id);
    const t = template(id, identity);
    setBody(t ? JSON.stringify(t, null, 2) : "");
    setRes(null);
    setParseError(null);
  };

  const send = async () => {
    let payload;
    if (ep.method === "POST") {
      try {
        payload = JSON.parse(body);
      } catch (err) {
        setParseError(`Request body is not valid JSON: ${err.message}`);
        return;
      }
    }
    setParseError(null);
    setBusy(true);
    if (ep.file) {
      // The response is a file: save it, and show its metadata (X-File-Metadata).
      const started = performance.now();
      const f = await downloadTrialFiles(payload, `rws_${endpoint}_download`);
      setBusy(false);
      setRes({
        ok: f.ok,
        status: f.ok ? 200 : f.status,
        ms: performance.now() - started,
        bytes: f.bytes || 0,
        data: f.ok ? { saved_as: f.name, size_bytes: f.bytes, "X-File-Metadata": f.metadata } : null,
        error: f.ok ? null : f.error,
      });
      return;
    }
    const r = await request(`/${endpoint}`, { method: ep.method, body: payload });
    setBusy(false);
    setRes(r);
  };

  const pretty = res?.data ? JSON.stringify(res.data, null, 2) : res?.raw || "";
  const tooBig = pretty.length > 2_000_000;

  return (
    <div className="space-y-6">
      <div className="rounded-lg bg-white p-6 shadow">
        <h2 className="text-xl font-semibold text-gray-800">API console</h2>
        <p className="mt-1 text-sm text-gray-500">
          Send raw requests to the Retrieval API. Requests go to <code>/mfi-ddb/&lt;endpoint&gt;</code> on the configured RWS.
        </p>

        <div className="mt-4 grid gap-4 md:grid-cols-[18rem_1fr]">
          <div className="space-y-1">
            {ENDPOINTS.map((e) => {
              const doc = docs?.find((d) => d.endpoint === `/${e.id}`);
              return (
                <button
                  key={e.id}
                  type="button"
                  onClick={() => !e.disabled && choose(e.id)}
                  disabled={e.disabled}
                  aria-pressed={endpoint === e.id}
                  className={`block w-full rounded-md border px-3 py-2 text-left text-sm ${
                    e.disabled ? "cursor-not-allowed border-dashed border-gray-200 opacity-70" : endpoint === e.id ? "border-blue-600 bg-blue-50" : "border-gray-200 hover:bg-gray-50"
                  }`}
                >
                  <span className="mr-2 rounded bg-gray-200 px-1.5 py-0.5 font-mono text-[11px] text-gray-700">{e.method}</span>
                  <span className="font-medium text-gray-800">/{e.id}</span>
                  <span className="mt-0.5 block text-xs text-gray-500">{doc?.description || e.label}</span>
                  {e.disabled && <span className="mt-1 block text-xs font-medium text-amber-700">Not implemented in RWS yet</span>}
                  {e.file && <span className="mt-1 block text-xs text-gray-500">Returns a file: it is saved, and its metadata shown.</span>}
                </button>
              );
            })}
          </div>

          <div>
            {ep.method === "POST" ? (
              <label className="block">
                <span className="mb-1 flex items-center justify-between text-sm font-medium text-gray-700">
                  Request body (JSON)
                  <button type="button" className="inline-flex items-center gap-1 text-xs font-normal text-gray-600 hover:text-gray-900" onClick={() => choose(endpoint)}>
                    <RotateCcw size={12} /> Reset template
                  </button>
                </span>
                <textarea
                  className="input h-72 font-mono text-xs"
                  spellCheck={false}
                  value={body}
                  onChange={(e) => setBody(e.target.value)}
                />
              </label>
            ) : (
              <p className="rounded-md border border-dashed border-gray-300 p-4 text-sm text-gray-500">No request body for this endpoint.</p>
            )}
            <div className="mt-3 flex flex-wrap items-center gap-3">
              <button type="button" className="btn-primary" onClick={send} disabled={busy}>
                {busy ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />} Send
              </button>
              {res && (
                <span className={`text-sm ${res.ok ? "text-green-700" : "text-red-700"}`}>
                  HTTP {res.status || "—"} · {Math.round(res.ms)} ms · {formatBytes(res.bytes)}
                </span>
              )}
            </div>
          </div>
        </div>
      </div>

      <ErrorBox message={parseError || res?.error} />

      {res && (pretty || res.status) && (
        <div className="overflow-hidden rounded-lg bg-white shadow">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-gray-200 bg-gray-50 px-4 py-2">
            <span className="text-sm font-medium text-gray-700">Response</span>
            <div className="flex items-center gap-1">
              {!tooBig && <CopyButton text={pretty} />}
              <button
                type="button"
                className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-gray-600 hover:bg-gray-100"
                onClick={() => download(`rws_${endpoint}_response.json`, pretty, "application/json")}
              >
                <Download size={14} /> Download
              </button>
            </div>
          </div>
          <pre className="max-h-[36rem] overflow-auto p-4 text-xs leading-relaxed text-gray-800">
            {tooBig ? `${pretty.slice(0, 200_000)}\n\n… response truncated in the view (${formatBytes(pretty.length)}); use Download for all of it.` : pretty}
          </pre>
        </div>
      )}
    </div>
  );
}
