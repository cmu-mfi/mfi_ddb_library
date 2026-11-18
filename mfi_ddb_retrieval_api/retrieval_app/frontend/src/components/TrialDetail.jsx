import { useState } from "react";
import { ArrowLeft, Download, FileArchive, Loader2 } from "lucide-react";
import { downloadTrialData, downloadTrialFiles } from "../api";
import { formatBytes } from "../lib/data";
import { formatTime, fromLocalInput, parseTime, toLocalInput } from "../lib/time";
import { CopyButton, ErrorBox, JsonBlock } from "./Common";

/*
 * Download window. Each edge is sent as `exact` while it is untouched: the
 * trial's own timestamp string from Type 1 (full precision, so nothing at the
 * very start or end is cut off), a search bound, or "now" (resolved when the
 * download starts, so a running trial includes everything recorded so far).
 * The time boxes hold whole seconds; once an edge is edited, the box is sent.
 */
const NOW = "now";

function edge(exact) {
  return { exact, input: toLocalInput(exact === NOW ? new Date() : parseTime(exact)) };
}

/** The whole trial: its start until its end, or until now if it has no later end. */
function wholeTrial(trial) {
  const born = parseTime(trial.birth_timestamp);
  const died = parseTime(trial.death_timestamp);
  return { start: edge(trial.birth_timestamp), end: edge(died && born && died > born ? trial.death_timestamp : NOW) };
}

/** The part of the trial inside the search range (the default). */
function searchWindow(trial, range) {
  const whole = wholeTrial(trial);
  const s = parseTime(range?.start);
  const e = parseTime(range?.end);
  const trialEnd = whole.end.exact === NOW ? new Date() : parseTime(whole.end.exact);
  return {
    start: s && s > parseTime(trial.birth_timestamp) ? edge(range.start) : whole.start,
    end: e && e < trialEnd ? edge(range.end) : whole.end,
  };
}

function sendable(e) {
  if (e.exact === NOW) return new Date().toISOString();
  return e.exact ?? fromLocalInput(e.input);
}

/** One trial: its metadata, and downloads of its data (Type 2) or files (Type 4). */
export default function TrialDetail({ trial, searchRange, identity, blocked, onBack }) {
  const [win, setWin] = useState(() => searchWindow(trial, searchRange));
  const [format, setFormat] = useState("csv");
  const [busy, setBusy] = useState(null); // "data" | "files" while downloading
  const [result, setResult] = useState(null); // { saved } or { error }

  const editEdge = (key, input) => setWin((w) => ({ ...w, [key]: { exact: null, input } }));

  const body = () => ({
    trial_uuid: trial.uuid,
    user_id: identity.user_id.trim(),
    user_domain: identity.user_domain.trim() || null,
    time_start: sendable(win.start),
    time_end: sendable(win.end),
  });
  const fileBase = `${trial.trial_name}_${trial.uuid.slice(0, 8)}`;

  const run = async (kind) => {
    setBusy(kind);
    setResult(null);
    const res =
      kind === "files"
        ? await downloadTrialFiles(body(), `${fileBase}_files`)
        : await downloadTrialData({ ...body(), data_format: format }, `${fileBase}.${format}`);
    setBusy(null);
    if (!res.ok) {
      setResult({ error: res.error });
      return;
    }
    const files = res.metadata?.files?.length;
    setResult({ saved: `Saved ${res.name} (${formatBytes(res.bytes)}${files ? `, ${files} file${files === 1 ? "" : "s"}` : ""}).` });
  };

  return (
    <div className="space-y-6">
      <button type="button" className="btn-secondary py-1.5" onClick={onBack}>
        <ArrowLeft size={16} /> Back to results
      </button>

      <div className="rounded-lg bg-white p-6 shadow">
        <h2 className="text-2xl font-semibold text-gray-800">{trial.trial_name}</h2>
        <p className="mt-1 flex items-center gap-1 font-mono text-xs text-gray-500">
          {trial.uuid} <CopyButton text={trial.uuid} label="Copy UUID" />
        </p>
        <dl className="mt-4 grid gap-x-6 gap-y-2 text-sm sm:grid-cols-3">
          <div>
            <dt className="text-xs text-gray-500">Started</dt>
            <dd className="text-gray-900">{formatTime(trial.birth_timestamp)}</dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500">Ended</dt>
            <dd className="text-gray-900">{formatTime(trial.death_timestamp)}</dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500">Project</dt>
            <dd className="text-gray-900">{trial.project_name || trial.project_id || "—"}</dd>
          </div>
        </dl>
        {(trial.data_topics || []).length > 0 && (
          <div className="mt-4">
            <p className="mb-1 text-xs text-gray-500">Data topics</p>
            <ul className="space-y-1">
              {trial.data_topics.map((t) => (
                <li key={t} className="break-all font-mono text-xs text-gray-700">
                  {t}
                </li>
              ))}
            </ul>
          </div>
        )}
        <div className="mt-4">
          <JsonBlock title="Trial metadata" value={trial.metadata || {}} />
        </div>
      </div>

      <div className="rounded-lg bg-white p-6 shadow">
        <h3 className="text-lg font-semibold text-gray-800">Download</h3>
        <p className="mt-1 text-sm text-gray-500">
          The window starts as the part of this trial inside your search. Change the times if you need a different one.
        </p>

        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">From (local time)</span>
            <input className="input" type="datetime-local" step="1" value={win.start.input} onChange={(e) => editEdge("start", e.target.value)} />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">To (local time)</span>
            <input className="input" type="datetime-local" step="1" value={win.end.input} onChange={(e) => editEdge("end", e.target.value)} />
            {win.end.exact === NOW && (
              <span className="mt-1 block text-xs text-gray-500">Until now: everything recorded up to when you download.</span>
            )}
          </label>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-3">
          <select className="input w-auto py-2" value={format} onChange={(e) => setFormat(e.target.value)} aria-label="Data format">
            <option value="csv">CSV</option>
            <option value="json">JSON</option>
          </select>
          <button type="button" className="btn-primary" onClick={() => run("data")} disabled={!!busy || blocked}>
            {busy === "data" ? <Loader2 size={16} className="animate-spin" /> : <Download size={16} />}
            {busy === "data" ? "Downloading…" : "Download data"}
          </button>
          <button type="button" className="btn-secondary" onClick={() => run("files")} disabled={!!busy || blocked}>
            {busy === "files" ? <Loader2 size={16} className="animate-spin" /> : <FileArchive size={16} />}
            {busy === "files" ? "Downloading…" : "Download files"}
          </button>
          {blocked && <span className="text-sm text-red-600">Enter your user ID under “Requesting as” first.</span>}
        </div>
        <p className="mt-2 text-xs text-gray-500">
          Data: every datapoint in the window. Files: the files stored in the window, several as a .zip. Long windows of
          high-rate data can take minutes.
        </p>
        {result?.saved && <p className="mt-3 text-sm text-green-700">{result.saved}</p>}
      </div>

      <ErrorBox message={result?.error} />
    </div>
  );
}
