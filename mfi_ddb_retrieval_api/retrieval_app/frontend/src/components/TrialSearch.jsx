import { useState } from "react";
import { ChevronRight, Loader2, Search } from "lucide-react";
import { searchTrials } from "../api";
import { formatTime, fromLocalInput, toLocalInput } from "../lib/time";
import { ErrorBox } from "./Common";

export function initialSearch() {
  const end = new Date();
  const start = new Date(end.getTime() - 24 * 3600e3);
  return {
    filters: { start: toLocalInput(start), end: toLocalInput(end), trial_id: "", project_id: "", project_name: "", terms: "" },
    result: null,
  };
}

/** Find trials (Type 1). Open one from the list to download its data or files. */
export default function TrialSearch({ identity, blocked, state, setState, onOpen }) {
  const { filters, result } = state;
  const [busy, setBusy] = useState(false);
  const set = (patch) => setState((s) => ({ ...s, filters: { ...s.filters, ...patch } }));

  const run = async (e) => {
    e.preventDefault();
    const body = {
      time_start: fromLocalInput(filters.start),
      time_end: fromLocalInput(filters.end),
      user_id: identity.user_id.trim(),
      user_domain: identity.user_domain.trim() || null,
    };
    if (filters.trial_id.trim()) body.trial_id = filters.trial_id.trim();
    if (filters.project_id.trim()) body.project_id = filters.project_id.trim();
    if (filters.project_name.trim()) body.project_name = filters.project_name.trim();
    const terms = filters.terms.split(",").map((t) => t.trim()).filter(Boolean);
    if (terms.length) body.search_terms = terms;

    setBusy(true);
    const res = await searchTrials(body);
    setBusy(false);
    setState((s) => ({ ...s, result: res.ok ? { trials: res.data?.trials || [], message: res.data?.message } : { error: res.error } }));
  };

  const trials = result?.trials || [];

  return (
    <div className="space-y-6">
      <form onSubmit={run} className="rounded-lg bg-white p-6 shadow">
        <h2 className="text-xl font-semibold text-gray-800">Find trials</h2>
        <p className="mt-1 text-sm text-gray-500">Trials that were active at any point in the time range.</p>

        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">From (local time)</span>
            <input className="input" type="datetime-local" step="1" value={filters.start} onChange={(e) => set({ start: e.target.value })} required />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">To (local time)</span>
            <input className="input" type="datetime-local" step="1" value={filters.end} onChange={(e) => set({ end: e.target.value })} required />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">Trial ID</span>
            <input className="input" value={filters.trial_id} placeholder="trial_001" onChange={(e) => set({ trial_id: e.target.value })} />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">Project ID</span>
            <input className="input" value={filters.project_id} onChange={(e) => set({ project_id: e.target.value })} />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">Project name</span>
            <input className="input" value={filters.project_name} onChange={(e) => set({ project_name: e.target.value })} />
          </label>
          <label className="block">
            <span className="mb-1 block text-sm font-medium text-gray-700">Search terms</span>
            <input className="input" value={filters.terms} placeholder="Comma separated" onChange={(e) => set({ terms: e.target.value })} />
          </label>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-3">
          <button type="submit" className="btn-primary" disabled={busy || blocked}>
            {busy ? <Loader2 size={16} className="animate-spin" /> : <Search size={16} />} Search
          </button>
          {blocked && <span className="text-sm text-red-600">Enter your user ID under “Requesting as” first.</span>}
          {result?.message && <span className="text-sm text-gray-600">{result.message}</span>}
        </div>
      </form>

      <ErrorBox message={result?.error} />

      {result && !result.error && (
        <div className="overflow-hidden rounded-lg bg-white shadow">
          {trials.length === 0 ? (
            <p className="px-6 py-10 text-center text-sm text-gray-500">No trials found. Widen the time range, or check the user ID.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-left text-xs font-medium uppercase tracking-wide text-gray-500">
                  <tr>
                    <th className="px-4 py-2.5">Trial</th>
                    <th className="px-4 py-2.5">Started</th>
                    <th className="px-4 py-2.5">Ended</th>
                    <th className="px-4 py-2.5">Project</th>
                    <th className="px-4 py-2.5" aria-label="Open" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {trials.map((t) => (
                    <tr key={t.uuid} className="cursor-pointer hover:bg-blue-50/50" onClick={() => onOpen(t)}>
                      <td className="px-4 py-3">
                        <div className="font-medium text-gray-800">{t.trial_name}</div>
                        <div className="font-mono text-xs text-gray-400">{t.uuid}</div>
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-gray-700">{formatTime(t.birth_timestamp)}</td>
                      <td className="whitespace-nowrap px-4 py-3 text-gray-700">{formatTime(t.death_timestamp)}</td>
                      <td className="px-4 py-3 text-gray-700">{t.project_name || t.project_id || "—"}</td>
                      <td className="px-4 py-3 text-right">
                        <span className="inline-flex items-center gap-1 text-sm font-medium text-blue-700">
                          Open <ChevronRight size={14} />
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
