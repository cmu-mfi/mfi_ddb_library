import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Copy,
  Download,
  Loader2,
  PencilLine,
  Printer,
  RotateCcw,
  Search,
  Users,
} from "lucide-react";
import { printLabels, searchEntries } from "../api";
import { useUsers, userKey, userLabel } from "../lib/users";

// Field types that get a filter control, and how.
const FILTER_KIND = {
  text: "contains",
  textarea: "contains",
  email: "contains",
  select: "any_of",
  number: "range",
  integer: "range",
  date: "dates",
  datetime: "dates",
  checkbox: "equals",
};

const VIEWER_KEY = "mfi-entries-viewer";
const LABEL_SIZE_KEY = "mfi-label-size";

const isVisible = (f) => !f.auto || f.editable;

const formatTime = (iso) => {
  if (!iso) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
};

function cellValue(field, values) {
  if (field.type === "user") {
    const id = values[field.user_id_key];
    return id ? `${id} · ${values[field.domain_key] ?? ""}` : "";
  }
  const v = values[field.name];
  if (v == null) return "";
  if (field.type === "checkbox") return v ? "Yes" : "No";
  if (field.type === "datetime") return formatTime(v);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** UI filter state -> API filters (empty filters dropped). */
function toApiFilters(fields, state) {
  const out = {};
  for (const f of fields) {
    const s = state[f.name];
    if (!s) continue;
    const kind = FILTER_KIND[f.type];
    if (kind === "contains" && s.contains?.trim()) out[f.name] = { contains: s.contains.trim() };
    if (kind === "any_of" && s.any_of?.length) out[f.name] = { any_of: s.any_of };
    if (kind === "range") {
      const r = {};
      if (s.min !== "" && s.min != null) r.min = Number(s.min);
      if (s.max !== "" && s.max != null) r.max = Number(s.max);
      if (Object.keys(r).length) out[f.name] = r;
    }
    if (kind === "dates") {
      const conv = (v) => (f.type === "datetime" ? new Date(v).toISOString() : v);
      const r = {};
      if (s.date_from) r.date_from = conv(s.date_from);
      if (s.date_to) r.date_to = conv(s.date_to);
      if (Object.keys(r).length) out[f.name] = r;
    }
    if (kind === "equals" && s.equals !== "" && s.equals != null) out[f.name] = { equals: s.equals === "yes" };
  }
  return out;
}

function FilterControl({ field, value = {}, onChange }) {
  const kind = FILTER_KIND[field.type];
  const set = (patch) => onChange({ ...value, ...patch });
  const id = `flt-${field.name}`;

  return (
    <div
      className={
        kind === "any_of"
          ? "sm:col-span-2 lg:col-span-3"
          : kind === "dates" && field.type === "datetime"
          ? "sm:col-span-2" // two date-time inputs don't fit in one column
          : ""
      }
    >
      <label htmlFor={id} className="mb-1 block text-xs font-medium text-gray-600">
        {field.label}
      </label>
      {kind === "contains" && (
        <input id={id} className="input" placeholder="contains…" value={value.contains ?? ""}
          onChange={(e) => set({ contains: e.target.value })} />
      )}
      {kind === "any_of" && (
        <div id={id} className="flex flex-wrap gap-2">
          {(field.options || []).map((opt) => {
            const key = String(opt.value);
            const on = (value.any_of || []).includes(key);
            return (
              <button
                type="button"
                key={key}
                aria-pressed={on}
                onClick={() =>
                  set({ any_of: on ? value.any_of.filter((v) => v !== key) : [...(value.any_of || []), key] })
                }
                className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
                  on ? "border-blue-600 bg-blue-600 text-white" : "border-gray-300 bg-white text-gray-700 hover:bg-gray-50"
                }`}
              >
                {opt.label ?? key}
              </button>
            );
          })}
        </div>
      )}
      {kind === "range" && (
        <div className="flex items-center gap-2">
          <input id={id} type="number" step="any" className="input min-w-0 flex-1" placeholder="min" value={value.min ?? ""}
            onChange={(e) => set({ min: e.target.value })} />
          <span className="text-gray-400">–</span>
          <input type="number" step="any" className="input min-w-0 flex-1" placeholder="max" aria-label={`${field.label} max`}
            value={value.max ?? ""} onChange={(e) => set({ max: e.target.value })} />
        </div>
      )}
      {kind === "dates" && (
        <div className="flex items-center gap-2">
          <input id={id} type={field.type === "date" ? "date" : "datetime-local"} className="input min-w-0 flex-1"
            value={value.date_from ?? ""} onChange={(e) => set({ date_from: e.target.value })} />
          <span className="text-gray-400">–</span>
          <input type={field.type === "date" ? "date" : "datetime-local"} className="input min-w-0 flex-1"
            aria-label={`${field.label} to`} value={value.date_to ?? ""}
            onChange={(e) => set({ date_to: e.target.value })} />
        </div>
      )}
      {kind === "equals" && (
        <select id={id} className="input" value={value.equals ?? ""} onChange={(e) => set({ equals: e.target.value })}>
          <option value="">Any</option>
          <option value="yes">Yes</option>
          <option value="no">No</option>
        </select>
      )}
    </div>
  );
}

function ViewerPicker({ viewer, onChange }) {
  const { users, status } = useUsers();
  return (
    <div className="flex items-center gap-2 text-sm">
      <Users size={16} className="text-gray-500" />
      <label htmlFor="entries-viewer" className="text-gray-600">Showing entries of</label>
      <select
        id="entries-viewer"
        className="input w-auto py-1.5"
        value={userKey(viewer)}
        onChange={(e) => {
          const u = users.find((x) => userKey(x) === e.target.value);
          onChange(u ? { user_id: u.user_id, domain: u.domain } : null);
        }}
      >
        <option value="">{status === "loading" ? "Loading users…" : "Select who you are…"}</option>
        {users.map((u) => (
          <option key={userKey(u)} value={userKey(u)}>{userLabel(u)}</option>
        ))}
      </select>
    </div>
  );
}

function downloadCsv(form, columns, entries) {
  const esc = (v) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const header = ["Entered", "Trial ID", "Version", "Created by", ...columns.map((c) => c.label)];
  const rows = entries.map((e) => [
    e.entered_at ?? "",
    e.trial_id ?? "",
    e.version ?? 1,
    e.created_by?.user_id ? `${e.created_by.user_id}@${e.created_by.domain}` : "",
    ...columns.map((c) => cellValue(c, e.values)),
  ]);
  const csv = [header, ...rows].map((r) => r.map(esc).join(",")).join("\n");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  const a = Object.assign(document.createElement("a"), { href: url, download: `${form.id}-entries.csv` });
  a.click();
  URL.revokeObjectURL(url);
}

export default function Entries({ forms, initialFormId, showAll, onUpdate, labels }) {
  const [formId, setFormId] = useState(
    forms.some((f) => f.id === initialFormId) ? initialFormId : forms[0]?.id
  );
  const form = forms.find((f) => f.id === formId);

  // ---- label printing (only for forms with a label configured) ----
  const labelSizes = labels?.sizes || [];
  const canPrint = Boolean(form && labels?.forms?.includes(form.id) && labelSizes.length);
  const [labelSize, setLabelSize] = useState(() => {
    try {
      return localStorage.getItem(LABEL_SIZE_KEY) || labelSizes[0] || "S";
    } catch {
      return labelSizes[0] || "S";
    }
  });
  const [selected, setSelected] = useState(() => new Set());
  const [printing, setPrinting] = useState(false);

  const chooseLabelSize = (size) => {
    setLabelSize(size);
    try {
      localStorage.setItem(LABEL_SIZE_KEY, size);
    } catch {
      /* storage unavailable */
    }
  };

  const print = async (toPrint) => {
    if (!toPrint.length) return;
    // Open the tab right away (inside the click) so popup blockers allow it.
    const tab = window.open("", "_blank");
    if (tab) tab.document.write("<p style='font-family:sans-serif'>Creating labels…</p>");
    setPrinting(true);
    const size = labelSizes.includes(labelSize) ? labelSize : labelSizes[0];
    const res = await printLabels(form.id, size, toPrint);
    setPrinting(false);
    if (!res.ok) {
      tab?.close();
      setError(res.message || "Label printing failed.");
      return;
    }
    const url = URL.createObjectURL(res.blob);
    if (tab) tab.location.href = url;
    else window.open(url, "_blank");
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  };

  const [enteredFrom, setEnteredFrom] = useState("");
  const [enteredTo, setEnteredTo] = useState("");
  const [text, setText] = useState("");
  const [filters, setFilters] = useState({});
  const [viewer, setViewer] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem(VIEWER_KEY)) || null;
    } catch {
      return null;
    }
  });

  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(null);

  const filterFields = useMemo(
    () => (form?.fields || []).filter((f) => isVisible(f) && FILTER_KIND[f.type]),
    [form]
  );
  const columns = useMemo(
    () =>
      (form?.fields || []).filter(
        (f) =>
          isVisible(f) && f.name !== "trial_id" && f.name !== "created_by" && f.type !== "list" && f.type !== "section"
      ),
    [form]
  );

  const run = useCallback(
    async (overrides = {}) => {
      if (!form) return;
      const state = { enteredFrom, enteredTo, text, filters, ...overrides };
      if (!showAll && !viewer) {
        setResult(null);
        setError(null);
        return;
      }
      setLoading(true);
      setError(null);
      const res = await searchEntries(form.id, {
        viewer: showAll ? null : viewer,
        entered_from: state.enteredFrom ? new Date(state.enteredFrom).toISOString() : null,
        entered_to: state.enteredTo ? new Date(state.enteredTo).toISOString() : null,
        text: state.text.trim() || null,
        filters: toApiFilters(filterFields, state.filters),
      });
      setLoading(false);
      setOpen(null);
      setSelected(new Set());
      if (res.ok && res.data?.ok) setResult(res.data);
      else {
        setResult(null);
        setError(res.data?.message || "Search failed.");
      }
    },
    [form, enteredFrom, enteredTo, text, filters, filterFields, showAll, viewer]
  );

  const reset = () => {
    setEnteredFrom("");
    setEnteredTo("");
    setText("");
    setFilters({});
    run({ enteredFrom: "", enteredTo: "", text: "", filters: {} });
  };

  // New form (or viewer) -> fresh search with no filters.
  useEffect(() => {
    setFilters({});
    setResult(null);
    run({ filters: {} });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formId, viewer]);

  const chooseViewer = (v) => {
    setViewer(v);
    try {
      if (v) localStorage.setItem(VIEWER_KEY, JSON.stringify(v));
      else localStorage.removeItem(VIEWER_KEY);
    } catch {
      /* storage unavailable */
    }
  };

  const entries = result?.entries || [];

  return (
    <div className="space-y-6">
      <div className="rounded-lg bg-white shadow">
        {forms.length > 1 && (
          <nav className="flex gap-1 overflow-x-auto border-b border-gray-200 px-4 pt-3" role="tablist">
            {forms.map((f) => (
              <button
                key={f.id}
                type="button"
                role="tab"
                aria-selected={f.id === formId}
                onClick={() => setFormId(f.id)}
                className={`-mb-px whitespace-nowrap border-b-2 px-4 py-2.5 text-sm font-medium transition-colors ${
                  f.id === formId ? "border-blue-600 text-blue-700" : "border-transparent text-gray-500 hover:text-gray-800"
                }`}
              >
                {f.title}
              </button>
            ))}
          </nav>
        )}

        <form
          className="space-y-5 p-6"
          onSubmit={(e) => {
            e.preventDefault();
            run();
          }}
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-gray-800">{form?.title} entries</h2>
              <p className="text-xs text-gray-500">
                Found through the retrieval API by the hidden form id{" "}
                <code className="text-gray-700">{form?.id}</code>.
              </p>
            </div>
            {showAll ? (
              <span className="rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-800">
                Showing everyone's entries
              </span>
            ) : (
              <ViewerPicker viewer={viewer} onChange={chooseViewer} />
            )}
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <div className="sm:col-span-2">
              <label htmlFor="flt-entered-from" className="mb-1 block text-xs font-medium text-gray-600">
                Entered between
              </label>
              <div className="flex items-center gap-2">
                <input id="flt-entered-from" type="datetime-local" className="input min-w-0 flex-1" value={enteredFrom}
                  onChange={(e) => setEnteredFrom(e.target.value)} />
                <span className="text-gray-400">–</span>
                <input type="datetime-local" className="input min-w-0 flex-1" aria-label="Entered to" value={enteredTo}
                  onChange={(e) => setEnteredTo(e.target.value)} />
              </div>
            </div>
            <div>
              <label htmlFor="flt-text" className="mb-1 block text-xs font-medium text-gray-600">
                Search all fields
              </label>
              <input id="flt-text" className="input" placeholder="any text…" value={text}
                onChange={(e) => setText(e.target.value)} />
            </div>
            {filterFields.map((f) => (
              <FilterControl
                key={f.name}
                field={f}
                value={filters[f.name]}
                onChange={(v) => setFilters((prev) => ({ ...prev, [f.name]: v }))}
              />
            ))}
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <button type="submit" className="btn-primary" disabled={loading || (!showAll && !viewer)}>
              {loading ? <Loader2 size={16} className="animate-spin" /> : <Search size={16} />} Search
            </button>
            <button type="button" className="btn-secondary" onClick={reset} disabled={loading}>
              <RotateCcw size={16} /> Reset filters
            </button>
          </div>
        </form>
      </div>

      <div className="rounded-lg bg-white p-6 shadow">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-lg font-semibold text-gray-800">
            {result ? `${result.count} entr${result.count === 1 ? "y" : "ies"}` : "Results"}
            {result?.truncated && (
              <span className="ml-2 text-xs font-normal text-amber-700">showing the newest {entries.length}</span>
            )}
          </h2>
          <div className="flex flex-wrap items-center gap-2">
            {canPrint && (
              <>
                <div className="flex items-center gap-1 rounded-md bg-gray-100 p-0.5 text-xs" role="group" aria-label="Label size">
                  <span className="px-1.5 text-gray-500">Label</span>
                  {labelSizes.map((s) => (
                    <button
                      key={s}
                      type="button"
                      aria-pressed={labelSize === s}
                      onClick={() => chooseLabelSize(s)}
                      className={`rounded px-2 py-1 font-medium ${
                        labelSize === s ? "bg-white text-gray-900 shadow-sm" : "text-gray-600 hover:text-gray-900"
                      }`}
                    >
                      {s}
                    </button>
                  ))}
                </div>
                <button
                  type="button"
                  className="btn-secondary px-3 py-1.5"
                  disabled={selected.size === 0 || printing}
                  onClick={() => print(entries.filter((e) => selected.has(e.entry_id)))}
                >
                  {printing ? <Loader2 size={14} className="animate-spin" /> : <Printer size={14} />}
                  Print selected{selected.size ? ` (${selected.size})` : ""}
                </button>
              </>
            )}
            <button
              type="button"
              className="btn-secondary px-3 py-1.5"
              disabled={entries.length === 0}
              onClick={() => downloadCsv(form, columns, entries)}
            >
              <Download size={14} /> Export CSV
            </button>
          </div>
        </div>

        {error && (
          <div role="alert" className="mb-4 rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </div>
        )}

        {!showAll && !viewer ? (
          <p className="py-6 text-center text-sm text-gray-500">Select who you are to see your entries.</p>
        ) : loading && !result ? (
          <p className="flex items-center justify-center gap-2 py-6 text-sm text-gray-500">
            <Loader2 size={16} className="animate-spin" /> Searching…
          </p>
        ) : result && entries.length === 0 ? (
          <p className="py-6 text-center text-sm text-gray-500">No entries match these filters.</p>
        ) : entries.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead className="border-b border-gray-200 text-xs uppercase tracking-wide text-gray-500">
                <tr>
                  {canPrint && (
                    <th className="w-8 py-2">
                      <input
                        type="checkbox"
                        aria-label="Select all entries"
                        className="h-4 w-4 rounded border-gray-300"
                        checked={entries.length > 0 && selected.size === entries.length}
                        onChange={(ev) =>
                          setSelected(ev.target.checked ? new Set(entries.map((e) => e.entry_id)) : new Set())
                        }
                      />
                    </th>
                  )}
                  <th className="w-8 py-2" />
                  {onUpdate && <th className="py-2 pr-4" />}
                  <th className="whitespace-nowrap py-2 pr-4 font-medium">Entered</th>
                  <th className="whitespace-nowrap py-2 pr-4 font-medium">Trial ID</th>
                  <th className="whitespace-nowrap py-2 pr-4 font-medium">Created by</th>
                  {columns.map((c) => (
                    <th key={c.name} className="whitespace-nowrap py-2 pr-4 font-medium">{c.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {entries.map((e, i) => {
                  const key = `${e.uuid}-${i}`;
                  return (
                    <Fragment key={key}>
                      <tr
                        className="cursor-pointer border-b border-gray-100 hover:bg-gray-50"
                        onClick={() => setOpen(open === key ? null : key)}
                      >
                        {canPrint && (
                          <td className="py-2" onClick={(ev) => ev.stopPropagation()}>
                            <input
                              type="checkbox"
                              aria-label={`Select ${e.trial_id}`}
                              className="h-4 w-4 rounded border-gray-300"
                              checked={selected.has(e.entry_id)}
                              onChange={(ev) =>
                                setSelected((prev) => {
                                  const next = new Set(prev);
                                  if (ev.target.checked) next.add(e.entry_id);
                                  else next.delete(e.entry_id);
                                  return next;
                                })
                              }
                            />
                          </td>
                        )}
                        <td className="py-2 text-gray-400">
                          {open === key ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                        </td>
                        {onUpdate && (
                          <td className="whitespace-nowrap py-1.5 pr-4">
                            <div className="flex gap-1.5">
                              <button
                                type="button"
                                className="btn-secondary px-2.5 py-1 text-xs"
                                title="Open this entry in the form to edit it and save a new version"
                                onClick={(ev) => {
                                  ev.stopPropagation();
                                  onUpdate(form.id, e, "update");
                                }}
                              >
                                <PencilLine size={13} /> Update
                              </button>
                              <button
                                type="button"
                                className="btn-secondary px-2.5 py-1 text-xs"
                                title="Start a new, separate entry pre-filled with this entry's data"
                                onClick={(ev) => {
                                  ev.stopPropagation();
                                  onUpdate(form.id, e, "copy");
                                }}
                              >
                                <Copy size={13} /> Copy
                              </button>
                              {canPrint && (
                                <button
                                  type="button"
                                  className="btn-secondary px-2.5 py-1 text-xs"
                                  title={`Print a ${labelSize} label for this entry`}
                                  disabled={printing}
                                  onClick={(ev) => {
                                    ev.stopPropagation();
                                    print([e]);
                                  }}
                                >
                                  <Printer size={13} /> Print
                                </button>
                              )}
                            </div>
                          </td>
                        )}
                        <td className="whitespace-nowrap py-2 pr-4 text-gray-700">{formatTime(e.entered_at)}</td>
                        <td className="whitespace-nowrap py-2 pr-4 font-mono text-xs">
                          {e.trial_id}
                          {e.version > 1 && (
                            <span
                              className="ml-2 rounded-full bg-blue-50 px-2 py-0.5 font-sans text-xs font-medium text-blue-800"
                              title={`${e.versions.length} earlier version(s) - expand the row to see them`}
                            >
                              v{e.version}
                            </span>
                          )}
                        </td>
                        <td className="whitespace-nowrap py-2 pr-4">
                          {e.created_by?.user_id}
                          <span className="ml-1 text-xs text-gray-400">{e.created_by?.domain}</span>
                        </td>
                        {columns.map((c) => (
                          <td key={c.name} className="max-w-xs truncate py-2 pr-4">{cellValue(c, e.values)}</td>
                        ))}
                      </tr>
                      {open === key && (
                        <tr className="border-b border-gray-100 bg-gray-50">
                          <td
                            colSpan={(canPrint ? 1 : 0) + (onUpdate ? 5 : 4) + columns.length}
                            className="space-y-3 p-3"
                          >
                            <p className="text-xs font-medium text-gray-600">
                              Current version{e.version > 1 ? ` (v${e.version})` : ""}
                            </p>
                            <pre className="max-h-64 overflow-auto rounded border border-gray-200 bg-white p-3 text-xs">
                              {JSON.stringify(e.values, null, 2)}
                            </pre>
                            {e.versions?.length > 0 && (
                              <div>
                                <p className="mb-2 text-xs font-medium text-gray-600">Previous versions</p>
                                <div className="space-y-2">
                                  {e.versions.map((v) => (
                                    <details key={v.uuid} className="rounded border border-gray-200 bg-white">
                                      <summary className="cursor-pointer px-3 py-2 text-xs text-gray-700">
                                        <span className="font-medium">v{v.version}</span> · {formatTime(v.entered_at)} ·{" "}
                                        {v.created_by?.user_id}
                                        <span className="ml-1 text-gray-400">{v.created_by?.domain}</span>
                                      </summary>
                                      <pre className="max-h-64 overflow-auto border-t border-gray-200 p-3 text-xs">
                                        {JSON.stringify(v.values, null, 2)}
                                      </pre>
                                    </details>
                                  ))}
                                </div>
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
      </div>
    </div>
  );
}
