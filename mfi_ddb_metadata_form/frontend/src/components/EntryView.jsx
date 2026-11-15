import { useEffect, useMemo, useState } from "react";
import { ArrowLeft, Copy, Loader2, PencilLine, Printer, Scissors } from "lucide-react";
import { cellValue, formatTime, shortEntryId } from "../lib/entries";
import { useUsers } from "../lib/users";

const isShown = (f) => !f.hidden && (!f.auto || f.editable);
const blank = <span className="text-gray-400">—</span>;

/** Split the form's fields into [{section, fields}] groups (section null = before any heading). */
function groupBySection(fields) {
  const groups = [{ section: null, fields: [] }];
  for (const f of fields) {
    if (f.type === "section") groups.push({ section: f, fields: [] });
    else if (isShown(f)) groups[groups.length - 1].fields.push(f);
  }
  return groups.filter((g) => g.section || g.fields.length);
}

function UserValue({ userId, domain, names }) {
  if (!userId) return blank;
  const name = names.get(`${userId}\u0000${domain}`);
  return (
    <span>
      {name && <span className="mr-1.5">{name}</span>}
      <span className={name ? "text-xs text-gray-500" : ""}>
        {userId} · {domain}
      </span>
    </span>
  );
}

function ListValue({ field, items, names }) {
  if (!Array.isArray(items) || items.length === 0) return blank;
  const subs = (field.fields || []).filter((s) => s.type !== "section");
  return (
    <div className="overflow-x-auto">
      <table className="text-left text-sm">
        <thead className="text-xs text-gray-500">
          <tr>
            {subs.map((s) => (
              <th key={s.name} className="py-1 pr-4 font-medium">{s.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {items.map((item, i) => (
            <tr key={i} className="border-t border-gray-100">
              {subs.map((s) => (
                <td key={s.name} className="py-1 pr-4">
                  <FieldValue field={s} values={item || {}} names={names} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FieldValue({ field, values, names }) {
  if (field.type === "user")
    return <UserValue userId={values[field.user_id_key]} domain={values[field.domain_key]} names={names} />;
  if (field.type === "list") return <ListValue field={field} items={values[field.name]} names={names} />;
  const v = values[field.name];
  if (v == null || v === "") return blank;
  if (field.type !== "checkbox" && typeof v === "object")
    return (
      <pre className="max-h-48 overflow-auto rounded border border-gray-200 bg-gray-50 p-2 text-xs">
        {JSON.stringify(v, null, 2)}
      </pre>
    );
  const text = cellValue(field, values);
  return <span className="whitespace-pre-wrap break-words">{text === "" ? blank : text}</span>;
}

/**
 * Read-only page for one entry (any of its versions). Rendered by Entries in place
 * of the search and results, so closing it leaves all filters and results as they were.
 */
export default function EntryView({
  form, entry, onClose, onUpdate, canSplit, splitKey, canPrint, labelSize, printing, onPrint, error,
}) {
  const { users } = useUsers();
  const names = useMemo(() => {
    const m = new Map();
    for (const u of users) if (u.name) m.set(`${u.user_id}\u0000${u.domain}`, u.name);
    return m;
  }, [users]);

  // Current version first, then the earlier ones (newest first).
  const versions = useMemo(
    () => [
      { ...entry, current: true },
      ...[...(entry.versions || [])].sort((a, b) => b.version - a.version),
    ],
    [entry]
  );
  const [shown, setShown] = useState(0);
  const v = versions[shown] || versions[0];
  const values = v.values || {};

  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const groups = useMemo(() => groupBySection(form.fields || []), [form]);
  const createdBy = v.created_by;

  return (
    <div className="rounded-lg bg-white shadow">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-gray-200 px-6 py-4">
        <div className="flex flex-wrap items-center gap-3">
          <button type="button" className="btn-secondary px-3 py-1.5" onClick={onClose} title="Back to entries (Esc)">
            <ArrowLeft size={16} /> Back to entries
          </button>
          <div>
            <h2 className="text-lg font-semibold text-gray-800">
              {form.title}: <span className="font-mono">{values.trial_id || entry.trial_id}</span>
              <span className="ml-2 rounded-full bg-blue-50 px-2 py-0.5 align-middle text-xs font-medium text-blue-800">
                v{v.version}
                {v.current ? "" : " (earlier version)"}
              </span>
            </h2>
            <p className="text-xs text-gray-500">Read-only view</p>
          </div>
        </div>
        <div className="flex flex-wrap gap-1.5">
          {onUpdate && (
            <>
              <button
                type="button"
                className="btn-secondary px-2.5 py-1 text-xs"
                title="Open this entry in the form to edit it and save a new version"
                onClick={() => onUpdate(form.id, entry, "update")}
              >
                <PencilLine size={13} /> Update
              </button>
              <button
                type="button"
                className="btn-secondary px-2.5 py-1 text-xs"
                title="Start a new, separate entry pre-filled with the data shown"
                onClick={() => onUpdate(form.id, { ...entry, values }, "copy")}
              >
                <Copy size={13} /> Copy
              </button>
              {canSplit && (
                <button
                  type="button"
                  className="btn-secondary px-2.5 py-1 text-xs"
                  title={`Split off part of the current version's ${splitKey} into a new entry`}
                  onClick={() => onUpdate(form.id, entry, "split")}
                >
                  <Scissors size={13} /> Split
                </button>
              )}
            </>
          )}
          {canPrint && (
            <button
              type="button"
              className="btn-secondary px-2.5 py-1 text-xs"
              title={`Print a ${labelSize} label of the version shown`}
              disabled={printing}
              onClick={() => onPrint({ ...entry, values, version: v.version })}
            >
              {printing ? <Loader2 size={13} className="animate-spin" /> : <Printer size={13} />} Print
            </button>
          )}
        </div>
      </div>

      <div className="space-y-6 p-6">
        {error && (
          <div role="alert" className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <dl className="grid gap-x-6 gap-y-2 rounded-md bg-gray-50 p-4 text-sm sm:grid-cols-2 lg:grid-cols-4">
          <div>
            <dt className="text-xs text-gray-500">Entered</dt>
            <dd className="text-gray-800">{formatTime(v.entered_at) || "—"}</dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500">Created by</dt>
            <dd className="text-gray-800">
              <UserValue userId={createdBy?.user_id} domain={createdBy?.domain} names={names} />
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500">Entry ID</dt>
            <dd className="font-mono text-gray-800" title={entry.entry_id}>{shortEntryId(entry.entry_id)}</dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500">Version</dt>
            <dd className="text-gray-800">
              {v.version}
              {versions.length > 1 && <span className="text-gray-500"> of {versions[0].version}</span>}
            </dd>
          </div>
        </dl>

        {versions.length > 1 && (
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span className="text-gray-500">Show version</span>
            <div className="flex flex-wrap gap-1 rounded-md bg-gray-100 p-0.5" role="group" aria-label="Version">
              {versions.map((ver, i) => (
                <button
                  key={ver.uuid || i}
                  type="button"
                  aria-pressed={shown === i}
                  onClick={() => setShown(i)}
                  className={`rounded px-2 py-1 font-medium ${
                    shown === i ? "bg-white text-gray-900 shadow-sm" : "text-gray-600 hover:text-gray-900"
                  }`}
                >
                  v{ver.version}
                  {ver.current ? " (current)" : ""}
                </button>
              ))}
            </div>
          </div>
        )}

        {groups.map((g, gi) => (
          <section key={g.section?.name || `g${gi}`}>
            {g.section && (
              <div className="mb-3 border-b border-gray-200 pb-1.5">
                <h3 className="text-base font-semibold text-gray-800">{g.section.label}</h3>
                {g.section.help && <p className="text-xs text-gray-500">{g.section.help}</p>}
              </div>
            )}
            {g.fields.length ? (
              <dl className="grid gap-x-8 gap-y-4 sm:grid-cols-2">
                {g.fields.map((f) => (
                  <div key={f.name} className={f.type === "list" || f.type === "json" || f.type === "textarea" ? "sm:col-span-2" : ""}>
                    <dt className="text-xs font-medium text-gray-500">{f.label}</dt>
                    <dd className="mt-0.5 text-sm text-gray-900">
                      <FieldValue field={f} values={values} names={names} />
                    </dd>
                  </div>
                ))}
              </dl>
            ) : (
              <p className="text-sm text-gray-400">No keys in this section.</p>
            )}
          </section>
        ))}
      </div>
    </div>
  );
}
