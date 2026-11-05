import { useState } from "react";
import { ArrowDown, ArrowUp, Heading, Lock, Loader2, Plus, Save, Trash2, X } from "lucide-react";

export const KEY_TYPES = [
  { value: "text", label: "Text", example: "abc" },
  { value: "textarea", label: "Long text", example: "Some notes…" },
  { value: "number", label: "Number", example: "21.5" },
  { value: "integer", label: "Whole number", example: "42" },
  { value: "checkbox", label: "Yes / No", example: "true" },
  { value: "select", label: "Dropdown", example: "" },
  { value: "date", label: "Date", example: "2026-11-05" },
  { value: "datetime", label: "Date & time", example: "2026-11-05T14:00:00Z" },
];

const keyFromLabel = (label) =>
  label
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/^(\d)/, "_$1");

let rowSeq = 0;
const newRow = (key = {}) => ({
  _id: ++rowSeq,
  _nameTouched: Boolean(key.name),
  _choice: "",
  name: "",
  label: "",
  type: "text",
  required: false,
  options: [],
  default: "",
  help: "",
  min: "",
  max: "",
  ...key,
  default: key.default ?? "",
  min: key.min ?? "",
  max: key.max ?? "",
  help: key.help ?? "",
});

function ChoicesInput({ row, update, error }) {
  const add = () => {
    const value = row._choice.trim();
    if (!value || row.options.includes(value)) return update({ _choice: "" });
    update({ options: [...row.options, value], _choice: "" });
  };
  return (
    <div className="sm:col-span-4">
      <span className="mb-1 block text-xs font-medium text-gray-600">Choices</span>
      <div className={`flex flex-wrap items-center gap-1.5 rounded-md border bg-white p-1.5 ${error ? "border-red-500" : "border-gray-300"}`}>
        {row.options.map((opt) => (
          <span key={opt} className="inline-flex items-center gap-1 rounded-full bg-blue-50 px-2.5 py-1 text-xs text-blue-800">
            {opt}
            <button
              type="button"
              aria-label={`Remove choice ${opt}`}
              className="text-blue-500 hover:text-blue-800"
              onClick={() => update({ options: row.options.filter((o) => o !== opt) })}
            >
              <X size={12} />
            </button>
          </span>
        ))}
        <input
          className="min-w-[10rem] flex-1 px-1.5 py-1 text-sm outline-none"
          placeholder={row.options.length ? "Add another…" : "Type a choice and press Enter"}
          value={row._choice}
          onChange={(e) => update({ _choice: e.target.value })}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === ",") {
              e.preventDefault();
              add();
            }
          }}
          onBlur={add}
        />
      </div>
    </div>
  );
}

function DefaultInput({ row, update }) {
  const common = { className: "input", value: row.default, onChange: (e) => update({ default: e.target.value }) };
  if (row.type === "checkbox") {
    return (
      <select {...common}>
        <option value="">No</option>
        <option value="true">Yes</option>
      </select>
    );
  }
  if (row.type === "select") {
    return (
      <select {...common}>
        <option value="">(none)</option>
        {row.options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    );
  }
  const type = { number: "number", integer: "number", date: "date" }[row.type] || "text";
  return <input {...common} type={type} placeholder={row.type === "datetime" ? "now" : ""} />;
}

function KeyRow({ row, index, count, update, move, remove, error }) {
  const isNumber = row.type === "number" || row.type === "integer";
  return (
    <div className={`rounded-md border p-4 ${error ? "border-red-300 bg-red-50/40" : "border-gray-200 bg-gray-50"}`}>
      <div className="grid gap-3 sm:grid-cols-4">
        <label className="block sm:col-span-1">
          <span className="mb-1 block text-xs font-medium text-gray-600">Label (what people see)</span>
          <input
            className="input"
            value={row.label}
            placeholder="Spindle speed"
            onChange={(e) => {
              const label = e.target.value;
              update(row._nameTouched ? { label } : { label, name: keyFromLabel(label) });
            }}
          />
        </label>
        <label className="block sm:col-span-1">
          <span className="mb-1 block text-xs font-medium text-gray-600">Key (sent in the data)</span>
          <input
            className={`input font-mono ${error ? "input-error" : ""}`}
            value={row.name}
            placeholder="spindle_speed"
            onChange={(e) => update({ name: e.target.value, _nameTouched: true })}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-xs font-medium text-gray-600">Type of value</span>
          <select className="input" value={row.type} onChange={(e) => update({ type: e.target.value, default: "" })}>
            {KEY_TYPES.map((t) => (
              <option key={t.value} value={t.value}>
                {t.label}
              </option>
            ))}
          </select>
        </label>
        <div className="flex items-end justify-between gap-2">
          <label className="mb-2 inline-flex cursor-pointer items-center gap-2 text-sm text-gray-700">
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-gray-300"
              checked={row.required}
              onChange={(e) => update({ required: e.target.checked })}
            />
            Required
          </label>
          <div className="mb-1">
            <MoveRemove index={index} count={count} move={move} remove={remove} what="key" />
          </div>
        </div>

        {row.type === "select" && <ChoicesInput row={row} update={update} error={error && row.options.length === 0} />}

        <label className="block">
          <span className="mb-1 block text-xs font-medium text-gray-600">Default value</span>
          <DefaultInput row={row} update={update} />
        </label>
        {isNumber && (
          <>
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-gray-600">Minimum</span>
              <input className="input" type="number" value={row.min} onChange={(e) => update({ min: e.target.value })} />
            </label>
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-gray-600">Maximum</span>
              <input className="input" type="number" value={row.max} onChange={(e) => update({ max: e.target.value })} />
            </label>
          </>
        )}
        <label className={`block ${isNumber ? "" : "sm:col-span-3"}`}>
          <span className="mb-1 block text-xs font-medium text-gray-600">Hint for the person filling it in</span>
          <input className="input" value={row.help} onChange={(e) => update({ help: e.target.value })} />
        </label>
      </div>
      {error && <p className="mt-2 text-xs text-red-600">{error}</p>}
    </div>
  );
}

function MoveRemove({ index, count, move, remove, what }) {
  return (
    <div className="flex gap-0.5">
      <button type="button" aria-label="Move up" disabled={index === 0} onClick={() => move(-1)}
        className="rounded p-1.5 text-gray-500 hover:bg-gray-200 disabled:opacity-30">
        <ArrowUp size={16} />
      </button>
      <button type="button" aria-label="Move down" disabled={index === count - 1} onClick={() => move(1)}
        className="rounded p-1.5 text-gray-500 hover:bg-gray-200 disabled:opacity-30">
        <ArrowDown size={16} />
      </button>
      <button type="button" aria-label={`Remove ${what}`} onClick={remove}
        className="rounded p-1.5 text-gray-500 hover:bg-red-100 hover:text-red-600">
        <Trash2 size={16} />
      </button>
    </div>
  );
}

/** A heading that groups the keys below it (until the next section). Sends no data. */
function SectionRow({ row, index, count, update, move, remove, error }) {
  return (
    <div className={`rounded-md border-l-4 p-4 ${error ? "border-red-400 bg-red-50/40" : "border-blue-500 bg-blue-50/60"}`}>
      <div className="flex flex-wrap items-start gap-3">
        <Heading size={18} className="mt-7 shrink-0 text-blue-600" />
        <label className="block min-w-[12rem] flex-1">
          <span className="mb-1 block text-xs font-medium text-gray-600">Section title</span>
          <input
            className={`input font-medium ${error ? "input-error" : ""}`}
            value={row.label}
            placeholder="Particle size"
            onChange={(e) => update({ label: e.target.value })}
          />
        </label>
        <label className="block min-w-[12rem] flex-[2]">
          <span className="mb-1 block text-xs font-medium text-gray-600">Description (optional)</span>
          <input className="input" value={row.help} onChange={(e) => update({ help: e.target.value })} />
        </label>
        <div className="mt-6">
          <MoveRemove index={index} count={count} move={move} remove={remove} what="section" />
        </div>
      </div>
      <p className="mt-2 text-xs text-gray-500">Groups the keys below it on the form. It sends no data.</p>
      {error && <p className="mt-1 text-xs text-red-600">{error}</p>}
    </div>
  );
}

function LockedRow({ label, keyName, note }) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-md border border-dashed border-gray-300 px-4 py-2.5 text-sm text-gray-600">
      <Lock size={14} className="text-gray-400" />
      <span className="font-medium text-gray-700">{label}</span>
      <code className="text-xs text-gray-500">{keyName}</code>
      <span className="text-xs text-gray-500">— {note}</span>
    </div>
  );
}

/** Builder form <-> editor state */
export function toEditorState(form) {
  return {
    id: form?.id ?? null,
    title: form?.title ?? "",
    description: form?.description ?? "",
    submit_label: form?.submit_label ?? "",
    site: form?.site ?? "",
    rows: form?.keys?.length ? form.keys.map((k) => newRow(k)) : [newRow()],
  };
}

function toRequest(state) {
  const num = (v) => (v === "" || v == null ? null : Number(v));
  return {
    title: state.title,
    description: state.description,
    submit_label: state.submit_label,
    site: state.site.trim() || null,
    keys: state.rows.map((r) =>
      r.type === "section"
        ? { name: r.name.trim() || null, label: r.label, type: "section", help: r.help }
        : {
      name: r.name.trim(),
      label: r.label,
      type: r.type,
      required: r.required,
      options: r.type === "select" ? r.options : [],
      default: r.default === "" ? null : String(r.default),
      help: r.help,
      min: num(r.min),
      max: num(r.max),
          }
    ),
  };
}

export default function FormEditor({ initial, defaultSite, onSave, onCancel }) {
  const [state, setState] = useState(() => toEditorState(initial));
  const [errors, setErrors] = useState({ title: null, rows: {}, general: [] });
  const [saving, setSaving] = useState(false);

  const setRows = (fn) => setState((s) => ({ ...s, rows: fn(s.rows) }));
  const updateRow = (i, patch) => setRows((rows) => rows.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  const moveRow = (i, d) =>
    setRows((rows) => {
      const next = [...rows];
      [next[i], next[i + d]] = [next[i + d], next[i]];
      return next;
    });

  const save = async () => {
    setSaving(true);
    const response = await onSave(toRequest(state));
    setSaving(false);
    if (response.ok) return;
    const next = { title: null, rows: {}, general: [] };
    for (const err of response.data?.errors || [{ message: response.data?.message }]) {
      const m = /^keys\.(\d+)/.exec(err.field || "");
      if (m) next.rows[m[1]] = next.rows[m[1]] ? `${next.rows[m[1]]}; ${err.message}` : err.message;
      else if (err.field === "title") next.title = err.message;
      else if (err.field === "site") next.site = err.message;
      else next.general.push(err.message);
    }
    setErrors(next);
  };

  return (
    <div>
      <div className="grid gap-4 sm:grid-cols-2">
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-gray-700">
            Form name <span className="text-red-600">*</span>
          </span>
          <input
            className={`input ${errors.title ? "input-error" : ""}`}
            value={state.title}
            placeholder="Machine setup"
            onChange={(e) => setState({ ...state, title: e.target.value })}
          />
          {errors.title && <p className="mt-1 text-xs text-red-600">{errors.title}</p>}
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-gray-700">Button text</span>
          <input
            className="input"
            value={state.submit_label}
            placeholder="Send data"
            onChange={(e) => setState({ ...state, submit_label: e.target.value })}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-gray-700">Description</span>
          <input
            className="input"
            value={state.description}
            placeholder="What is this form for?"
            onChange={(e) => setState({ ...state, description: e.target.value })}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-gray-700">Site</span>
          <input
            className={`input ${errors.site ? "input-error" : ""}`}
            value={state.site}
            placeholder={defaultSite || ""}
            onChange={(e) => setState({ ...state, site: e.target.value })}
          />
          <span className="mt-1 block text-xs text-gray-500">
            {errors.site || `Data from this form is published under this site. Empty = ${defaultSite || "the default"}.`}
          </span>
        </label>
      </div>

      <h3 className="mb-1 mt-6 text-base font-semibold text-gray-800">Keys</h3>
      <p className="mb-3 text-sm text-gray-500">
        Each key becomes a field in the form. People filling in the form enter the values, which are sent as a{" "}
        <code className="text-xs">data</code> payload. Add sections to group keys under headings.
      </p>

      <div className="space-y-3">
        <LockedRow label="Form ID" keyName="form_id" note="hidden; this form's id, used to find its entries" />
        <LockedRow label="Trial ID" keyName="trial_id" note="always included and required" />
        <LockedRow
          label="Created by"
          keyName="created_by_user_id / created_by_domain"
          note="required; picked from the registered users"
        />
        {state.rows.map((row, i) => {
          const Row = row.type === "section" ? SectionRow : KeyRow;
          return (
            <Row
              key={row._id}
              row={row}
              index={i}
              count={state.rows.length}
              update={(patch) => updateRow(i, patch)}
              move={(d) => moveRow(i, d)}
              remove={() => setRows((rows) => rows.filter((_, j) => j !== i))}
              error={errors.rows[i]}
            />
          );
        })}
        <div className="flex flex-wrap gap-2">
          <button type="button" className="btn-secondary" onClick={() => setRows((rows) => [...rows, newRow()])}>
            <Plus size={16} /> Add key
          </button>
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setRows((rows) => [...rows, newRow({ type: "section" })])}
          >
            <Heading size={16} /> Add section
          </button>
        </div>
        <LockedRow label="Timestamp" keyName="timestamp" note="filled in automatically when sent" />
      </div>

      {errors.general.length > 0 && (
        <div role="alert" className="mt-5 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {errors.general.map((m, i) => (
            <p key={i}>{m}</p>
          ))}
        </div>
      )}

      <div className="mt-6 flex flex-wrap gap-3">
        <button type="button" className="btn-primary" onClick={save} disabled={saving}>
          {saving ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
          {state.id ? "Save changes" : "Create form"}
        </button>
        <button type="button" className="btn-secondary" onClick={onCancel} disabled={saving}>
          Cancel
        </button>
      </div>
    </div>
  );
}
