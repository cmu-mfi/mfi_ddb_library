import { useMemo, useState } from "react";
import {
  ArrowDown,
  ArrowUp,
  Copy,
  Eye,
  Heading,
  Lock,
  Loader2,
  Pencil,
  Plus,
  Save,
  Trash2,
  X,
} from "lucide-react";
import FieldInput, { pickedValues } from "./FieldInput";
import { SectionHeader, useSections } from "./Sections";
import { initialValues, isSection, isUserField } from "../lib/form";

export const KEY_TYPES = [
  { value: "text", label: "Text" },
  { value: "textarea", label: "Long text" },
  { value: "email", label: "Email" },
  { value: "number", label: "Number" },
  { value: "integer", label: "Whole number" },
  { value: "checkbox", label: "Yes / No" },
  { value: "select", label: "Dropdown (pick one)" },
  { value: "multiselect", label: "Multiple choice (pick any)" },
  { value: "date", label: "Date" },
  { value: "datetime", label: "Date & time" },
  { value: "person", label: "Person (registered user)" },
];

const HAS_CHOICES = new Set(["select", "multiselect"]);
const NO_DEFAULT = new Set(["multiselect", "person"]);

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

const isSectionRow = (row) => row.type === "section";

// ---------------------------------------------------------------------------
// Row pieces
// ---------------------------------------------------------------------------

function RowTools({ index, count, move, duplicate, remove, what }) {
  const btn = "rounded p-1.5 text-gray-500 hover:bg-gray-200 disabled:opacity-30";
  return (
    <div className="flex gap-0.5">
      <button type="button" aria-label={`Move ${what} up`} title="Move up" disabled={index === 0} onClick={() => move(-1)} className={btn}>
        <ArrowUp size={16} />
      </button>
      <button type="button" aria-label={`Move ${what} down`} title="Move down" disabled={index === count - 1} onClick={() => move(1)} className={btn}>
        <ArrowDown size={16} />
      </button>
      <button type="button" aria-label={`Duplicate ${what}`} title="Duplicate" onClick={duplicate} className={btn}>
        <Copy size={16} />
      </button>
      <button type="button" aria-label={`Remove ${what}`} title="Remove" onClick={remove}
        className="rounded p-1.5 text-gray-500 hover:bg-red-100 hover:text-red-600">
        <Trash2 size={16} />
      </button>
    </div>
  );
}

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
  const type = { number: "number", integer: "number", date: "date", email: "email" }[row.type] || "text";
  return <input {...common} type={type} placeholder={row.type === "datetime" ? "now" : ""} />;
}

function SectionRow({ row, update, tools, error }) {
  return (
    <div className={`rounded-md border border-l-4 p-4 ${error ? "border-red-300 border-l-red-400 bg-red-50/40" : "border-blue-200 border-l-blue-500 bg-blue-50/40"}`}>
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="inline-flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-blue-800">
          <Heading size={14} /> Section
        </span>
        {tools}
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <label className="block">
          <span className="mb-1 block text-xs font-medium text-gray-600">Heading</span>
          <input className="input" value={row.label} placeholder="Machine settings" onChange={(e) => update({ label: e.target.value })} />
        </label>
        <label className="block">
          <span className="mb-1 block text-xs font-medium text-gray-600">Description (optional)</span>
          <input className="input" value={row.help} placeholder="Shown under the heading" onChange={(e) => update({ help: e.target.value })} />
        </label>
      </div>
      <p className="mt-2 text-xs text-gray-500">Groups the keys below it, up to the next section. Sections are not sent as data.</p>
      {error && <p className="mt-2 text-xs text-red-600">{error}</p>}
    </div>
  );
}

function KeyRow({ row, update, tools, error }) {
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
          <div className="mb-1">{tools}</div>
        </div>

        {HAS_CHOICES.has(row.type) && <ChoicesInput row={row} update={update} error={error && row.options.length === 0} />}

        {row.type === "person" && (
          <p className="text-xs text-gray-500 sm:col-span-4">
            People pick from registered users. Sends the user ID as <code>{row.name || "key"}</code> and their domain as{" "}
            <code>{row.name || "key"}_domain</code>.
          </p>
        )}

        {!NO_DEFAULT.has(row.type) && (
          <label className="block">
            <span className="mb-1 block text-xs font-medium text-gray-600">Default value</span>
            <DefaultInput row={row} update={update} />
          </label>
        )}
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
        <label className={`block ${isNumber ? "" : NO_DEFAULT.has(row.type) ? "sm:col-span-4" : "sm:col-span-3"}`}>
          <span className="mb-1 block text-xs font-medium text-gray-600">Hint for the person filling it in</span>
          <input className="input" value={row.help} onChange={(e) => update({ help: e.target.value })} />
        </label>
      </div>
      {error && <p className="mt-2 text-xs text-red-600">{error}</p>}
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

// ---------------------------------------------------------------------------
// Preview: the form as people will see it (mirrors the backend's to_form())
// ---------------------------------------------------------------------------

function previewFields(rows) {
  const fields = [{ name: "trial_id", label: "Trial ID", type: "text", required: true, help: "Data is attached to this trial." }];
  rows.forEach((r, i) => {
    if (isSectionRow(r)) {
      if (r.label.trim()) fields.push({ name: `_section_${i}`, type: "section", label: r.label, help: r.help || null });
      return;
    }
    const name = r.name.trim() || `key_${i}`;
    const base = { name, label: r.label || name, required: r.required, help: r.help || null };
    if (r.type === "person") {
      fields.push({ ...base, type: "text", lookup: "user", domain_field: `${name}_domain` });
      fields.push({ name: `${name}_domain`, label: `${r.label || name} (domain)`, type: "text", required: r.required });
      return;
    }
    fields.push({
      ...base,
      type: r.type,
      default: r.type === "checkbox" ? r.default === "true" : r.default || null,
      options: HAS_CHOICES.has(r.type) ? r.options.map((o) => ({ value: o, label: o })) : null,
      min: r.min === "" ? null : Number(r.min),
      max: r.max === "" ? null : Number(r.max),
    });
  });
  if (rows.some(isSectionRow)) fields.push({ name: "_section_created_by", type: "section", label: "Submitted by" });
  fields.push(
    { name: "created_by_user_id", label: "Created by", type: "text", required: true, lookup: "user", domain_field: "created_by_domain" },
    { name: "created_by_domain", label: "Created by (domain)", type: "text", required: true },
  );
  return fields;
}

function FormPreview({ state }) {
  const fields = useMemo(() => previewFields(state.rows), [state.rows]);
  const [values, setValues] = useState(() => initialValues(fields));
  const shown = useMemo(() => fields.filter((f) => isSection(f) || isUserField(f)), [fields]);
  const sections = useSections(shown);
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <div className="mb-5 border-b border-gray-200 pb-4">
        <h2 className="text-xl font-semibold text-gray-800">{state.title || "Untitled form"}</h2>
        {state.description && <p className="mt-1 text-sm text-gray-600">{state.description}</p>}
      </div>
      <div className="grid gap-x-5 gap-y-4 sm:grid-cols-2">
        {shown.map((field) =>
          isSection(field) ? (
            <SectionHeader key={field.name} field={field} sections={sections} />
          ) : sections.isHidden(field) ? null : (
            <FieldInput
              key={field.name}
              field={field}
              path={field.name}
              value={values[field.name]}
              onChange={(v) => setValues((prev) => ({ ...prev, [field.name]: v }))}
              siblings={values}
              onPick={(user) => setValues((prev) => ({ ...prev, ...pickedValues(field, user) }))}
              errors={{}}
            />
          ),
        )}
      </div>
      <p className="mt-5 text-xs text-gray-500">
        Preview only: try the fields, nothing is sent. The timestamp is added when the form is submitted.
      </p>
      <button type="button" className="btn-primary mt-3" disabled>
        Submit
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Editor
// ---------------------------------------------------------------------------

/** Builder form <-> editor state */
export function toEditorState(form) {
  return {
    id: form?.id ?? null,
    title: form?.title ?? "",
    description: form?.description ?? "",
    site: form?.site ?? "",
    rows: form?.keys?.length ? form.keys.map((k) => newRow(k)) : [newRow()],
  };
}

function toRequest(state) {
  const num = (v) => (v === "" || v == null ? null : Number(v));
  return {
    title: state.title,
    description: state.description,
    site: state.site.trim() || null,
    keys: state.rows.map((r) =>
      isSectionRow(r)
        ? { type: "section", name: "", label: r.label, help: r.help }
        : {
            name: r.name.trim(),
            label: r.label,
            type: r.type,
            required: r.required,
            options: HAS_CHOICES.has(r.type) ? r.options : [],
            default: r.default === "" || NO_DEFAULT.has(r.type) ? null : String(r.default),
            help: r.help,
            min: num(r.min),
            max: num(r.max),
          },
    ),
  };
}

export default function FormEditor({ initial, defaultSite, onSave, onCancel }) {
  const [state, setState] = useState(() => toEditorState(initial));
  const [savedJson] = useState(() => JSON.stringify(toRequest(toEditorState(initial))));
  const [errors, setErrors] = useState({ title: null, rows: {}, general: [] });
  const [saving, setSaving] = useState(false);
  const [mode, setMode] = useState("edit"); // "edit" | "preview"

  const dirty = JSON.stringify(toRequest(state)) !== savedJson;
  const keyCount = state.rows.filter((r) => !isSectionRow(r)).length;
  const sectionCount = state.rows.length - keyCount;

  const setRows = (fn) => setState((s) => ({ ...s, rows: fn(s.rows) }));
  const updateRow = (i, patch) => setRows((rows) => rows.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  const moveRow = (i, d) =>
    setRows((rows) => {
      const next = [...rows];
      [next[i], next[i + d]] = [next[i + d], next[i]];
      return next;
    });
  const duplicateRow = (i) =>
    setRows((rows) => {
      // eslint-disable-next-line no-unused-vars
      const { _id, ...src } = rows[i];
      const copy = newRow({
        ...src,
        options: [...src.options],
        label: src.label ? `${src.label} (copy)` : src.label,
        name: isSectionRow(src) ? "" : src.name ? `${src.name}_copy` : "",
      });
      copy._nameTouched = !isSectionRow(src);
      return [...rows.slice(0, i + 1), copy, ...rows.slice(i + 1)];
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
    setMode("edit");
  };

  const cancel = () => {
    if (dirty && !window.confirm("Discard your changes to this form?")) return;
    onCancel();
  };

  const modeBtn = (key, label, Icon) => (
    <button
      type="button"
      onClick={() => setMode(key)}
      aria-pressed={mode === key}
      className={`inline-flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium ${
        mode === key ? "bg-white text-gray-900 shadow-sm" : "text-gray-600 hover:text-gray-900"
      }`}
    >
      <Icon size={16} /> {label}
    </button>
  );

  return (
    <div>
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <div className="flex gap-1 rounded-lg bg-gray-200 p-1">
          {modeBtn("edit", "Edit", Pencil)}
          {modeBtn("preview", "Preview", Eye)}
        </div>
        <span className="text-xs text-gray-500">
          {keyCount} key{keyCount === 1 ? "" : "s"}
          {sectionCount > 0 && ` in ${sectionCount} section${sectionCount === 1 ? "" : "s"}`}
          {dirty && <span className="ml-2 rounded-full bg-amber-100 px-2 py-0.5 font-medium text-amber-800">Unsaved changes</span>}
        </span>
      </div>

      {mode === "preview" ? (
        <FormPreview key={JSON.stringify(toRequest(state))} state={state} />
      ) : (
        <>
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
            <label className="block sm:col-span-2">
              <span className="mb-1 block text-sm font-medium text-gray-700">Description</span>
              <input
                className="input"
                value={state.description}
                placeholder="What is this form for?"
                onChange={(e) => setState({ ...state, description: e.target.value })}
              />
            </label>
          </div>

          <h3 className="mb-1 mt-6 text-base font-semibold text-gray-800">Keys and sections</h3>
          <p className="mb-3 text-sm text-gray-500">
            Each key becomes a field in the form; people filling it in enter the values, sent as a{" "}
            <code className="text-xs">data</code> payload. Use sections to group related keys under a heading.
          </p>

          <div className="space-y-3">
            <LockedRow label="Trial ID" keyName="trial_id" note="always included and required" />
            {state.rows.map((row, i) => {
              const tools = (
                <RowTools
                  index={i}
                  count={state.rows.length}
                  what={isSectionRow(row) ? "section" : "key"}
                  move={(d) => moveRow(i, d)}
                  duplicate={() => duplicateRow(i)}
                  remove={() => setRows((rows) => rows.filter((_, j) => j !== i))}
                />
              );
              const props = { row, update: (patch) => updateRow(i, patch), tools, error: errors.rows[i] };
              return isSectionRow(row) ? <SectionRow key={row._id} {...props} /> : <KeyRow key={row._id} {...props} />;
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
            <LockedRow
              label="Created by"
              keyName="created_by_user_id · created_by_domain"
              note="always included and required; picked from registered users"
            />
            <LockedRow label="Timestamp" keyName="timestamp" note="filled in automatically when sent" />
          </div>
        </>
      )}

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
        <button type="button" className="btn-secondary" onClick={cancel} disabled={saving}>
          Cancel
        </button>
      </div>
    </div>
  );
}
