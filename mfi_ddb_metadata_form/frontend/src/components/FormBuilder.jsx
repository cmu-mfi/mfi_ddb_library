import { useCallback, useEffect, useState } from "react";
import { ExternalLink, KeyRound, Pencil, Plus, Trash2 } from "lucide-react";
import FormEditor, { KEY_TYPES } from "./FormEditor";
import {
  createBuilderForm,
  deleteBuilderForm,
  fetchBuilderForms,
  getAdminToken,
  setAdminToken,
  updateBuilderForm,
} from "../api";

const typeLabel = Object.fromEntries(KEY_TYPES.map((t) => [t.value, t.label]));

function TokenBar({ onChange }) {
  const [value, setValue] = useState(getAdminToken());
  return (
    <div className="mb-5 flex flex-wrap items-end gap-2 rounded-md border border-amber-200 bg-amber-50 p-3">
      <label className="block min-w-[14rem] flex-1">
        <span className="mb-1 flex items-center gap-1.5 text-xs font-medium text-amber-900">
          <KeyRound size={14} /> Admin token (needed to create, edit or delete forms)
        </span>
        <input className="input" type="password" value={value} onChange={(e) => setValue(e.target.value)} />
      </label>
      <button
        type="button"
        className="btn-secondary"
        onClick={() => {
          setAdminToken(value);
          onChange?.();
        }}
      >
        Use token
      </button>
    </div>
  );
}

export default function FormBuilder({ builder, yamlForms, onChanged, onOpenForm }) {
  const [forms, setForms] = useState([]);
  const [editing, setEditing] = useState(null); // null | "new" | form
  const [message, setMessage] = useState(null);

  const load = useCallback(async () => {
    const res = await fetchBuilderForms();
    if (res.ok && Array.isArray(res.data)) setForms(res.data);
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const handleSave = async (body) => {
    const isNew = editing === "new";
    const res = isNew ? await createBuilderForm(body) : await updateBuilderForm(editing.id, body);
    if (res.ok) {
      setMessage({ ok: true, text: `${isNew ? "Created" : "Saved"} “${res.data.form.title}”.`, id: res.data.form.id });
      setEditing(null);
      await load();
      onChanged?.();
    } else if (res.status === 401) {
      setMessage({ ok: false, text: "Admin token missing or wrong – enter it above and try again." });
    }
    return res;
  };

  const handleDelete = async (form) => {
    if (!window.confirm(`Delete the form “${form.title}”? Data already sent is not affected.`)) return;
    const res = await deleteBuilderForm(form.id);
    if (res.ok) {
      setMessage({ ok: true, text: `Deleted “${form.title}”.` });
      await load();
      onChanged?.();
    } else {
      setMessage({ ok: false, text: res.data?.message || "Delete failed." });
    }
  };

  if (!builder?.enabled) {
    return <p className="text-sm text-gray-600">The form builder is turned off in the configuration (builder.enabled).</p>;
  }

  return (
    <div>
      {builder.requires_token && <TokenBar onChange={() => setMessage(null)} />}

      {message && (
        <div
          role="status"
          className={`mb-5 flex flex-wrap items-center justify-between gap-2 rounded-md border px-4 py-3 text-sm ${
            message.ok ? "border-green-200 bg-green-50 text-green-800" : "border-red-200 bg-red-50 text-red-700"
          }`}
        >
          <span>{message.text}</span>
          {message.ok && message.id && (
            <button type="button" className="inline-flex items-center gap-1 font-medium underline" onClick={() => onOpenForm(message.id)}>
              Open form <ExternalLink size={14} />
            </button>
          )}
        </div>
      )}

      {editing ? (
        <>
          <h2 className="mb-4 text-xl font-semibold text-gray-800">{editing === "new" ? "New form" : `Edit “${editing.title}”`}</h2>
          <FormEditor
            key={editing === "new" ? "new" : editing.id}
            initial={editing === "new" ? null : editing}
            onSave={handleSave}
            onCancel={() => setEditing(null)}
          />
        </>
      ) : (
        <>
          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-gray-800">Your forms</h2>
              <p className="text-sm text-gray-500">Create a form by listing its keys; people using it fill in the values.</p>
            </div>
            <button type="button" className="btn-primary" onClick={() => { setMessage(null); setEditing("new"); }}>
              <Plus size={16} /> New form
            </button>
          </div>

          {forms.length === 0 ? (
            <div className="rounded-lg border-2 border-dashed border-gray-300 bg-gray-50 px-6 py-12 text-center">
              <p className="font-medium text-gray-600">No forms created yet.</p>
              <p className="mt-1 text-sm text-gray-400">Click “New form” to make one.</p>
            </div>
          ) : (
            <ul className="divide-y divide-gray-100 rounded-md border border-gray-200">
              {forms.map((form) => (
                <li key={form.id} className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
                  <div className="min-w-0">
                    <p className="font-medium text-gray-800">{form.title}</p>
                    <p className="mt-0.5 flex flex-wrap gap-1.5 text-xs text-gray-500">
                      {form.keys.map((k) => (
                        <span key={k.name} className="rounded bg-gray-100 px-1.5 py-0.5">
                          <code>{k.name}</code> · {typeLabel[k.type] || k.type}
                          {k.required ? " *" : ""}
                        </span>
                      ))}
                    </p>
                  </div>
                  <div className="flex gap-2">
                    <button type="button" className="btn-secondary px-3 py-1.5" onClick={() => onOpenForm(form.id)}>
                      Open
                    </button>
                    <button type="button" className="btn-secondary px-3 py-1.5" onClick={() => { setMessage(null); setEditing(form); }}>
                      <Pencil size={14} /> Edit
                    </button>
                    <button
                      type="button"
                      aria-label={`Delete ${form.title}`}
                      className="btn-secondary px-3 py-1.5 text-red-600 hover:bg-red-50"
                      onClick={() => handleDelete(form)}
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          )}

          {yamlForms.length > 0 && (
            <p className="mt-5 text-xs text-gray-500">
              Also available (defined in config/form-config.yaml, edit there): {yamlForms.map((f) => f.title).join(", ")}.
            </p>
          )}
        </>
      )}
    </div>
  );
}
