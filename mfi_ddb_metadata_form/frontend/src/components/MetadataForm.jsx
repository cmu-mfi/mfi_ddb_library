import { useEffect, useMemo, useState } from "react";
import {
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Copy,
  Eye,
  Loader2,
  PencilLine,
  RotateCcw,
  Send,
  Sparkles,
  X,
  XCircle,
} from "lucide-react";
import FieldInput from "./FieldInput";
import { previewForm, submitForm } from "../api";
import { describeAuto, indexErrors, initialValues, isUserField, toSubmitValues } from "../lib/form";

function PayloadBlock({ title, topic, payload }) {
  return (
    <div className="mt-4 overflow-hidden rounded-md border border-gray-200">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-gray-200 bg-gray-50 px-3 py-2 text-xs">
        <span className="font-medium text-gray-700">{title}</span>
        {topic && <code className="break-all text-gray-500">{topic}</code>}
      </div>
      <pre className="max-h-80 overflow-auto bg-white p-3 text-xs leading-relaxed text-gray-800">
        {JSON.stringify(payload, null, 2)}
      </pre>
    </div>
  );
}

/**
 * `editing` = {values, revises: {entry_id, version, trial_uuid}, trialId} when an
 * existing entry is being updated (from the Entries page); saving then publishes
 * the next version of that entry. `onEditDone` is called after saving or cancelling.
 */
export default function MetadataForm({ form, onSubmitted, editing = null, onEditDone }) {
  const userFields = useMemo(() => form.fields.filter(isUserField), [form]);
  const autoFields = useMemo(() => form.fields.filter((f) => !isUserField(f)), [form]);
  const startValues = () => editing?.values ?? initialValues(form.fields);

  const [values, setValues] = useState(startValues);
  const [errors, setErrors] = useState({ byPath: {}, general: [] });
  const [busy, setBusy] = useState(null); // "preview" | "submit" | null
  const [result, setResult] = useState(null);
  const [preview, setPreview] = useState(null);

  // ---- collapsible sections: each field belongs to the section heading above it ----
  const [collapsed, setCollapsed] = useState(() => new Set());
  const { sectionOf, sectionFieldCount } = useMemo(() => {
    const of = {};
    const counts = {};
    let current = null;
    for (const f of userFields) {
      if (f.type === "section") current = f.name;
      else if (current) {
        of[f.name] = current;
        counts[current] = (counts[current] || 0) + 1;
      }
    }
    return { sectionOf: of, sectionFieldCount: counts };
  }, [userFields]);
  const toggleSection = (name) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  // Open any collapsed section that holds a field with an error (incl. list rows "list.0.x").
  const revealErrors = (byPath) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      for (const path of Object.keys(byPath)) next.delete(sectionOf[path.split(".")[0]]);
      return next.size === prev.size ? prev : next;
    });

  // Reset when the form definition changes (another tab or a config reload).
  useEffect(() => {
    setCollapsed(new Set());
    setValues(startValues());
    setErrors({ byPath: {}, general: [] });
    setResult(null);
    setPreview(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [form]);

  // Starting / leaving an update. The "saved" message survives leaving after a save.
  useEffect(() => {
    setValues(startValues());
    setErrors({ byPath: {}, general: [] });
    setPreview(null);
    if (editing) setResult(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [editing]);

  const setField = (name, value) => {
    setValues((prev) => ({ ...prev, [name]: value }));
    if (errors.byPath[name]) {
      setErrors((prev) => {
        const byPath = { ...prev.byPath };
        delete byPath[name];
        return { ...prev, byPath };
      });
    }
  };

  const run = async (kind) => {
    setBusy(kind);
    setResult(null);
    setPreview(null);
    const body = toSubmitValues(form.fields, values);
    const revises = editing?.revises;
    const response =
      kind === "preview" ? await previewForm(form.id, body, revises) : await submitForm(form.id, body, revises);
    setBusy(null);

    if (!response.ok) {
      const indexed = indexErrors(response.data?.errors || [{ message: response.data?.message || "Request failed" }]);
      setErrors(indexed);
      revealErrors(indexed.byPath);
      if (kind === "submit" && response.status >= 500) {
        setResult({ ok: false, message: response.data?.message, payload: response.data?.payload });
      }
      return;
    }

    setErrors({ byPath: {}, general: [] });
    if (kind === "preview") {
      setPreview(response.data);
    } else {
      setResult({ ok: true, ...response.data });
      setValues(initialValues(form.fields));
      onSubmitted?.();
      if (editing) onEditDone?.();
    }
  };

  const reset = () => {
    setValues(startValues());
    setErrors({ byPath: {}, general: [] });
    setResult(null);
    setPreview(null);
  };

  return (
    <form
      noValidate
      onSubmit={(e) => {
        e.preventDefault();
        run("submit");
      }}
    >
      <div className="mb-5 border-b border-gray-200 pb-4">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-xl font-semibold text-gray-800">{form.title}</h2>
          <span className="rounded bg-gray-100 px-2 py-0.5 font-mono text-xs text-gray-600">msg_type: {form.msg_type}</span>
        </div>
        {form.description && <p className="mt-1 text-sm text-gray-600">{form.description}</p>}
      </div>

      {editing && (
        <div
          className={`mb-5 flex flex-wrap items-center justify-between gap-3 rounded-md border px-4 py-3 text-sm ${
            editing.revises ? "border-amber-200 bg-amber-50 text-amber-900" : "border-blue-200 bg-blue-50 text-blue-900"
          }`}
        >
          <p className="flex items-center gap-2">
            {editing.revises ? <PencilLine size={16} className="shrink-0" /> : <Copy size={16} className="shrink-0" />}
            {editing.revises ? (
              <span>
                Updating entry <span className="font-mono font-medium">{editing.trialId}</span> (version{" "}
                {editing.revises.version}). Saving creates version {editing.revises.version + 1}; the earlier
                version is kept. Pick yourself under <span className="font-medium">Created by</span>.
              </span>
            ) : (
              <span>
                New entry copied from <span className="font-mono font-medium">{editing.trialId}</span>. Saving creates
                a separate entry; the original is not changed. Enter a new{" "}
                <span className="font-medium">Trial ID</span> and pick yourself under{" "}
                <span className="font-medium">Created by</span>.
              </span>
            )}
          </p>
          <button type="button" className="btn-secondary py-1.5" disabled={Boolean(busy)} onClick={() => onEditDone?.()}>
            <X size={16} /> {editing.revises ? "Cancel update" : "Cancel copy"}
          </button>
        </div>
      )}

      <div className="grid gap-x-5 gap-y-4 sm:grid-cols-2">
        {userFields.map((field) => {
          if (field.type === "section") {
            const isCollapsed = collapsed.has(field.name);
            const count = sectionFieldCount[field.name] || 0;
            return (
              <div key={field.name} className="col-span-full mt-2 border-b border-gray-200 first:mt-0">
                <button
                  type="button"
                  aria-expanded={!isCollapsed}
                  onClick={() => toggleSection(field.name)}
                  className="group flex w-full items-center gap-2 pb-1.5 text-left"
                >
                  {isCollapsed ? (
                    <ChevronRight size={16} className="shrink-0 text-gray-400 group-hover:text-gray-700" />
                  ) : (
                    <ChevronDown size={16} className="shrink-0 text-gray-400 group-hover:text-gray-700" />
                  )}
                  <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-700">{field.label}</h3>
                  {isCollapsed && (
                    <span className="text-xs font-normal normal-case text-gray-400">
                      {count} field{count === 1 ? "" : "s"} hidden
                    </span>
                  )}
                </button>
                {field.help && !isCollapsed && <p className="-mt-1 pb-1.5 pl-6 text-xs text-gray-500">{field.help}</p>}
              </div>
            );
          }
          if (collapsed.has(sectionOf[field.name])) return null;
          return (
            <FieldInput
              key={field.name}
              field={field}
              path={field.name}
              value={values[field.name]}
              onChange={(v) => setField(field.name, v)}
              errors={errors.byPath}
            />
          );
        })}
      </div>

      {autoFields.length > 0 && (
        <div className="mt-5 flex flex-wrap items-center gap-2 text-xs text-gray-500">
          <Sparkles size={14} className="shrink-0" />
          <span>Filled in automatically:</span>
          {autoFields.map((f) => (
            <span key={f.name} className="rounded-full bg-gray-100 px-2.5 py-1">
              <span className="font-mono text-gray-700">{f.name}</span> · {describeAuto(f)}
            </span>
          ))}
        </div>
      )}

      {errors.general.length > 0 && (
        <div role="alert" className="mt-5 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {errors.general.map((msg, i) => (
            <p key={i}>{msg}</p>
          ))}
        </div>
      )}
      {Object.keys(errors.byPath).length > 0 && errors.general.length === 0 && (
        <p role="alert" className="mt-5 text-sm text-red-600">
          Please fix the highlighted fields.
        </p>
      )}

      <div className="mt-6 flex flex-wrap items-center gap-3">
        <button type="submit" className="btn-primary" disabled={Boolean(busy)}>
          {busy === "submit" ? <Loader2 size={16} className="animate-spin" /> : <Send size={16} />}
          {busy === "submit" ? "Publishing…" : editing?.revises ? `Save version ${editing.revises.version + 1}` : form.submit_label}
        </button>
        <button type="button" className="btn-secondary" disabled={Boolean(busy)} onClick={() => run("preview")}>
          {busy === "preview" ? <Loader2 size={16} className="animate-spin" /> : <Eye size={16} />}
          Preview payload
        </button>
        <button type="button" className="btn-secondary" disabled={Boolean(busy)} onClick={reset}>
          <RotateCcw size={16} /> Reset
        </button>
      </div>

      {preview && <PayloadBlock title="Payload preview (not sent)" topic={preview.topic} payload={preview.payload} />}

      {result?.ok && (
        <div
          role="status"
          className={`mt-5 rounded-md border px-4 py-3 text-sm ${
            result.status === "dry_run" ? "border-amber-200 bg-amber-50 text-amber-800" : "border-green-200 bg-green-50 text-green-800"
          }`}
        >
          <p className="flex items-center gap-2 font-medium">
            <CheckCircle2 size={16} />
            {result.status === "dry_run" ? "Validated (dry run, not published)" : "Published through the KV adapter"}
            {result.payload?.entry_version > 1 && ` as version ${result.payload.entry_version}`}
          </p>
          <p className="mt-1 break-all text-xs">
            Topic: <code>{result.topic}</code>
          </p>
          <PayloadBlock title="Sent payload" payload={result.payload} />
        </div>
      )}
      {result && !result.ok && (
        <div role="alert" className="mt-5 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          <p className="flex items-center gap-2 font-medium">
            <XCircle size={16} /> Not published
          </p>
          {result.payload && <PayloadBlock title="Payload that failed to publish" payload={result.payload} />}
        </div>
      )}
    </form>
  );
}
