import { useEffect, useMemo, useState } from "react";
import { CheckCircle2, Eye, Loader2, RotateCcw, Send, Sparkles, XCircle } from "lucide-react";
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

export default function MetadataForm({ form, onSubmitted }) {
  const userFields = useMemo(() => form.fields.filter(isUserField), [form]);
  const autoFields = useMemo(() => form.fields.filter((f) => !isUserField(f)), [form]);

  const [values, setValues] = useState(() => initialValues(form.fields));
  const [errors, setErrors] = useState({ byPath: {}, general: [] });
  const [busy, setBusy] = useState(null); // "preview" | "submit" | null
  const [result, setResult] = useState(null);
  const [preview, setPreview] = useState(null);

  // Reset when the form definition changes (another tab or a config reload).
  useEffect(() => {
    setValues(initialValues(form.fields));
    setErrors({ byPath: {}, general: [] });
    setResult(null);
    setPreview(null);
  }, [form]);

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
    const response = kind === "preview" ? await previewForm(form.id, body) : await submitForm(form.id, body);
    setBusy(null);

    if (!response.ok) {
      const indexed = indexErrors(response.data?.errors || [{ message: response.data?.message || "Request failed" }]);
      setErrors(indexed);
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
    }
  };

  const reset = () => {
    setValues(initialValues(form.fields));
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

      <div className="grid gap-x-5 gap-y-4 sm:grid-cols-2">
        {userFields.map((field) => (
          <FieldInput
            key={field.name}
            field={field}
            path={field.name}
            value={values[field.name]}
            onChange={(v) => setField(field.name, v)}
            errors={errors.byPath}
          />
        ))}
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
          {busy === "submit" ? "Publishing…" : form.submit_label}
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
