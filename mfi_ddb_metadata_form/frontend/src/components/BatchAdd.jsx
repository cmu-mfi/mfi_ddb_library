import { useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ClipboardPaste,
  Download,
  FileSpreadsheet,
  Loader2,
  RotateCcw,
  Search,
  Send,
  Upload,
  XCircle,
} from "lucide-react";
import FieldInput from "./FieldInput";
import { checkBatch, submitForm } from "../api";

const UNSUPPORTED = new Set(["list", "json", "multiselect", "section"]);

/** Fields a table column can fill (same rules as the backend). Created by is picked once. */
export const batchFields = (form) =>
  form.fields.filter(
    (f) => !UNSUPPORTED.has(f.type) && f.name !== "created_by" && !(f.auto && !f.editable)
  );

function downloadTemplate(form) {
  const header = batchFields(form).map((f) => f.name).join(",");
  const blob = new Blob(["﻿" + header + "\n"], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  Object.assign(document.createElement("a"), { href: url, download: `${form.id}-batch-template.csv` }).click();
  URL.revokeObjectURL(url);
}

const readFile = (file) =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    const isExcel = /\.xlsx$/i.test(file.name);
    reader.onerror = () => reject(new Error("Could not read the file."));
    reader.onload = () =>
      resolve(
        isExcel
          ? { format: "xlsx", content: String(reader.result).split(",")[1] || "", name: file.name }
          : { format: "csv", content: String(reader.result), name: file.name }
      );
    if (isExcel) reader.readAsDataURL(file);
    else reader.readAsText(file);
  });

export default function BatchAdd({ form, onSubmitted, onShowEntries }) {
  const userField = useMemo(() => form.fields.find((f) => f.name === "created_by" && f.type === "user"), [form]);
  const fields = useMemo(() => batchFields(form), [form]);
  const required = fields.filter((f) => f.required);

  const [createdBy, setCreatedBy] = useState(null);
  const [source, setSource] = useState(null); // {format, content, name}
  const [pasted, setPasted] = useState("");
  const [checking, setChecking] = useState(false);
  const [check, setCheck] = useState(null);
  const [requestError, setRequestError] = useState(null);
  const [published, setPublished] = useState(() => new Set()); // spreadsheet row numbers
  const [publishing, setPublishing] = useState(false);
  const [publishError, setPublishError] = useState(null);
  const fileInput = useRef(null);

  // a different form starts from scratch
  useEffect(() => {
    setCreatedBy(null);
    setSource(null);
    setPasted("");
    setCheck(null);
    setRequestError(null);
    setPublished(new Set());
    setPublishError(null);
  }, [form]);

  // (re)check whenever the table or Created by changes
  useEffect(() => {
    if (!source) return;
    let cancelled = false;
    (async () => {
      setChecking(true);
      setRequestError(null);
      setPublished(new Set());
      setPublishError(null);
      const res = await checkBatch(form.id, {
        format: source.format,
        content: source.content,
        created_by: createdBy,
      });
      if (cancelled) return;
      setChecking(false);
      if (res.ok) setCheck(res.data);
      else {
        setCheck(null);
        setRequestError(res.data?.message || "The table could not be checked.");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [source, createdBy, form.id]);

  const onFile = async (file) => {
    if (!file) return;
    if (!/\.(csv|txt|xlsx)$/i.test(file.name)) {
      setRequestError("Choose a .csv or .xlsx file.");
      return;
    }
    try {
      setSource(await readFile(file));
    } catch (err) {
      setRequestError(err.message);
    }
  };

  const rows = check?.rows || [];
  const columns = (check?.columns || []).filter((c) => c.field);
  const remaining = rows.filter((r) => !published.has(r.row));
  const allDone = rows.length > 0 && remaining.length === 0;

  const publish = async () => {
    setPublishing(true);
    setPublishError(null);
    const done = new Set(published);
    for (const row of remaining) {
      const res = await submitForm(form.id, row.values);
      if (!res.ok) {
        const msg = res.data?.errors?.map((e) => e.message).join("; ") || res.data?.message || "Publishing failed.";
        setPublishError(`Row ${row.row}: ${msg}`);
        break;
      }
      done.add(row.row);
      setPublished(new Set(done));
    }
    setPublishing(false);
    onSubmitted?.();
  };

  const startOver = () => {
    setSource(null);
    setPasted("");
    setCheck(null);
    setRequestError(null);
    setPublished(new Set());
    setPublishError(null);
    if (fileInput.current) fileInput.current.value = "";
  };

  return (
    <div className="space-y-6">
      <div className="border-b border-gray-200 pb-4">
        <h2 className="text-xl font-semibold text-gray-800">{form.title}: batch add</h2>
        <p className="mt-1 text-sm text-gray-600">
          Add many entries at once from a table. Column titles are the form's keys (or labels); each row becomes one
          entry. Every row is checked first, and nothing is sent unless all rows pass.
        </p>
      </div>

      {/* 1. who + template */}
      <div className="grid gap-5 sm:grid-cols-2">
        {userField && (
          <FieldInput
            field={{ ...userField, help: "Applies to every row in the table." }}
            path="created_by"
            value={createdBy}
            onChange={setCreatedBy}
            errors={{}}
          />
        )}
        <div>
          <p className="mb-1 text-sm font-medium text-gray-700">Columns</p>
          <p className="text-xs text-gray-600">
            Required:{" "}
            {required.map((f, i) => (
              <span key={f.name}>
                {i > 0 && ", "}
                <code className="text-gray-800">{f.name}</code>
              </span>
            ))}
            {required.length === 0 && "none"}
            . Optional: {fields.filter((f) => !f.required).map((f) => f.name).join(", ") || "none"}.
          </p>
          <button type="button" className="btn-secondary mt-2 px-3 py-1.5" onClick={() => downloadTemplate(form)}>
            <Download size={14} /> Download template (.csv)
          </button>
        </div>
      </div>

      {/* 2. the table */}
      <div className="grid gap-5 lg:grid-cols-2">
        <div
          className="flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed border-gray-300 p-6 text-center"
          onDragOver={(e) => e.preventDefault()}
          onDrop={(e) => {
            e.preventDefault();
            onFile(e.dataTransfer.files?.[0]);
          }}
        >
          <FileSpreadsheet size={28} className="text-gray-400" />
          <p className="text-sm text-gray-600">Drop a .csv or .xlsx file here</p>
          <input
            ref={fileInput}
            type="file"
            accept=".csv,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            className="hidden"
            onChange={(e) => onFile(e.target.files?.[0])}
          />
          <button type="button" className="btn-secondary px-3 py-1.5" onClick={() => fileInput.current?.click()}>
            <Upload size={14} /> Choose file
          </button>
          {source?.name && <p className="text-xs text-gray-500">Loaded: {source.name}</p>}
        </div>
        <div>
          <label htmlFor="batch-paste" className="mb-1 block text-sm font-medium text-gray-700">
            …or paste cells from Excel / Sheets (include the title row)
          </label>
          <textarea
            id="batch-paste"
            rows={5}
            className="input font-mono text-xs"
            placeholder={"trial_id\talloy\tvendor\np-1\t17-4 PH\tEOS"}
            value={pasted}
            onChange={(e) => setPasted(e.target.value)}
          />
          <button
            type="button"
            className="btn-secondary mt-2 px-3 py-1.5"
            disabled={!pasted.trim()}
            onClick={() => setSource({ format: "tsv", content: pasted, name: "pasted cells" })}
          >
            <ClipboardPaste size={14} /> Check pasted cells
          </button>
        </div>
      </div>

      {requestError && (
        <div role="alert" className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {requestError}
        </div>
      )}

      {/* 3. results */}
      {checking && (
        <p className="flex items-center gap-2 text-sm text-gray-500">
          <Loader2 size={16} className="animate-spin" /> Checking the table…
        </p>
      )}

      {check && !checking && (
        <div className="space-y-4">
          <div
            className={`flex flex-wrap items-center gap-2 rounded-md border px-4 py-3 text-sm ${
              check.ok ? "border-green-200 bg-green-50 text-green-800" : "border-amber-200 bg-amber-50 text-amber-900"
            }`}
          >
            {check.ok ? <CheckCircle2 size={16} /> : <AlertTriangle size={16} />}
            {check.ok
              ? `All ${check.count} row${check.count === 1 ? "" : "s"} passed the checks.`
              : `${check.count - check.bad_rows} of ${check.count} rows OK` +
                (check.bad_rows ? `, ${check.bad_rows} with problems` : "") +
                (check.errors.length ? `; ${check.errors.length} problem(s) with the table` : "") +
                ". Fix them and load the table again - nothing is sent until everything passes."}
          </div>

          {check.errors.length > 0 && (
            <ul className="list-disc space-y-1 rounded-md border border-red-200 bg-red-50 py-3 pl-8 pr-4 text-sm text-red-700">
              {check.errors.map((e, i) => (
                <li key={i}>{e.message}</li>
              ))}
            </ul>
          )}

          {rows.length > 0 && (
            <div className="max-h-[28rem] overflow-auto rounded-md border border-gray-200">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 border-b border-gray-200 bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
                  <tr>
                    <th className="px-3 py-2 font-medium">Row</th>
                    {columns.map((c) => (
                      <th key={c.field} className="whitespace-nowrap px-3 py-2 font-medium" title={c.header}>
                        {c.label}
                      </th>
                    ))}
                    <th className="px-3 py-2 font-medium">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => {
                    const bad = new Set(r.errors.map((e) => e.field));
                    const isPublished = published.has(r.row);
                    return (
                      <tr key={r.row} className={`border-b border-gray-100 ${r.errors.length ? "bg-red-50/60" : ""}`}>
                        <td className="px-3 py-1.5 text-xs text-gray-500">{r.row}</td>
                        {columns.map((c) => (
                          <td
                            key={c.field}
                            className={`max-w-[12rem] truncate px-3 py-1.5 ${
                              bad.has(c.field) ? "font-medium text-red-700 underline decoration-red-400" : ""
                            }`}
                          >
                            {String(r.values[c.field] ?? "")}
                          </td>
                        ))}
                        <td className="px-3 py-1.5 text-xs">
                          {r.errors.length ? (
                            <span className="text-red-700">{r.errors.map((e) => e.message).join("; ")}</span>
                          ) : isPublished ? (
                            <span className="inline-flex items-center gap-1 text-green-700">
                              <CheckCircle2 size={12} /> Published
                            </span>
                          ) : (
                            <span className="text-gray-500">OK</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {publishError && (
            <div role="alert" className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              <p className="flex items-center gap-2 font-medium">
                <XCircle size={16} /> Stopped after {published.size} of {rows.length} entries were published.
              </p>
              <p className="mt-1">{publishError}</p>
              <p className="mt-1 text-xs">
                The {published.size} published entr{published.size === 1 ? "y stays" : "ies stay"} saved; Retry sends
                only the rest.
              </p>
            </div>
          )}

          {allDone ? (
            <div className="flex flex-wrap items-center gap-3 rounded-md border border-green-200 bg-green-50 px-4 py-3 text-sm text-green-800">
              <CheckCircle2 size={16} /> Published {rows.length} new entr{rows.length === 1 ? "y" : "ies"}.
              {onShowEntries && (
                <button type="button" className="btn-secondary px-3 py-1.5" onClick={onShowEntries}>
                  <Search size={14} /> View in Entries
                </button>
              )}
              <button type="button" className="btn-secondary px-3 py-1.5" onClick={startOver}>
                <RotateCcw size={14} /> Add another batch
              </button>
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-3">
              <button type="button" className="btn-primary" disabled={!check.ok || publishing} onClick={publish}>
                {publishing ? <Loader2 size={16} className="animate-spin" /> : <Send size={16} />}
                {publishing
                  ? `Publishing ${published.size + 1} / ${rows.length}…`
                  : publishError
                  ? `Retry remaining ${remaining.length}`
                  : `Publish ${rows.length} entr${rows.length === 1 ? "y" : "ies"}`}
              </button>
              <button type="button" className="btn-secondary" disabled={publishing} onClick={startOver}>
                <RotateCcw size={16} /> Start over
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
