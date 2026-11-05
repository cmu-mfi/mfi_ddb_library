import { Plus, RefreshCw, Trash2 } from "lucide-react";
import { initialValues, isUserField } from "../lib/form";
import { useUsers, userKey, userLabel } from "../lib/users";

function UserSelect({ field, id, value, onChange, error, common, cls }) {
  const { status, users, error: loadError, reload } = useUsers();
  const selected = userKey(value);
  const known = users.some((u) => userKey(u) === selected);
  return (
    <div className="flex gap-2">
      <select
        {...common}
        className={`${cls} min-w-0 flex-1`}
        value={selected}
        disabled={status === "loading" && users.length === 0}
        onChange={(e) => {
          const pick = users.find((u) => userKey(u) === e.target.value);
          onChange(pick ? { user_id: pick.user_id, domain: pick.domain } : null);
        }}
      >
        <option value="">
          {status === "loading" && users.length === 0
            ? "Loading users…"
            : loadError && users.length === 0
            ? "Users unavailable"
            : "Select a user…"}
        </option>
        {selected && !known && (
          <option value={selected}>
            {value.user_id} · {value.domain} (not found)
          </option>
        )}
        {users.map((u) => (
          <option key={userKey(u)} value={userKey(u)}>
            {userLabel(u)}
          </option>
        ))}
      </select>
      <button
        type="button"
        className="rounded-md border border-gray-300 px-2 text-gray-500 hover:bg-gray-50 hover:text-gray-700"
        title={loadError ? `${loadError} — retry` : "Reload users from the metadata store"}
        aria-label={`Reload users for ${field.label}`}
        onClick={reload}
      >
        <RefreshCw size={14} className={status === "loading" ? "animate-spin" : ""} />
      </button>
    </div>
  );
}

function Label({ field, htmlFor }) {
  return (
    <label htmlFor={htmlFor} className="mb-1 block text-sm font-medium text-gray-700">
      {field.label}
      {field.required && <span className="ml-0.5 text-red-600">*</span>}
    </label>
  );
}

function UserLoadError() {
  const { error } = useUsers();
  return error ? <p className="mt-1 text-xs text-amber-700">{error}</p> : null;
}

function Hint({ field, error }) {
  if (error) return <p className="mt-1 text-xs text-red-600">{error}</p>;
  if (field.type === "user") {
    return (
      <>
        <UserLoadError />
        {field.help && <p className="mt-1 text-xs text-gray-500">{field.help}</p>}
      </>
    );
  }
  if (field.help) return <p className="mt-1 text-xs text-gray-500">{field.help}</p>;
  return null;
}

function Control({ field, id, value, onChange, error }) {
  const cls = `input ${error ? "input-error" : ""}`;
  const common = { id, name: id, placeholder: field.placeholder || undefined, "aria-invalid": Boolean(error) };

  switch (field.type) {
    case "user":
      return <UserSelect field={field} id={id} value={value} onChange={onChange} error={error} common={common} cls={cls} />;
    case "textarea":
      return <textarea {...common} rows={3} className={cls} value={value ?? ""} onChange={(e) => onChange(e.target.value)} />;
    case "json":
      return (
        <textarea
          {...common}
          rows={5}
          className={`${cls} font-mono text-xs`}
          value={value ?? ""}
          onChange={(e) => onChange(e.target.value)}
        />
      );
    case "number":
    case "integer":
      return (
        <input
          {...common}
          type="number"
          step={field.type === "integer" ? 1 : "any"}
          min={field.min ?? undefined}
          max={field.max ?? undefined}
          className={cls}
          value={value ?? ""}
          onChange={(e) => onChange(e.target.value)}
        />
      );
    case "email":
      return <input {...common} type="email" className={cls} value={value ?? ""} onChange={(e) => onChange(e.target.value)} />;
    case "date":
      return <input {...common} type="date" className={cls} value={value ?? ""} onChange={(e) => onChange(e.target.value)} />;
    case "datetime":
      return (
        <input {...common} type="datetime-local" className={cls} value={value ?? ""} onChange={(e) => onChange(e.target.value)} />
      );
    case "checkbox":
      return (
        <label htmlFor={id} className="inline-flex cursor-pointer items-center gap-2 text-sm text-gray-700">
          <input
            id={id}
            type="checkbox"
            className="h-4 w-4 rounded border-gray-300 text-blue-600"
            checked={Boolean(value)}
            onChange={(e) => onChange(e.target.checked)}
          />
          {field.placeholder || "Yes"}
        </label>
      );
    case "select":
      return (
        <select {...common} className={cls} value={value ?? ""} onChange={(e) => onChange(e.target.value)}>
          <option value="">{field.placeholder || "Select…"}</option>
          {(field.options || []).map((opt) => (
            <option key={String(opt.value)} value={String(opt.value)}>
              {opt.label ?? String(opt.value)}
            </option>
          ))}
        </select>
      );
    case "multiselect": {
      const selected = new Set((value || []).map(String));
      return (
        <div id={id} className="flex flex-wrap gap-2">
          {(field.options || []).map((opt) => {
            const key = String(opt.value);
            const on = selected.has(key);
            return (
              <button
                type="button"
                key={key}
                onClick={() =>
                  onChange(on ? (value || []).filter((v) => String(v) !== key) : [...(value || []), opt.value])
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
      );
    }
    default:
      return <input {...common} type="text" className={cls} value={value ?? ""} onChange={(e) => onChange(e.target.value)} />;
  }
}

function ListField({ field, path, value, onChange, errors }) {
  const items = value || [];
  const subFields = (field.fields || []).filter(isUserField);
  const itemLabel = field.item_label || "item";
  const canAdd = field.max_items == null || items.length < field.max_items;

  const update = (index, name, v) => onChange(items.map((it, i) => (i === index ? { ...it, [name]: v } : it)));

  return (
    <div className="sm:col-span-2">
      <Label field={field} />
      <div className="space-y-3">
        {items.length === 0 && (
          <p className="rounded-md border border-dashed border-gray-300 px-3 py-4 text-center text-sm text-gray-500">
            No {itemLabel}s added.
          </p>
        )}
        {items.map((item, index) => (
          <div key={index} className="relative rounded-md border border-gray-200 bg-gray-50 p-3 pr-11">
            <div className="grid gap-3 sm:grid-cols-3">
              {subFields.map((sub) => (
                <FieldInput
                  key={sub.name}
                  field={sub}
                  path={`${path}.${index}.${sub.name}`}
                  value={item[sub.name]}
                  onChange={(v) => update(index, sub.name, v)}
                  errors={errors}
                  compact
                />
              ))}
            </div>
            <button
              type="button"
              title={`Remove ${itemLabel}`}
              aria-label={`Remove ${itemLabel} ${index + 1}`}
              onClick={() => onChange(items.filter((_, i) => i !== index))}
              className="absolute right-2 top-2 rounded p-1.5 text-gray-400 hover:bg-red-50 hover:text-red-600"
            >
              <Trash2 size={16} />
            </button>
          </div>
        ))}
        {canAdd && (
          <button
            type="button"
            className="btn-secondary py-1.5"
            onClick={() => onChange([...items, initialValues(field.fields || [])])}
          >
            <Plus size={16} /> Add {itemLabel}
          </button>
        )}
      </div>
      <Hint field={field} error={errors[path]} />
    </div>
  );
}

export default function FieldInput({ field, path, value, onChange, errors, compact = false }) {
  if (field.type === "section") {
    return (
      <div className="col-span-full mt-2 border-b border-gray-200 pb-1.5 first:mt-0">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-700">{field.label}</h3>
        {field.help && <p className="mt-0.5 text-xs text-gray-500">{field.help}</p>}
      </div>
    );
  }
  if (field.type === "list") {
    return <ListField field={field} path={path} value={value} onChange={onChange} errors={errors} />;
  }
  const id = `f-${path.replace(/\./g, "-")}`;
  const wide = !compact && ["textarea", "json", "multiselect"].includes(field.type);
  return (
    <div className={wide ? "sm:col-span-2" : ""}>
      {field.type !== "checkbox" || !compact ? <Label field={field} htmlFor={id} /> : null}
      <Control field={field} id={id} value={value} onChange={onChange} error={errors[path]} />
      <Hint field={field} error={errors[path]} />
    </div>
  );
}
