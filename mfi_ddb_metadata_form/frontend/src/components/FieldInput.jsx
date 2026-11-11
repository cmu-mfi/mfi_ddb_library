import { Plus, Trash2 } from "lucide-react";
import { initialValues, isUserField } from "../lib/form";
import UserPicker from "./UserPicker";

/** Values to set when a user is picked: the ID, plus its domain field if any. */
export function pickedValues(field, user) {
  const out = { [field.name]: user.user_id };
  if (field.domain_field) out[field.domain_field] = user.domain;
  return out;
}

function Label({ field, htmlFor }) {
  return (
    <label htmlFor={htmlFor} className="mb-1 block text-sm font-medium text-gray-700">
      {field.label}
      {field.required && <span className="ml-0.5 text-red-600">*</span>}
    </label>
  );
}

function Hint({ field, error }) {
  if (error) return <p className="mt-1 text-xs text-red-600">{error}</p>;
  if (field.help) return <p className="mt-1 text-xs text-gray-500">{field.help}</p>;
  return null;
}

function Control({ field, id, value, onChange, error }) {
  const cls = `input ${error ? "input-error" : ""}`;
  const common = { id, name: id, placeholder: field.placeholder || undefined, "aria-invalid": Boolean(error) };

  switch (field.type) {
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

function ListField({ field, path, value, onChange, errors, onUserStatus }) {
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
                  siblings={item}
                  onPick={(user) =>
                    onChange(items.map((it, i) => (i === index ? { ...it, ...pickedValues(sub, user) } : it)))
                  }
                  onUserStatus={onUserStatus}
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

/** Section heading: full width, separated from the fields above it. */
function SectionHeading({ field }) {
  return (
    <div className="mt-3 border-t border-gray-200 pt-5 first:mt-0 first:border-t-0 first:pt-0 sm:col-span-2">
      <h3 className="text-base font-semibold text-gray-800">{field.label}</h3>
      {field.help && <p className="mt-0.5 text-sm text-gray-500">{field.help}</p>}
    </div>
  );
}

export default function FieldInput({
  field,
  path,
  value,
  onChange,
  errors,
  compact = false,
  siblings,
  onPick,
  onUserStatus,
}) {
  if (field.type === "section") return <SectionHeading field={field} />;
  if (field.type === "list") {
    return (
      <ListField field={field} path={path} value={value} onChange={onChange} errors={errors} onUserStatus={onUserStatus} />
    );
  }
  const id = `f-${path.replace(/\./g, "-")}`;
  const wide = !compact && ["textarea", "json", "multiselect"].includes(field.type);
  return (
    <div className={wide ? "sm:col-span-2" : ""}>
      {field.type !== "checkbox" || !compact ? <Label field={field} htmlFor={id} /> : null}
      {field.lookup === "user" && onPick ? (
        <UserPicker
          id={id}
          value={value}
          domainValue={field.domain_field ? siblings?.[field.domain_field] : undefined}
          onChange={onChange}
          onPick={onPick}
          onStatus={onUserStatus ? (status) => onUserStatus(path, status) : undefined}
          error={errors[path]}
          placeholder={field.placeholder}
        />
      ) : (
        <Control field={field} id={id} value={value} onChange={onChange} error={errors[path]} />
      )}
      <Hint field={field} error={errors[path]} />
    </div>
  );
}
