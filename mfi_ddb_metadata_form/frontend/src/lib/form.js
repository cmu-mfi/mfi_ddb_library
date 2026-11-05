// Helpers that turn the field definitions from /config into form state and
// back into the values sent to the backend.

const pad = (n) => String(n).padStart(2, "0");

/** Date -> value for <input type="datetime-local"> (local time, minutes). */
export function toLocalInput(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(
    date.getHours()
  )}:${pad(date.getMinutes())}`;
}

/** Fields the user fills in (auto fields that aren't editable are server-side only). */
export const isUserField = (field) => !field.auto || field.editable;

function initialValue(field) {
  if (field.type === "list") {
    const rows = Math.max(field.min_items || 0, 0);
    return Array.from({ length: rows }, () => initialValues(field.fields || []));
  }
  if (field.type === "checkbox") return Boolean(field.default);
  if (field.type === "multiselect") return Array.isArray(field.default) ? field.default : [];
  if (field.type === "datetime") {
    if (field.default === "now" || field.auto === "now") return toLocalInput(new Date());
    return field.default ? toLocalInput(new Date(field.default)) : "";
  }
  if (field.type === "json" && field.default && typeof field.default === "object") {
    return JSON.stringify(field.default, null, 2);
  }
  if (field.auto && field.editable) {
    if (field.auto === "now") return new Date().toISOString();
    return field.auto_value ?? field.default ?? "";
  }
  return field.default ?? "";
}

export function initialValues(fields) {
  const values = {};
  for (const field of fields) {
    if (isUserField(field)) values[field.name] = initialValue(field);
  }
  return values;
}

/** Form state -> request body values (datetimes become ISO 8601 UTC). */
export function toSubmitValues(fields, values) {
  const out = {};
  for (const field of fields) {
    if (!isUserField(field)) continue;
    const value = values[field.name];
    if (field.type === "datetime") {
      out[field.name] = value ? new Date(value).toISOString() : "";
    } else if (field.type === "list") {
      out[field.name] = (value || []).map((item) => toSubmitValues(field.fields || [], item));
    } else {
      out[field.name] = value;
    }
  }
  return out;
}

/** [{field, message}] -> {"path": "message"} plus messages without a field. */
export function indexErrors(errors = []) {
  const byPath = {};
  const general = [];
  for (const err of errors) {
    if (err.field) byPath[err.field] = byPath[err.field] ? `${byPath[err.field]}; ${err.message}` : err.message;
    else general.push(err.message);
  }
  return { byPath, general };
}

export function describeAuto(field) {
  switch (field.auto) {
    case "now":
      return "time of submission";
    case "uuid":
      return "generated id";
    default:
      return field.auto_value != null && field.auto_value !== "" ? String(field.auto_value) : field.auto;
  }
}
