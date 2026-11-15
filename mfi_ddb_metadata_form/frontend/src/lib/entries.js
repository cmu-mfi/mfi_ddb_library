// Display helpers shared by the Entries table and the read-only entry view.

export const formatTime = (iso) => {
  if (!iso) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
};

/** A field's value in an entry's payload, as display text ("" = empty). */
export function cellValue(field, values) {
  if (field.type === "user") {
    const id = values[field.user_id_key];
    return id ? `${id} · ${values[field.domain_key] ?? ""}` : "";
  }
  const v = values[field.name];
  if (v == null) return "";
  if (field.type === "checkbox") return v ? "Yes" : "No";
  if (field.type === "datetime") return formatTime(v);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** First 12 letters/digits of the entry id (what the labels print). */
export const shortEntryId = (id) => String(id || "").replace(/[^A-Za-z0-9]/g, "").slice(0, 12);
