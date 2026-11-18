// Time helpers. The API speaks ISO 8601; the UI shows local time.

/** Parse an API timestamp. Timestamps without an offset are UTC. */
export function parseTime(value) {
  if (value == null || value === "") return null;
  const s = String(value);
  const hasZone = /([zZ]|[+-]\d{2}:?\d{2})$/.test(s);
  const d = new Date(hasZone ? s : `${s}Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

const fmt = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

export function formatTime(value) {
  const d = parseTime(value);
  return d ? fmt.format(d) : "—";
}

/** Date -> value for <input type="datetime-local"> (local time, seconds). */
export function toLocalInput(date) {
  if (!date) return "";
  const pad = (n) => String(n).padStart(2, "0");
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
  );
}

/** <input type="datetime-local"> value -> ISO UTC string. */
export function fromLocalInput(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}
