import { useEffect, useRef, useState } from "react";
import { AlertTriangle, CheckCircle2, Loader2, XCircle } from "lucide-react";
import { checkUser, searchUsers } from "../api";

/**
 * Text input for a user ID with suggestions from the registered users.
 * Picking a suggestion fills in the user ID and its domain (onPick).
 * If the directory is unavailable it behaves like a plain text box.
 */
export default function UserPicker({ id, value, domainValue, onChange, onPick, onStatus, error, placeholder }) {
  const [open, setOpen] = useState(false);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [available, setAvailable] = useState(true);
  const [active, setActive] = useState(-1);
  const [status, setStatus] = useState(null); // {registered, name} | {reserved} | null
  const searchSeq = useRef(0);

  // Tell the form whether this field currently holds an unregistered user
  // (it disables Submit); clear that when the field goes away.
  const onStatusRef = useRef(onStatus);
  onStatusRef.current = onStatus;
  useEffect(() => {
    onStatusRef.current?.(status);
  }, [status]);
  useEffect(() => () => onStatusRef.current?.(null), []);

  // Suggestions while typing (debounced).
  useEffect(() => {
    if (!open || !available) return undefined;
    const seq = ++searchSeq.current;
    setLoading(true);
    const t = setTimeout(async () => {
      const res = await searchUsers(value || "");
      if (seq !== searchSeq.current) return;
      setLoading(false);
      if (!res.ok || res.data?.available === false) {
        setAvailable(false);
        setResults([]);
        return;
      }
      setResults(res.data.users || []);
      setActive(-1);
    }, 200);
    return () => clearTimeout(t);
  }, [value, open, available]);

  // Registered or not, for the current user ID + domain.
  useEffect(() => {
    setStatus(null); // unknown until checked
    if (!available || !value || !String(value).trim()) return undefined;
    let cancelled = false;
    const t = setTimeout(async () => {
      const res = await checkUser(domainValue, String(value).trim());
      if (cancelled) return;
      if (res.ok && res.data?.reserved) {
        setStatus({ reserved: true });
        return;
      }
      if (!res.ok || res.data?.available === false) {
        setStatus(null);
        return;
      }
      setStatus({ registered: res.data.registered, name: res.data.user?.name });
    }, 400);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [value, domainValue, available]);

  const pick = (user) => {
    onPick(user);
    setOpen(false);
    setStatus({ registered: true, name: user.name });
  };

  const onKeyDown = (e) => {
    if (!open || results.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === "Enter" && active >= 0) {
      e.preventDefault();
      pick(results[active]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  const listId = `${id}-options`;

  return (
    <div className="relative">
      <input
        id={id}
        name={id}
        type="text"
        autoComplete="off"
        role="combobox"
        aria-expanded={open && results.length > 0}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-invalid={Boolean(error)}
        className={`input ${error || (status && !status.registered && !status.reserved) ? "input-error" : ""}`}
        placeholder={placeholder || (available ? "Type a name or ID…" : undefined)}
        value={value ?? ""}
        onChange={(e) => {
          onChange(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        onKeyDown={onKeyDown}
      />
      {loading && open && (
        <Loader2 size={14} className="absolute right-3 top-3 animate-spin text-gray-400" aria-hidden="true" />
      )}

      {open && available && results.length > 0 && (
        <ul
          id={listId}
          role="listbox"
          className="absolute z-20 mt-1 max-h-60 w-full overflow-auto rounded-md border border-gray-200 bg-white py-1 text-sm shadow-lg"
        >
          {results.map((u, i) => (
            <li
              key={`${u.domain}/${u.user_id}`}
              role="option"
              aria-selected={i === active}
              className={`cursor-pointer px-3 py-1.5 ${i === active ? "bg-blue-50" : "hover:bg-gray-50"}`}
              onMouseDown={(e) => {
                e.preventDefault(); // keep focus until picked
                pick(u);
              }}
            >
              <span className="font-medium text-gray-900">{u.user_id}</span>
              <span className="text-gray-500"> · {u.domain}</span>
              {u.name && <span className="block text-xs text-gray-500">{u.name}</span>}
            </li>
          ))}
        </ul>
      )}

      {!error && status?.reserved && (
        <p className="mt-1 flex items-center gap-1 text-xs text-amber-700">
          <AlertTriangle size={12} /> Reserved for platform admins - needs the admin token.
        </p>
      )}
      {!error && !status?.reserved && status?.registered && (
        <p className="mt-1 flex items-center gap-1 text-xs text-green-700">
          <CheckCircle2 size={12} /> Registered{status.name ? `: ${status.name}` : ""}
        </p>
      )}
      {!error && status && !status.reserved && !status.registered && (
        <p role="alert" className="mt-1 flex items-center gap-1 text-xs text-red-600">
          <XCircle size={12} /> Not a registered user{domainValue ? ` in ${domainValue}` : ""}. Register them first
          (New user form) or pick someone from the list.
        </p>
      )}
    </div>
  );
}
