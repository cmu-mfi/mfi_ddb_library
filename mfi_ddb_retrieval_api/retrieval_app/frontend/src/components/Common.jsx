import { useEffect, useRef, useState } from "react";
import { AlertTriangle, Check, ChevronDown, ChevronRight, Copy, UserRound } from "lucide-react";

/**
 * "Requesting as": the user ID and domain sent with every request. RWS only
 * returns trials that user owns or reaches through a project role. Reports its
 * status to the app, which blocks searches while the user ID is empty.
 * With single sign-on this bar is replaced by the logged-in user.
 */
export function IdentityBar({ identity, onChange, onStatus }) {
  const set = (patch) => onChange({ ...identity, ...patch });
  const state = identity.user_id.trim() ? "set" : "empty";

  const onStatusRef = useRef(onStatus);
  onStatusRef.current = onStatus;
  useEffect(() => onStatusRef.current?.({ state }), [state]);

  return (
    <div className="flex flex-wrap items-start gap-3 rounded-lg bg-white p-4 shadow">
      <div className="flex items-center gap-2 pt-7 text-sm font-medium text-gray-700">
        <UserRound size={18} /> Requesting as
      </div>
      <label className="block min-w-[12rem] flex-[2]">
        <span className="mb-1 block text-xs font-medium text-gray-600">User ID</span>
        <input
          className="input"
          autoComplete="off"
          value={identity.user_id}
          placeholder="Your user ID, e.g. your Andrew ID"
          onChange={(e) => set({ user_id: e.target.value })}
        />
      </label>
      <label className="block min-w-[8rem] flex-1">
        <span className="mb-1 block text-xs font-medium text-gray-600">Domain</span>
        <input
          className="input"
          value={identity.user_domain}
          placeholder="ANDREW"
          onChange={(e) => set({ user_domain: e.target.value })}
        />
      </label>

      <div className="basis-full text-xs">
        {state === "empty" && <p className="text-gray-500">Enter your user ID to search.</p>}
        {state === "set" && <p className="text-gray-500">You see trials you own or reach through a project role.</p>}
      </div>
    </div>
  );
}

export function ErrorBox({ message, hint }) {
  if (!message) return null;
  return (
    <div role="alert" className="flex items-start gap-2 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      <AlertTriangle size={16} className="mt-0.5 shrink-0" />
      <div>
        <p>{message}</p>
        {hint && <p className="mt-1 text-red-600/90">{hint}</p>}
      </div>
    </div>
  );
}

export function CopyButton({ text, label = "Copy" }) {
  const [done, setDone] = useState(false);
  return (
    <button
      type="button"
      className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-gray-600 hover:bg-gray-100"
      onClick={async () => {
        try {
          await navigator.clipboard.writeText(text);
          setDone(true);
          setTimeout(() => setDone(false), 1500);
        } catch {
          /* clipboard blocked */
        }
      }}
    >
      {done ? <Check size={14} /> : <Copy size={14} />} {done ? "Copied" : label}
    </button>
  );
}

/** Collapsible pretty-printed JSON. */
export function JsonBlock({ title, value, defaultOpen = false, maxHeight = "24rem" }) {
  const [open, setOpen] = useState(defaultOpen);
  const text = JSON.stringify(value, null, 2);
  return (
    <div className="overflow-hidden rounded-md border border-gray-200">
      <div className="flex items-center justify-between gap-2 bg-gray-50 px-3 py-1.5">
        <button
          type="button"
          className="inline-flex items-center gap-1.5 text-sm font-medium text-gray-700"
          aria-expanded={open}
          onClick={() => setOpen(!open)}
        >
          {open ? <ChevronDown size={16} /> : <ChevronRight size={16} />} {title}
        </button>
        <CopyButton text={text} />
      </div>
      {open && (
        <pre className="overflow-auto bg-white p-3 text-xs leading-relaxed text-gray-800" style={{ maxHeight }}>
          {text}
        </pre>
      )}
    </div>
  );
}
