import { useCallback, useEffect, useState } from "react";
import { CheckCircle2, Loader2, MapPin, PlugZap, RotateCcw, Save, Server, XCircle } from "lucide-react";
import { TokenBar } from "./FormBuilder";
import { fetchSettings, resetSettings, saveSettings, testConnection } from "../api";

const toMqttState = (mqtt) => ({
  broker_address: mqtt?.broker_address ?? "",
  broker_port: mqtt?.broker_port ?? 1883,
  enterprise: mqtt?.enterprise ?? "",
  username: mqtt?.username ?? "",
  password: "", // write-only: blank = keep the saved password
  clearPassword: false,
});

const toSitesState = (forms) => Object.fromEntries((forms || []).map((f) => [f.id, f.override ?? ""]));

function Field({ label, hint, children }) {
  return (
    <label className="block">
      <span className="mb-1 block text-sm font-medium text-gray-700">{label}</span>
      {children}
      {hint && <span className="mt-1 block text-xs text-gray-500">{hint}</span>}
    </label>
  );
}

function Banner({ message }) {
  if (!message) return null;
  const Icon = message.ok ? CheckCircle2 : XCircle;
  return (
    <div
      role="status"
      className={`flex items-start gap-2 rounded-md border px-4 py-3 text-sm ${
        message.ok ? "border-green-200 bg-green-50 text-green-800" : "border-red-200 bg-red-50 text-red-700"
      }`}
    >
      <Icon size={16} className="mt-0.5 shrink-0" />
      <div>
        <p>{message.text}</p>
        {message.details?.length > 0 && (
          <ul className="mt-1 list-disc pl-5">
            {message.details.map((d, i) => (
              <li key={i}>{d}</li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

const errorDetails = (res) =>
  (res.data?.errors || [])
    .map((e) => (e.field ? `${e.field}: ${e.message}` : e.message))
    .filter((m) => m && m !== res.data?.message);

export default function Settings({ onChanged }) {
  const [view, setView] = useState(null);
  const [loadError, setLoadError] = useState(null);
  const [mqtt, setMqtt] = useState(toMqttState());
  const [sites, setSites] = useState({});
  const [busy, setBusy] = useState(null); // "save" | "test" | "reset"
  const [message, setMessage] = useState(null);
  const [testResult, setTestResult] = useState(null);

  const apply = (data) => {
    setView(data);
    setMqtt(toMqttState(data.mqtt));
    setSites(toSitesState(data.forms));
  };

  const load = useCallback(async () => {
    const res = await fetchSettings();
    if (!res.ok) {
      setLoadError(res.data?.message || "Could not load the settings.");
      return;
    }
    setLoadError(null);
    apply(res.data);
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const setM = (patch) => {
    setMqtt((m) => ({ ...m, ...patch }));
    setTestResult(null);
  };

  const mqttBody = (forTest = false) => {
    const body = {
      broker_address: mqtt.broker_address,
      broker_port: mqtt.broker_port === "" ? null : Number(mqtt.broker_port),
      enterprise: mqtt.enterprise,
      username: mqtt.username,
    };
    if (mqtt.password) body.password = mqtt.password;
    else if (mqtt.clearPassword && !forTest) body.password = "";
    return body;
  };

  const handleUnauthorized = (res) =>
    res.status === 401
      ? { ok: false, text: "Admin token missing or wrong – enter it above and try again." }
      : null;

  const save = async () => {
    setBusy("save");
    setMessage(null);
    const res = await saveSettings({ mqtt: mqttBody(), form_sites: sites });
    setBusy(null);
    if (res.ok) {
      apply(res.data);
      setMessage({ ok: true, text: "Settings saved. New submissions use them right away." });
      onChanged?.();
    } else {
      setMessage(
        handleUnauthorized(res) || {
          ok: false,
          text: res.data?.message || "Saving failed.",
          details: errorDetails(res),
        },
      );
    }
  };

  const reset = async () => {
    if (!window.confirm("Reset the broker settings and all form sites to the values in the configuration file?")) return;
    setBusy("reset");
    setMessage(null);
    const res = await resetSettings();
    setBusy(null);
    if (res.ok) {
      apply(res.data);
      setMessage({ ok: true, text: "Settings reset to the configuration file defaults." });
      onChanged?.();
    } else {
      setMessage(handleUnauthorized(res) || { ok: false, text: res.data?.message || "Reset failed." });
    }
  };

  const test = async () => {
    setBusy("test");
    setTestResult(null);
    const res = await testConnection(mqttBody(true));
    setBusy(null);
    if (res.ok) setTestResult({ ok: res.data.ok, text: res.data.message });
    else
      setTestResult(
        handleUnauthorized(res) || {
          ok: false,
          text: res.data?.message || "Connection test failed.",
          details: errorDetails(res),
        },
      );
  };

  if (loadError) {
    return (
      <div role="alert" className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">
        <p>{loadError}</p>
        <button type="button" className="btn-secondary mt-3 py-1.5" onClick={load}>
          Retry
        </button>
      </div>
    );
  }

  if (!view) {
    return (
      <div className="flex items-center justify-center gap-2 py-12 text-gray-500">
        <Loader2 className="animate-spin" size={18} /> Loading settings…
      </div>
    );
  }

  const d = view.defaults;
  const enterprise = mqtt.enterprise || d.enterprise;
  const overridden = new Set(view.overridden);

  return (
    <div className="space-y-8">
      {view.requires_token && (
        <TokenBar label="Admin token (needed to change settings)" onChange={() => setMessage(null)} />
      )}

      <section>
        <h2 className="flex items-center gap-2 text-xl font-semibold text-gray-800">
          <Server size={20} /> MQTT broker
        </h2>
        <p className="mt-1 text-sm text-gray-500">
          Where submissions are published. Values saved here override the configuration file.
        </p>

        <div className="mt-4 grid gap-4 sm:grid-cols-3">
          <div className="sm:col-span-2">
            <Field label="Broker address" hint={`Default: ${d.broker_address}`}>
              <input
                className="input font-mono"
                value={mqtt.broker_address}
                placeholder={d.broker_address}
                onChange={(e) => setM({ broker_address: e.target.value })}
              />
            </Field>
          </div>
          <Field label="Broker port" hint={`Default: ${d.broker_port}`}>
            <input
              className="input font-mono"
              type="number"
              min={1}
              max={65535}
              value={mqtt.broker_port}
              onChange={(e) => setM({ broker_port: e.target.value })}
            />
          </Field>
          <Field label="Enterprise" hint={`Default: ${d.enterprise}. First level of every topic.`}>
            <input
              className="input"
              value={mqtt.enterprise}
              placeholder={d.enterprise}
              onChange={(e) => setM({ enterprise: e.target.value })}
            />
          </Field>
          <Field label="Username" hint="Leave empty if the broker allows anonymous access.">
            <input
              className="input"
              autoComplete="off"
              value={mqtt.username}
              onChange={(e) => setM({ username: e.target.value })}
            />
          </Field>
          <Field
            label="Password"
            hint={
              view.mqtt.has_password
                ? "A password is saved. Leave empty to keep it."
                : "No password saved."
            }
          >
            <input
              className="input"
              type="password"
              autoComplete="new-password"
              value={mqtt.password}
              placeholder={view.mqtt.has_password ? "••••••••" : ""}
              onChange={(e) => setM({ password: e.target.value, clearPassword: false })}
            />
            {view.mqtt.has_password && overridden.has("password") && (
              <label className="mt-1.5 inline-flex cursor-pointer items-center gap-2 text-xs text-gray-600">
                <input
                  type="checkbox"
                  className="h-3.5 w-3.5 rounded border-gray-300"
                  checked={mqtt.clearPassword}
                  onChange={(e) => setM({ clearPassword: e.target.checked, password: "" })}
                />
                Remove the saved password
              </label>
            )}
          </Field>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <button type="button" className="btn-secondary" onClick={test} disabled={busy !== null}>
            {busy === "test" ? <Loader2 size={16} className="animate-spin" /> : <PlugZap size={16} />}
            Test connection
          </button>
          {testResult && (
            <span className={`inline-flex items-center gap-1.5 text-sm ${testResult.ok ? "text-green-700" : "text-red-700"}`}>
              {testResult.ok ? <CheckCircle2 size={16} /> : <XCircle size={16} />}
              {testResult.text}
            </span>
          )}
        </div>
      </section>

      <section>
        <h2 className="flex items-center gap-2 text-xl font-semibold text-gray-800">
          <MapPin size={20} /> Site per form
        </h2>
        <p className="mt-1 text-sm text-gray-500">
          Each form publishes under its own site. Leave a site empty to use the form&apos;s default.
        </p>

        <div className="mt-4 overflow-x-auto rounded-md border border-gray-200">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 text-left text-xs font-medium uppercase tracking-wide text-gray-500">
              <tr>
                <th className="px-4 py-2.5">Form</th>
                <th className="px-4 py-2.5">Site</th>
                <th className="px-4 py-2.5">Publishes to</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {view.forms.map((form) => {
                const site = sites[form.id] || form.configured_site;
                return (
                  <tr key={form.id}>
                    <td className="px-4 py-3 align-top">
                      <p className="font-medium text-gray-800">{form.title}</p>
                      <p className="text-xs text-gray-500">
                        {form.source === "ui" ? "Form builder" : "Configuration file"} · <code>{form.msg_type}</code>
                      </p>
                    </td>
                    <td className="px-4 py-3 align-top">
                      <input
                        className="input min-w-[10rem]"
                        value={sites[form.id] ?? ""}
                        placeholder={form.configured_site}
                        aria-label={`Site for ${form.title}`}
                        onChange={(e) => setSites((s) => ({ ...s, [form.id]: e.target.value }))}
                      />
                      <span className="mt-1 block text-xs text-gray-500">Default: {form.configured_site}</span>
                    </td>
                    <td className="px-4 py-3 align-top">
                      <code className="break-all text-xs text-gray-600">
                        mfi-v1.0-kv/{enterprise}/{site}/{form.msg_type}/data
                      </code>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      <Banner message={message} />

      <div className="flex flex-wrap gap-3 border-t border-gray-100 pt-5">
        <button type="button" className="btn-primary" onClick={save} disabled={busy !== null}>
          {busy === "save" ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
          Save settings
        </button>
        <button type="button" className="btn-secondary" onClick={reset} disabled={busy !== null}>
          {busy === "reset" ? <Loader2 size={16} className="animate-spin" /> : <RotateCcw size={16} />}
          Reset to defaults
        </button>
      </div>
    </div>
  );
}
