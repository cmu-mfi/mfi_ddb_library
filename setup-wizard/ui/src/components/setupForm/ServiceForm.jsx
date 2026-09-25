const inputClass = 'w-full min-w-0 px-3 py-2 bg-white border border-neutral-300 rounded-lg text-neutral-900 focus:outline-none focus:border-cmu-red text-sm';

export default function ServiceForm({ configDef, currentValues, onValueChange }) {
  return (
    <section className="p-5 bg-neutral-50 rounded-xl border border-neutral-300 border-l-4 border-l-cmu-red space-y-4 shadow-sm">
      <h3 className="text-sm font-bold text-neutral-700 break-all">{configDef.title}</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {configDef.fields.map(field => {
          const value = currentValues[field.key] !== undefined ? currentValues[field.key] : field.default;
          const change = next => onValueChange(field.key, next);
          return (
            <div key={field.key} className={`flex flex-col gap-1 ${field.type === 'list' ? 'md:col-span-2' : ''}`}>
              <label id={`${field.key}-label`} htmlFor={field.key} className="text-xs font-semibold text-neutral-600 break-all">{field.label}</label>
              {field.nullable && (
                <label className="flex items-center gap-2 text-xs text-neutral-600">
                  <input type="checkbox" checked={value === null} onChange={event => change(event.target.checked ? null : '')} />
                  null (unset)
                </label>
              )}
              {field.type === 'list' ? (
                <div role="group" aria-labelledby={`${field.key}-label`} className="space-y-2">
                  {value.map((item, index) => (
                    <div key={index} className="flex items-center gap-2">
                      <input id={index === 0 ? field.key : `${field.key}-${index}`} className={inputClass}
                        aria-label={`${field.label}[${index}]`} value={item}
                        onChange={event => change(value.map((entry, i) => i === index ? event.target.value : entry))} />
                      <button type="button" className="text-xs text-red-700 p-2" aria-label={`Remove ${field.label}[${index}]`}
                        onClick={() => change(value.filter((_, i) => i !== index))}>Remove</button>
                    </div>
                  ))}
                  <button type="button" className="text-xs font-semibold text-neutral-700 border border-neutral-300 rounded px-3 py-2"
                    onClick={() => change([...value, ''])}>Add item</button>
                </div>
              ) : field.type === 'boolean' ? (
                <select id={field.key} className={inputClass} value={String(value)} onChange={event => change(event.target.value === 'true')}>
                  <option value="false">false</option><option value="true">true</option>
                </select>
              ) : (
                <input id={field.key} className={inputClass} type={field.type} value={value ?? ''} disabled={value === null}
                  step={field.type === 'number' ? 'any' : undefined}
                  onChange={event => change(field.type === 'number' && event.target.value !== '' ? Number(event.target.value) : event.target.value)} />
              )}
            </div>
          );
        })}
      </div>
    </section>
  );
}
