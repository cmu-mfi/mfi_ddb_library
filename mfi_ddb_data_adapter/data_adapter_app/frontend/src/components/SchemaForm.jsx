import { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';

// ─── Schema helpers ────────────────────────────────────────────────────────────

function resolveSchema(propSchema, rootSchema) {
  if (!propSchema) return null;
  if (propSchema.$ref) {
    const path = propSchema.$ref.replace(/^#\//, '').split('/');
    let node = rootSchema;
    for (const key of path) node = node?.[key];
    return resolveSchema(node, rootSchema);
  }
  if (propSchema.anyOf) {
    const nonNull = propSchema.anyOf.filter((s) => s.type !== 'null' && s.const !== null);
    if (nonNull.length === 1) {
      const inner = resolveSchema(nonNull[0], rootSchema);
      const { anyOf: _anyOf, ...rest } = propSchema;
      return { ...rest, ...inner };
    }
  }
  return propSchema;
}

function toLabel(key) {
  return key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

// ─── Individual field renderers ────────────────────────────────────────────────

const inputCls =
  'w-full border border-gray-300 rounded-md px-3 py-2 text-gray-700 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors disabled:bg-gray-100 disabled:cursor-not-allowed';

function TextField({ value, onChange, disabled, placeholder }) {
  return (
    <input
      type="text"
      className={inputCls}
      value={value ?? ''}
      placeholder={placeholder}
      disabled={disabled}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}

function NumberField({ value, onChange, disabled, schema }) {
  return (
    <input
      type="number"
      className={inputCls}
      value={value ?? ''}
      min={schema.minimum ?? schema.exclusiveMinimum}
      max={schema.maximum ?? schema.exclusiveMaximum}
      step={schema.type === 'integer' ? 1 : 'any'}
      disabled={disabled}
      onChange={(e) => {
        const v = e.target.value;
        onChange(v === '' ? null : schema.type === 'integer' ? parseInt(v, 10) : parseFloat(v));
      }}
    />
  );
}

function BooleanField({ value, onChange, disabled, label }) {
  return (
    <label className="flex items-center gap-2 cursor-pointer select-none">
      <input
        type="checkbox"
        className="w-4 h-4 rounded border-gray-300 accent-blue-600 disabled:cursor-not-allowed"
        checked={Boolean(value)}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
      />
      <span className="text-sm text-gray-700">{label}</span>
    </label>
  );
}

function SelectField({ value, onChange, disabled, options }) {
  return (
    <select
      className={inputCls}
      value={value ?? ''}
      disabled={disabled}
      onChange={(e) => onChange(e.target.value)}
    >
      <option value="">Select…</option>
      {options.map((opt) => (
        <option key={String(opt)} value={String(opt)}>
          {String(opt)}
        </option>
      ))}
    </select>
  );
}

// ─── Recursive field renderer ──────────────────────────────────────────────────

function Field({ name, propSchema, value, onChange, disabled, rootSchema, required }) {
  const eff = resolveSchema(propSchema, rootSchema);
  if (!eff) return null;

  const label = eff.title || propSchema.title || toLabel(name);
  const description = eff.description || propSchema.description;

  const handleChange = (newVal) => onChange(name, newVal);

  let control = null;

  if (eff.enum) {
    control = <SelectField value={value} onChange={handleChange} disabled={disabled} options={eff.enum} />;
  } else if (eff.type === 'boolean') {
    control = <BooleanField value={value} onChange={handleChange} disabled={disabled} label={label} />;
  } else if (eff.type === 'integer' || eff.type === 'number') {
    control = <NumberField value={value} onChange={handleChange} disabled={disabled} schema={eff} />;
  } else if (eff.type === 'object' && eff.properties) {
    return (
      <ObjectSection
        name={name}
        schema={eff}
        value={value ?? {}}
        onChange={handleChange}
        disabled={disabled}
        rootSchema={rootSchema}
        label={label}
        description={description}
      />
    );
  } else {
    control = (
      <TextField
        value={value}
        onChange={handleChange}
        disabled={disabled}
        placeholder={eff.examples?.[0] ?? eff.default ?? ''}
      />
    );
  }

  return (
    <div className="mb-4">
      {eff.type !== 'boolean' && (
        <label className="flex items-center gap-1 mb-1 text-sm font-medium text-gray-700">
          {label}
          {required && <span className="text-red-500">*</span>}
        </label>
      )}
      {description && <p className="text-xs text-gray-400 mb-1">{description}</p>}
      {control}
    </div>
  );
}

// Nested object rendered as a collapsible section
function ObjectSection({ name, schema, value, onChange, disabled, rootSchema, label, description }) {
  const [open, setOpen] = useState(true);
  const required = schema.required ?? [];

  const handleFieldChange = (fieldName, newVal) => {
    onChange({ ...value, [fieldName]: newVal });
  };

  return (
    <div className="mb-4 border border-gray-200 rounded-md overflow-hidden">
      <button
        type="button"
        className="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 text-sm font-medium text-gray-700 hover:bg-gray-100 transition-colors text-left"
        onClick={() => setOpen((v) => !v)}
      >
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        {label}
      </button>
      {open && (
        <div className="px-3 pt-3">
          {description && <p className="text-xs text-gray-400 mb-3">{description}</p>}
          {Object.entries(schema.properties).map(([key, prop]) => (
            <Field
              key={key}
              name={key}
              propSchema={prop}
              value={value?.[key]}
              onChange={handleFieldChange}
              disabled={disabled}
              rootSchema={rootSchema}
              required={required.includes(key)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Main SchemaForm ───────────────────────────────────────────────────────────

export default function SchemaForm({ schema, value = {}, onChange, disabled = false }) {
  const [showAdvanced, setShowAdvanced] = useState(false);

  if (!schema?.properties) {
    return <p className="text-sm text-gray-400 italic">No schema available for this adapter.</p>;
  }

  const required = schema.required ?? [];
  const allKeys = Object.keys(schema.properties);
  const requiredKeys = allKeys.filter((k) => required.includes(k));
  const optionalKeys = allKeys.filter((k) => !required.includes(k));

  const handleFieldChange = (name, newVal) => {
    onChange({ ...value, [name]: newVal });
  };

  const renderFields = (keys) =>
    keys.map((key) => (
      <Field
        key={key}
        name={key}
        propSchema={schema.properties[key]}
        value={value[key]}
        onChange={handleFieldChange}
        disabled={disabled}
        rootSchema={schema}
        required={required.includes(key)}
      />
    ));

  return (
    <div>
      {renderFields(requiredKeys)}

      {optionalKeys.length > 0 && (
        <div className="mt-2">
          <button
            type="button"
            className="flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800 font-medium mb-3"
            onClick={() => setShowAdvanced((v) => !v)}
          >
            {showAdvanced ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {showAdvanced ? 'Hide advanced' : `Show advanced (${optionalKeys.length} optional field${optionalKeys.length > 1 ? 's' : ''})`}
          </button>
          {showAdvanced && renderFields(optionalKeys)}
        </div>
      )}
    </div>
  );
}
