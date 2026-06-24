import React from 'react';

export default function ServiceForm({ sectionKey, configDef, currentValues, onValueChange }) {
  return (
    <div className="p-5 bg-neutral-50 rounded-xl border border-neutral-300 border-l-4 border-l-cmu-red space-y-4 shadow-sm">
      <h3 className="text-xs font-bold uppercase tracking-wider text-neutral-700">{configDef.title}</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {configDef.fields.map((field) => {
          const value = currentValues[field.key] !== undefined ? currentValues[field.key] : field.default;

          return (
            <div key={field.key} className="flex flex-col space-y-1">
              <label htmlFor={field.key} className="text-xs font-semibold text-neutral-500 tracking-wide">
                {field.label}
              </label>
              
              {/* Conditional rendering depending on field type definitions */}
              {field.type === 'select' ? (
                <select
                  id={field.key}
                  value={value}
                  onChange={(e) => onValueChange(field.key, e.target.value)}
                  className="w-full px-3.5 py-2 bg-white border border-neutral-300 rounded-lg text-neutral-900 focus:outline-none focus:border-cmu-red text-sm transition appearance-none cursor-pointer"
                >
                  {field.options.map((option) => (
                    <option key={option} value={option}>
                      {option.charAt(0).toUpperCase() + option.slice(1)}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  id={field.key}
                  type={field.type}
                  value={value}
                  onChange={(e) => onValueChange(field.key, e.target.value)}
                  placeholder={field.default}
                  className="w-full px-3.5 py-2 bg-white border border-neutral-300 rounded-lg text-neutral-900 placeholder-neutral-400 focus:outline-none focus:border-cmu-red text-sm transition"
                />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}