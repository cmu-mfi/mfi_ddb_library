import React from "react";

export function Select({ value, onChange, children }) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="border p-2 rounded w-full"
    >
      {children}
    </select>
  );
}

export function SelectTrigger({ children }) {
  return <div className="p-2 border rounded">{children}</div>;
}

export function SelectContent({ children }) {
  return <div className="border rounded mt-1">{children}</div>;
}

export function SelectItem({ value, children }) {
  return <option value={value}>{children}</option>;
}

export function SelectValue({ placeholder }) {
  return <span className="text-gray-500">{placeholder}</span>;
}
