import React from "react";

export function Card({ children, className = "" }) {
  return (
    <div className={`bg-white shadow-md p-4 rounded-md ${className}`}>
      {children}
    </div>
  );
}

export function CardContent({ children }) {
  return <div className="p-2">{children}</div>;
}
