import React from "react";
import "./Dialog.css"; // Import the CSS file

export function Dialog({ open, onClose, children }) {
  if (!open) return null;

  return (
    <div className="dialog-overlay">
      <div className="dialog-container">
        <button onClick={onClose} className="dialog-close-button">
          ✖
        </button>
        {children}
      </div>
    </div>
  );
}

export function DialogContent({ children }) {
  return <div className="dialog-content">{children}</div>;
}

export function DialogHeader({ children }) {
  return <h2 className="dialog-header">{children}</h2>;
}

export function DialogTitle({ children }) {
  return <h3 className="dialog-title">{children}</h3>;
}
