import React, { useState } from "react";
import "./App.css";
import { DialogContent, DialogHeader, DialogTitle } from "./components/ui/dialog";

// Reusable Dialog
export function Dialog({ open, onOpenChange, children }) {
  return (
    <div
      className={`dialog-overlay ${open ? 'visible' : 'hidden'}`}
      onClick={() => onOpenChange(false)}
    >
      <div
        className="dialog-container"
        onClick={(e) => e.stopPropagation()} // Prevent event propagation
      >
        {children}
      </div>
    </div>
  );
}

function Connection({ conn, idx, toggleExpand, editConnection }) {
  return (
    <div
      className={`card-item ${
        (conn.status || "").toLowerCase() === "connected" ? "connected" : "unknown"
      }`}
      onClick={() => toggleExpand(idx)}
    >
      <div className="main-connection">
        <span>{conn.name}</span>
        <button
          className="edit-icon"
          onClick={(e) => {
            e.stopPropagation(); // Prevent toggling the dropdown
            editConnection(idx, conn.name);
          }}
        >
          ✎
        </button>
      </div>
      {conn.expanded && (
        <div className="sub-connections">
          {conn.subConnections.map((subConn, subIdx) => (
            <SubConnection key={subIdx} subConn={subConn} />
          ))}
        </div>
      )}
    </div>
  );
}

function SubConnection({ subConn }) {
  return (
    <div className="sub-connection-item">
      <span className="sub-connection-name">{subConn.name}</span>
      <span
        className={`status ${
          (subConn.status || "").toLowerCase() === "connected" ? "connected" : ""
        }`}
      >
        {subConn.status || "Unknown"}
      </span>
    </div>
  );
}

export default function ConnectionManager() {
  const [showDialog, setShowDialog] = useState(false);
  const [isConfiguring, setIsConfiguring] = useState(false);
  const [selectedType, setSelectedType] = useState("");
  const [trailId, setTrailId] = useState("");
  const [editConfig, setEditConfig] = useState("");
  const [uploadedFile, setUploadedFile] = useState(null);
  const [editingConnectionIndex, setEditingConnectionIndex] = useState(null);
  const [connections, setConnections] = useState([
    {
      name: "MTConnect",
      status: "Connected",
      expanded: false,
      subConnections: [
        { name: "MTConnect to Broker", status: "Connected" },
        { name: "MTConnect to Event Listener", status: "Connected" },
        { name: "Event Listener to Broker", status: "Connected" },
      ],
    },
  ]);

  const resetDialogState = () => {
    setShowDialog(false);
    setIsConfiguring(false);
    setSelectedType("");
    setTrailId("");
    setEditConfig("");
  };

  const handleSave = () => {
    if (!trailId) {
      alert("Trail ID is required!");
      return;
    }

    const updatedConnection = {
      name: selectedType,
      expanded: false,
      subConnections: [
        { name: `${selectedType} to Broker`, status: "Pending" },
        { name: `${selectedType} to Event Listener`, status: "Pending" },
        { name: `${selectedType} Event Listener to Broker`, status: "Pending" },
      ],
    };

    setConnections((prev) =>
      editingConnectionIndex !== null
        ? prev.map((conn, idx) =>
            idx === editingConnectionIndex ? updatedConnection : conn
          )
        : [...prev, updatedConnection]
    );

    resetDialogState();
  };

  const toggleExpand = (idx) => {
    setConnections((prev) =>
      prev.map((conn, i) =>
        i === idx ? { ...conn, expanded: !conn.expanded } : conn
      )
    );
  };

  const editConnection = (idx, name) => {
    setEditingConnectionIndex(idx);
    setSelectedType(name);
    setTrailId(""); // Populate with actual trail ID if available
    setEditConfig(""); // Populate with actual configuration if available
    setShowDialog(true);
    setIsConfiguring(true);
  };

  return (
    <div className="container">
      <div className="card">
        <div className="header">
          <h1 className="header-title">Connections</h1>
          <button
            className="button"
            onClick={() => {
              setEditingConnectionIndex(null);
              setSelectedType("");
              setTrailId("");
              setEditConfig("");
              setShowDialog(true);
              setIsConfiguring(false);
            }}
          >
            + New Connection
          </button>
        </div>
        <div className="card-content">
          {connections.map((conn, idx) => (
            <Connection
              key={idx}
              conn={conn}
              idx={idx}
              toggleExpand={toggleExpand}
              editConnection={editConnection}
            />
          ))}
        </div>
      </div>

      <Dialog
        open={showDialog}
        onOpenChange={(open) => {
          if (!open) resetDialogState();
          else setShowDialog(true);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editingConnectionIndex !== null
                ? `Edit ${selectedType} Connection`
                : isConfiguring
                ? `New ${selectedType} Connection`
                : "New Connection"}
            </DialogTitle>
            <button className="dialog-close-button" onClick={resetDialogState}>
              ✖
            </button>
          </DialogHeader>

          {!isConfiguring ? (
            <div className="select-connection-type">
              <label htmlFor="connection-type">Select Connection Type:</label>
              <select
                id="connection-type"
                className="input"
                value={selectedType}
                onChange={(e) => setSelectedType(e.target.value)}
              >
                <option value="" disabled>
                  Select a type
                </option>
                <option value="MTConnect">MTConnect</option>
                <option value="OPC UA">OPC UA</option>
                <option value="MQTT">MQTT</option>
                <option value="REST API">REST API</option>
              </select>
              <button
                className="configure-button"
                onClick={() => {
                  if (!selectedType) {
                    alert("Please select a connection type!");
                    return;
                  }
                  setIsConfiguring(true);
                }}
              >
                Configure
              </button>
            </div>
          ) : (
            <div className="text-editor">
              <label htmlFor="trail-id">Trail ID:</label>
              <input
                id="trail-id"
                className="input"
                value={trailId}
                onChange={(e) => setTrailId(e.target.value)}
                placeholder="Enter trail ID"
                required
              />
              <label htmlFor="config">Configuration:</label>
              <textarea
                id="config"
                className="input"
                value={editConfig}
                onChange={(e) => setEditConfig(e.target.value)}
                placeholder="Enter configuration"
              />
              <label htmlFor="file-upload">Upload Configuration File:</label>
              <input
                id="file-upload"
                type="file"
                className="input"
                onChange={(e) => {
                  const file = e.target.files[0];
                  setUploadedFile(file);
                  console.log("Uploaded file:", file);
                }}
              />
              <button className="save-button" onClick={handleSave}>
                Save
              </button>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
