import React from "react";
import ConnectionItem from "./ConnectionItem";

const ConnectionList = ({
  connections,
  onNewConnection,
  onEditConnection,
  onToggleExpand,
  onTerminateConnection,
}) => {
  return (
    <div className="connection-manager">
      <div className="connection-card">
        <div className="connection-header">
          <h1 className="header-title">Data Adapters</h1>
          <button className="new-connection-button" onClick={onNewConnection}>
            + New Adapter
          </button>
        </div>

        {/*
          If there are no saved connections yet, show a placeholder.
          Only render the actual list when connections.length > 0.
        */}
        {connections.length === 0 ? (
          <div className="connection-item disconnected">
            <div className="connection-main">
              <span className="connection-name">
                No active adapter connection.
              </span>
              {/* <span className="connection-status disconnected">
                Disconnected
              </span> */}
            </div>
          </div>
        ) : (
          <div className="connection-list">
            {connections.map((connection) => (
              <ConnectionItem
                key={connection.id}
                connection={connection}
                onEdit={onEditConnection}
                onToggleExpand={onToggleExpand}
                onTerminate={onTerminateConnection}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default ConnectionList;
