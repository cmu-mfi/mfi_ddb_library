// ConnectionList.js
import ConnectionItem from "./ConnectionItem";

const ConnectionList = ({
  connections,
  onNewConnection,
  onEditConnection,
  onTerminateConnection,
  onRestoreConnections,
  onPauseConnection,
  onResumeConnection,
  isRestoring,
}) => {
  const hasItems = connections.length > 0;

  return (
    <div className="connection-manager">
      <div className="connection-card">
        <div className="connection-header">
          <h1 className="header-title">Data Adapters</h1>
          <div className="header-actions">
            {/* <button
              className="restore-button"
              onClick={onRestoreConnections}
              disabled={isRestoring}
              title="Reconnect all saved adapters"
            >
              {isRestoring ? "Restoring..." : "Restore All Adapter"}
            </button> */}

            <button className="new-connection-button" onClick={onNewConnection}>
              + New Adapter
            </button>
          </div>
        </div>
        <div className={`connection-list ${hasItems ? "has-items" : ""}`}>
          {connections.map((connection) => (
            <ConnectionItem
              key={connection.id}
              connection={connection}
              onEdit={onEditConnection}
              onTerminate={onTerminateConnection}
              onPause={onPauseConnection}
              onResume={onResumeConnection}
            />
          ))}
          {!hasItems && (
            <div className="connection-name no-connections-message">
              <span>No active adapter connection.</span>
              <p className="help-text">
                Click <strong>"+ New Adapter"</strong> to create an adapter
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConnectionList;
