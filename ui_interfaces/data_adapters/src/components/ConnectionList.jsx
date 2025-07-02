// ConnectionList.js
import ConnectionItem from "./ConnectionItem";

const ConnectionList = ({
  connections,
  onNewConnection,
  onEditConnection,
  onTerminateConnection,
}) => {
  const hasItems = connections.length > 0;

  return (
    <div className="connection-manager">
      <div className="connection-card">
        <div className="connection-header">
          <h1 className="header-title">Data Adapters</h1>
          <button className="new-connection-button" onClick={onNewConnection}>
            + New Adapter
          </button>
        </div>
        <div className={`connection-list ${hasItems ? "has-items" : ""}`}>
          {connections.map((connection) => (
            <ConnectionItem
              key={connection.id}
              connection={connection}
              onEdit={onEditConnection}
              onTerminate={onTerminateConnection}
            />
          ))}
          {!hasItems && (
            <span className="connection-name no-connections-message">
              No active adapter connection.
            </span>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConnectionList;
