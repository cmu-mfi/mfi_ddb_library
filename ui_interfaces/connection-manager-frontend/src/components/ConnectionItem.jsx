import React from "react";

const ConnectionItem = ({
  connection,
  onEdit,
  onToggleExpand,
  onTerminate,
}) => {
  const statusClass =
    connection.status?.toLowerCase() === "connected"
      ? "connected"
      : connection.status?.toLowerCase() === "disconnected"
      ? "disconnected"
      : "pending";

  return (
    <div className={`connection-item ${statusClass}`}>
      <div
        className="connection-main"
        onClick={() => onToggleExpand(connection.id)}
      >
        <span className="connection-name">{connection.name}</span>
        <div className="connection-actions">
          <span className={`connection-status ${statusClass}`}>
            {connection.status}
          </span>
          <button
            className="edit-button"
            onClick={(e) => {
              e.stopPropagation();
              onEdit(connection);
            }}
          >
            ✎
          </button>
          <button
            className="terminate-button"
            onClick={(e) => {
              e.stopPropagation();
              if (
                window.confirm(
                  `Are you sure you want to terminate "${connection.name}"?`
                )
              ) {
                onTerminate(connection.id);
              }
            }}
            title="Terminate Connection"
          >
            ✖
          </button>
        </div>
      </div>

      {connection.expanded && connection.subConnections && (
        <div className="sub-connections">
          {connection.subConnections.map((subConn, index) => {
            const subStatusClass =
              subConn.status?.toLowerCase() === "connected"
                ? "connected"
                : subConn.status?.toLowerCase() === "disconnected"
                ? "disconnected"
                : "pending";

            return (
              <div key={index} className="sub-connection-item">
                <span className="sub-connection-name">{subConn.name}</span>
                <span className={`sub-connection-status ${subStatusClass}`}>
                  {subConn.status}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default ConnectionItem;
