import ConnectionItem from "./ConnectionItem";

const ConnectionList = ({
  connections,
  onNewConnection,
  onEditConnection,
  onTerminateConnection,
  onPauseConnection,
  onResumeConnection,
}) => {
  const hasItems = connections.length > 0;

  return (
    <div className="p-8 max-w-5xl mx-auto">
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex justify-between items-center mb-5 pb-4 border-b-2 border-gray-200">
          <h1 className="text-3xl font-semibold text-gray-800">Data Adapters</h1>
          <div className="flex gap-3 items-center">
            <button
              className="bg-blue-600 text-white border border-blue-600 px-4 py-2.5 rounded-md text-sm font-medium cursor-pointer transition-all hover:bg-blue-700 hover:-translate-y-px"
              onClick={onNewConnection}
            >
              + New Adapter
            </button>
          </div>
        </div>

        <div className={`flex flex-col gap-4 ${hasItems ? "mt-8" : ""}`}>
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
            <div className="text-center text-gray-500 py-16 px-10 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300 mt-10">
              <span className="block text-lg font-medium text-gray-600">No active adapter connection.</span>
              <p className="mt-3 text-sm text-gray-400">
                Click <strong className="text-gray-600">"+ New Adapter"</strong> to create an adapter
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ConnectionList;
