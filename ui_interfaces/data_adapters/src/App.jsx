import React, { useState, useEffect } from "react";
import ConnectionList from "./components/ConnectionList";
import ConnectionModal from "./components/ConnectionModal";
import "./App.css";

const App = () => {
  // load any saved connections from localStorage, or start empty
  const [connections, setConnections] = useState(() => {
    try {
      const json = localStorage.getItem("connections");
      return json ? JSON.parse(json) : [];
    } catch {
      return [];
    }
  });
  const [modalOpen, setModalOpen] = useState(false);

  // Holds data for editing mode
  const [current, setCurrent] = useState({
    isEditing: false,
    editingId: null,
    type: "",
    topicFamily: "",
    configuration: "",
    mqttConfig: "",
  });

  // whenever connections change, write them back to localStorage
  useEffect(() => {
    try {
      localStorage.setItem("connections", JSON.stringify(connections));
    } catch {}
  }, [connections]);
  const openNewModal = () => {
    setCurrent({
      isEditing: false,
      editingId: null,
      type: "",
      topicFamily: "",
      configuration: "",
    });
    setModalOpen(true);
  };

  const handleEdit = (conn) => {
    setCurrent({
      isEditing: true,
      editingId: conn.id,
      type: conn.type,
      topicFamily: conn.topicFamily,
      configuration: conn.configuration,
      mqttConfig: conn.mqttConfig || "",
    });
    setModalOpen(true);
  };

  const handleSave = (data) => {
    if (current.isEditing) {
      // Update existing
      setConnections((all) =>
        all.map((c) =>
          c.id === current.editingId
            ? {
                ...c,
                type: data.type,
                name: data.type,
                topicFamily: data.topicFamily,
                configuration: data.configuration,
                mqttConfig: data.mqttConfig,
                status: "Connected", // Add this line for edited connections
              }
            : c
        )
      );
    } else {
      // New connection
      const newConn = {
        id: data.id,
        type: data.type,
        name: data.type,
        status: "Connected",
        topicFamily: data.topicFamily,
        configuration: data.configuration,
        mqttConfig: data.mqttConfig,
        expanded: false,
        subConnections: [
          { name: `${data.type} to Broker`, status: "Connected" }, // ✅ Change these too
          { name: `${data.type} to Event Listener`, status: "Connected" }, // ✅ Change these too
        ],
      };
      setConnections((all) => [...all, newConn]);
    }
    setModalOpen(false);
  };

  const toggleExpand = (id) => {
    setConnections((all) =>
      all.map((c) => (c.id === id ? { ...c, expanded: !c.expanded } : c))
    );
  };

  const handleTerminate = (id) => {
    setConnections((all) => all.filter((c) => c.id !== id));
  };

  return (
    <div className="app">
      <ConnectionList
        connections={connections}
        onNewConnection={openNewModal}
        onEditConnection={handleEdit}
        onToggleExpand={toggleExpand}
        onTerminateConnection={handleTerminate}
      />

      <ConnectionModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        onSave={handleSave}
        initialData={{
          id: current.editingId,
          type: current.type,
          topicFamily: current.topicFamily,
          configuration: current.configuration,
          mqttConfig: current.mqttConfig,
        }}
        isEditing={current.isEditing}
      />
    </div>
  );
};

export default App;
