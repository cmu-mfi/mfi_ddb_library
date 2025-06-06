import React, { useState } from "react";
import ConnectionList from "./components/ConnectionList";
import ConnectionModal from "./components/ConnectionModal";
import "./App.css";

const App = () => {
  const [connections, setConnections] = useState([]);
  const [modalOpen, setModalOpen] = useState(false);

  // Holds data for editing mode
  const [current, setCurrent] = useState({
    isEditing: false,
    editingId: null,
    type: "",
    trailId: "",
    configuration: "",
  });

  const openNewModal = () => {
    setCurrent({
      isEditing: false,
      editingId: null,
      type: "",
      trailId: "",
      configuration: "",
    });
    setModalOpen(true);
  };

  const handleEdit = (conn) => {
    setCurrent({
      isEditing: true,
      editingId: conn.id,
      type: conn.type,
      trailId: conn.trailId,
      configuration: conn.configuration,
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
                trailId: data.trailId,
                configuration: data.configuration,
              }
            : c
        )
      );
    } else {
      // New connection
      const newConn = {
        id: Date.now(),
        type: data.type,
        name: data.type,
        status: "Pending",
        trailId: data.trailId,
        configuration: data.configuration,
        expanded: false,
        subConnections: [
          { name: `${data.type} to Broker`, status: "Pending" },
          { name: `${data.type} to Event Listener`, status: "Pending" },
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
          type: current.type,
          trailId: current.trailId,
          configuration: current.configuration,
        }}
        isEditing={current.isEditing}
      />
    </div>
  );
};

export default App;
