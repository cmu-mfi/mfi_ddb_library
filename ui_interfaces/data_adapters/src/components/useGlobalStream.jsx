// Create this as a new file: useGlobalStream.js
import { useState, useEffect, useRef } from "react";

// Global state to share across all components
let globalEventSource = null;
let globalSubscribers = new Set();
let globalData = [];
let globalStatus = "Not connected";

// Custom hook for shared EventSource
export function useGlobalStream() {
  const [lines, setLines] = useState(globalData);
  const [connectionStatus, setConnectionStatus] = useState(globalStatus);
  const subscriberRef = useRef(null);

  useEffect(() => {
    // Create subscriber function
    const subscriber = (newData, status) => {
      setLines([...newData]);
      setConnectionStatus(status);
    };

    subscriberRef.current = subscriber;
    globalSubscribers.add(subscriber);

    // Create EventSource only if it doesn't exist
    if (!globalEventSource) {
      console.log("🌐 Creating GLOBAL EventSource connection");

      globalEventSource = new EventSource("/config/stream");

      globalEventSource.onopen = (e) => {
        console.log("✅ GLOBAL EventSource connected");
        globalStatus = "Connected";
        notifyAllSubscribers();
      };

      globalEventSource.onmessage = (e) => {
        console.log("📨 GLOBAL Received data length:", e.data.length);

        try {
          const data = JSON.parse(e.data);
          console.log(
            "✅ GLOBAL Parsed data - Status:",
            data.status,
            "Counter:",
            data.counter
          );

          // Add to global data array
          globalData = [...globalData, data].slice(-5); // Keep last 5
          globalStatus = "Receiving data";

          // Notify all subscribers
          notifyAllSubscribers();
        } catch (error) {
          console.error("❌ GLOBAL Error parsing data:", error);
        }
      };

      globalEventSource.onerror = (error) => {
        console.error("❌ GLOBAL EventSource error:", error);
        globalStatus = "Error";
        notifyAllSubscribers();
      };
    } else {
      console.log("🔄 Using existing GLOBAL EventSource");
      // Update with current data immediately
      subscriber(globalData, globalStatus);
    }

    // Cleanup function
    return () => {
      if (subscriberRef.current) {
        globalSubscribers.delete(subscriberRef.current);
        console.log(
          `🗑️ Removed subscriber (${globalSubscribers.size} remaining)`
        );

        // Close EventSource only when no subscribers left
        if (globalSubscribers.size === 0 && globalEventSource) {
          console.log("🔌 Closing GLOBAL EventSource (no subscribers)");
          globalEventSource.close();
          globalEventSource = null;
          globalData = [];
          globalStatus = "Disconnected";
        }
      }
    };
  }, []);

  return { lines, connectionStatus };
}

// Helper function to notify all subscribers
function notifyAllSubscribers() {
  globalSubscribers.forEach((subscriber) => {
    subscriber(globalData, globalStatus);
  });
}
