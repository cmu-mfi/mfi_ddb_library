/**
 * Helper flag to safely detect the runtime execution context.
 * If true, we are running locally wrapped inside Electron.
 * If false, we are running in a standard web browser over a network socket.
 */
const isElectron = typeof window !== 'undefined' && 
                    window.process && 
                    window.process.type === 'renderer';

export const hostPlatform = {
  /**
   * Evaluates if a given port on the target machine is currently open/unbound.
   * @param {number} port - The TCP port number to check (e.g., 5432)
   * @returns {Promise<boolean>} True if free, false if blocked/in use
   */
  checkPort: async (port) => {
    if (isElectron) {
      // Desktop: Invoke the native Node socket layer exposed via the Electron preload bridge
      try {
        return await window.electronAPI.checkPort(Number(port));
      } catch (err) {
        console.error("Electron IPC port check crash:", err);
        return false;
      }
    } else {
      // Web Server: Make a REST API request to the lightweight Express container running on the host
      try {
        const response = await fetch(`/api/check-port?port=${port}`);
        if (!response.ok) throw new Error("Network status error response");
        const data = await response.json();
        return !!data.available;
      } catch (err) {
        console.error("Web network socket port check failure:", err);
        return false;
      }
    }
  },

  /**
   * Passes the active topology configuration vector down to the host runtime to execute 'docker compose'
   * @param {Object} payload 
   * @param {string[]} payload.selectedServices - Array of checked strings e.g. ['kv', 'blob']
   * @param {Object} payload.configs - Map of all key-value string variables from the wizard form
   * @returns {Promise<{success: boolean, log: string}>} The engine deployment result and terminal logs
   */
  launchPipeline: async (payload) => {
    if (isElectron) {
      // Desktop: Hand the configuration object directly to Electron's main process loop
      try {
        return await window.electronAPI.launchPipeline(payload);
      } catch (err) {
        return { success: false, log: `[CRITICAL DESKTOP FAIL] IPC Bridge: ${err.message}` };
      }
    } else {
      // Web Server: Submit the payload as an HTTP POST JSON block to the deployment server
      try {
        const response = await fetch('/api/launch', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          throw new Error(errorData.error || `HTTP error server code ${response.status}`);
        }
        
        return await response.json();
      } catch (err) {
        return { success: false, log: `[CRITICAL REMOTE SERVER FAIL] HTTP Post: ${err.message}` };
      }
    }
  }
};