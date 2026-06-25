const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  checkPort: (port) => ipcRenderer.invoke('check-port', port),
  launchPipeline: (payload) => ipcRenderer.invoke('launch-pipeline', payload),
  launchPipelineStream: (payload) => ipcRenderer.send('launch-pipeline-stream', payload),
  onDeploymentLog: (callback) => {
    const listener = (event, value) => callback(value);
    ipcRenderer.on('deployment-log', listener);
    return () => ipcRenderer.removeListener('deployment-log', listener);
  },
  onDeploymentError: (callback) => {
    const listener = (event, value) => callback(value);
    ipcRenderer.on('deployment-error', listener);
    return () => ipcRenderer.removeListener('deployment-error', listener);
  }
});