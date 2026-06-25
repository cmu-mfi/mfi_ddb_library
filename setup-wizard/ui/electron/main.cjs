const { app, BrowserWindow } = require('electron');
const path = require('path');
const { spawn } = require('child_process'); // To run background processes

let mainWindow;
let backendProcess = null;

function startBackend() {
  const isDev = !app.isPackaged;
  
  if (isDev) {
    console.log("Running in development mode - make sure your backend server is running manually.");
  } else {
    const binaryName = process.platform === 'win32' ? 'main.exe' : 'main';
    
    // 🎯 Bulletproof pathing matching your exact setup-wizard location structure
    const backendPath = path.join(process.resourcesPath, 'backend-bin', binaryName);
    const backendDir = path.join(process.resourcesPath, 'backend-bin');

    console.log(`Attempting to spawn background server at: ${backendPath}`);

    // macOS Security Fix: Ensure the packaged binary has permission to execute
    if (process.platform !== 'win32') {
      try {
        const fs = require('fs');
        fs.chmodSync(backendPath, '755'); // Grants read/execute privileges natively
      } catch (err) {
        console.error("Failed to set binary execution permissions:", err);
      }
    }

    // Spawn the background backend process
    backendProcess = spawn(backendPath, [], {
      cwd: backendDir,
      env: { 
        ...process.env,
        PATH: process.env.PATH + ':/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin' // Fix to ensure binary sees Docker engine paths
      }
    });

    backendProcess.stdout.on('data', (data) => {
      console.log(`Backend Server Output: ${data}`);
    });

    backendProcess.stderr.on('data', (data) => {
      console.error(`Backend Server Error Log: ${data}`);
    });
  }
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.cjs')
    },
  });

  if (!app.isPackaged) {
    mainWindow.loadURL('http://localhost:5173');
    mainWindow.webContents.openDevTools();
  } else {
    mainWindow.loadFile(path.join(app.getAppPath(), 'dist/index.html'));
  }
}

// App Lifecycle Management
app.whenReady().then(() => {
  startBackend(); // 👈 1. Start Python engine
  createWindow(); // 👈 2. Open UI window

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

// Make sure to kill the hidden backend server when the user closes the app!
app.on('window-all-closed', () => {
  if (backendProcess) {
    backendProcess.kill(); 
  }
  if (process.platform !== 'darwin') app.quit();
});