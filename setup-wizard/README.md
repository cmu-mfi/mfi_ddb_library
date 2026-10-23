# MFI DDB Setup Wizard

The Setup Wizard helps you choose Digital Data Backbone (DDB) services, enter
their settings, and start them using Docker. You can also download a copy of your
settings from the app.

## Before you start

- Install [Docker with Docker Compose](https://docs.docker.com/get-started/get-docker/)
  and make sure it is running. If you use Docker Desktop on Windows, select Linux
  containers.
- Have an internet connection so Docker can download the required service images.
- Have the addresses, ports, usernames, and passwords for the services you want
  to connect to.

The packaged app includes what it needs to run. You do not need to install Python
or Node.js, or download this project's source code.

## Download and open the app

Download the MFI DDB Setup Wizard package shared with you for your operating system.

| Operating system | Package | How to open it |
| --- | --- | --- |
| macOS | `.dmg` | Open the file, drag the app into Applications, and launch it. |
| Windows | `.exe` | Run the installer, follow the prompts, and launch the installed app. |
| Linux | `.AppImage` | Allow the file to run as a program in its file permissions, then open it. |

If you receive a `.zip` package, extract it first, then open the installer or app
inside it.

## Import configurations or start fresh

1. Open the app and click **Get Started**.
2. When asked **Import saved configurations?**, choose one of these options:

   - **Import Folder:** select a folder containing saved YAML configurations.
   - **Upload ZIP:** select a configuration ZIP exported by the wizard.
   - **No, configure manually:** continue with the usual setup pages.

Keep the service folders and YAML filenames together when importing. For example,
select the folder created when you extract an exported configuration ZIP. Each
service needs its complete set of YAML files. The app shows an error if files
are missing, invalid, or duplicated, so you can choose another folder or ZIP.

## Choose services and review settings

1. Select the services you want to run. Services with imported YAML files are
   selected automatically. Confirm any other services you need, such as the
   **MQTT Broker Node** or **Data Adapter App**.
2. Click **Configure Parameters**.
3. Review each configuration page. Imported values are already filled in; you
   can edit them before launching. If you skipped importing, enter your settings
   in the usual forms. Use **Next
   Configuration** to continue or **Previous Configuration** to go back. Your
   entries are kept as you move between pages.

## Export your configurations

On the final configuration page, click **Export YAML (ZIP)** to download
`mfi-ddb-configurations.zip`. It contains only the actual YAML configuration
files for your selected services, organized in their service folders.

The save dialog starts in your **Downloads** folder. You can choose a different
location. Extract the ZIP to read the individual YAML files, or keep the ZIP to
import during a future setup.

Saving a copy is optional. You can export your settings without starting Docker
services, and your entries stay in the wizard so you can continue with setup.
Services without YAML configuration inputs have no files to export. If you select
only those services, the export button is disabled.

## Start the services with Docker

1. Make sure Docker is running, then click **Launch DDB Pipeline** on the final
   configuration page.
2. The wizard applies your settings and uses Docker Compose to download any
   missing service images and start the selected containers.
3. Watch the **Orchestration Progress Monitor** for download and startup updates.
   The first launch can take longer while images are being downloaded.
4. Wait for the progress bar to reach **100%** and the success message to appear.

The wizard runs the Docker commands for you. You can also see the running
containers in Docker Desktop or your usual Docker management tool.

## Open the Data Adapter App

After a successful deployment, click **Continue to Dashboard** to open the
Data Adapter App in your browser. This button appears when you selected
**Data Adapter App** during setup. The default desktop address is
`http://localhost:3001/`.

Closing the wizard leaves the Docker services running. Use Docker Desktop or
your usual Docker management tool to stop them when you are finished.

## If setup fails

Read the error shown in the progress monitor. Check that Docker is running, your
settings are correct, and the required ports are available. Use **Adjust
Configurations** to change your entries, or **Retry Deployment** to try again.

## Build the app locally

Install Python and Node.js, then build on the operating system you want to run
the app on. From the repository root:

```sh
cd setup-wizard
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r backend/requirements.txt pyinstaller
cd ui
npm ci
npm run electron:build
```

On Windows, use `python` instead of `python3`, and activate the environment with
`.\.venv\Scripts\Activate.ps1` in PowerShell instead of `source .venv/bin/activate`.

The build command packages both the backend and the app. Find the finished
installer in `setup-wizard/ui/dist/`.
