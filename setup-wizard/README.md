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

## Choose services and enter settings

1. Open the app and click **Get Started**.
2. Select the services you want to run. Include **Data Adapter App** if you want
   to open its dashboard after setup.
3. Click **Configure Parameters**.
4. Review each configuration page and enter your settings. Use **Next
   Configuration** to continue or **Previous Configuration** to go back. Your
   entries are kept as you move between pages.

## Download your configurations

On the final configuration page, you can save a copy of your settings before
starting the services. Choose either option:

| Button | What you download |
| --- | --- |
| **Download JSON** | One `mfi-ddb-configurations.json` file containing your selected services and their settings. |
| **Download YAML + JSON (ZIP)** | One `mfi-ddb-configurations.zip` archive containing individual YAML configuration files and the same JSON file. |

The save dialog starts in your **Downloads** folder. You can choose a different
location. Extract the ZIP to read the individual YAML files.

Saving a copy is optional. You can download either format without starting Docker
services, and your entries stay in the wizard so you can continue with setup.

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
