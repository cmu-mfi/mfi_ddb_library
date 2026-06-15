# Data Adapter Application

The application gives a REST API interface (`backend`) to mfi-ddb library for data adapter connections. It also provides a ReactJS based frontend (`frontend`) to interact with the backend API and display the data adapter connections.

> [!Note]
> Details of both the backend and frontend are provided in respective README.md files. \
> Backend: [backend/README.md](backend/README.md) \
> Frontend: [frontend/README.md](frontend/README.md)

## Docker Setup

The application can be run using Docker for a consistent and isolated environment.

**Quick Start:**
```
docker compose up --build
```

Open the browser and navigate to `http://localhost:3333` to access the application.

### Customization

* If the application needs to be access from a different host, edit the `compose.yaml` file:
    - Replace `REACT_APP_API_URL=http://localhost:5555` with `REACT_APP_API_URL=http://<your-host-ip>:5555`.
    - Make sure the <your-host-ip> is the IP address of the network you are using to access the application.

* To use a different port for the frontend, edit the `compose.yaml` file:
    - Replace `ports: - "3333:3000"` with `ports: - "<your-port>:80"`.

* The backend API is exposed on port `5555`. If the backend needs to be accessed from a different host, edit the `compose.yaml` file:
    - Replace `ports: - "5555:8000"` with `ports: - "<your-port>:8000"`.
    - Make sure to replace `5555` in the `REACT_APP_API_URL` environment variable in the frontend service with the new port number.