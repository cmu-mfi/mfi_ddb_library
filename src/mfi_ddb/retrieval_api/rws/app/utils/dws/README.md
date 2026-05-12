# Database Web Service (DWS)
Each database node exposes a [gRPC](https://grpc.io/docs/what-is-grpc/core-concepts/) web service for data retrieval. The following services are made available:

| Method | Type | Description | Use Case |
| :--- | :--- | :--- | :--- |
| **`GetDataPoint`** | Unary | Retrieves a **single** datapoint for a specific topic at an exact timestamp. | Auditing specific events or checking system state at a past moment. |
| **`GetDataRange`** | Unary | Retrieves a list of datapoints between a `start_time` and `end_time`. Supports **pagination** via tokens. | Generating historical charts, trend analysis, or bulk data export. |
| **`StreamData`** | Server Stream | Opens a persistent connection. The server pushes new datapoints to the client in **real-time** as they occur. | Live dashboards, active monitoring, and immediate alerting. |

## Compiling proto files for Python

### Installing Dependencies
Through apt:

```sh
sudo apt install python3-grpc-tools python3-grpcio
```

Through pip:

> [!Note]
> Use this method with virtual environment, if you don't have sudo access.

```sh
pip install grpcio grpcio-tools 
```


### Compiling proto files
Before we can read or write anything, the protobuf definitions have to be compiled to be used by python.

To do this, run the script in this folder **build_all.sh** (ignore the warning messages)

```sh
bash ./build_all.sh
```
