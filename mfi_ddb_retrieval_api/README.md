# MFI-DDB Retrieval API

Retrieval API is a web service that provides an interface for users to query and retrieve data stored in the database nodes of the MFI-DDB framework. It acts as a bridge between the database nodes and the end-users, allowing them to access the data through a standardized API.


- [Overview](#overview)
- [Components](#components)
    - [Metadata Store (MDS)](#metadata-store-mds)
    - [Retrieval Web Service (RWS)](#retrieval-web-service-rws)

## Overview

The API interfaces with the database nodes using their exposed web services to fetch the requested data. The user queries are processed using the metadata stored in the Metadata Store to identify the relevant database nodes and data entries.


_Arrow direction in the diagram below shows the data flow in the framework. It doesn't represent the direction of requests._

```mermaid
flowchart BT
    subgraph ModuleX["Database Nodes"]
        direction TB
        D1[Database Node 1]
        D2[Database Node ...]
        D3[Database Node n]       
    end

    subgraph ModuleY["Retrieval API"]
        direction LR
        E["MDS (MetaData Store)"] --> F["RWS (Retrieval Web Service)"]
    end

    A[Broker] --> E    
    A --> ModuleX
    D1 --> F
    D2 --> F
    D3 --> F
    F --> User

    classDef highlight fill:#094d57
    class ModuleY highlight;    
```

> [!Note]
> For more details on the database nodes, please refer to the [Databases](/mfi_ddb_database_nodes/README.md) documentation.

## Components

Two major components of the Retrieval API module are:
* Retrieval Web Service (RWS)
* Metadata Store (MDS)

### Retrieval Web Service (RWS)

The RWS is implemented as a RESTful web service that handles incoming requests from users, processes them using the metadata, and retrieves the relevant data from the database nodes. It provides endpoints for querying the data based on various parameters and returns the results in a structured format (e.g., JSON).

#### API Endpoints

| Endpoint | Method | Description | Request Parameters |
|----------|--------|-------------|-------------------|
| `/mfi-ddb/type0` | GET | Get information about available endpoints. | None |
| `/mfi-ddb/type1` | POST | Search trials using metadata store filters. Returns matching trial UUIDs. | `enterprise_id`, `time_start`, `time_end`, `user_id`, `user_domain` (optional), `site` (optional), `device` (optional), `trial_id` (optional), `project_id` (optional), `project_name` (optional), `search_terms` (optional) |
| `/mfi-ddb/type2` | POST | Retrieve data for a specific trial UUID. | `trial_uuid`, `user_id`, `user_domain` (optional), `time_start` (optional), `time_end` (optional), `frequency` (optional, default: 0 Hz), `data_format` (optional) |
| `/mfi-ddb/type3` | POST | Search trials and retrieve data when a unique trial is found. If multiple trials match, returns list of UUIDs instead. | Same as type1 + `frequency` (optional) |

> [!Note]
> All endpoints are prefixed with `/mfi-ddb` in the API path.

### Metadata Store (MDS)

The MDS stores metadata from the incoming data streams, that helps filter, search, and retrieve the relevant data from the database nodes. It maintains information about the data entries, their locations, and other relevant attributes that facilitate efficient querying and retrieval of data. We use PostgreSQL as the database for MDS.

```mermaid
erDiagram
    DDB_USER {
        VARCHAR(50) user_id PK
        VARCHAR(50) domain PK
        VARCHAR(50) created_by_user_id FK
        VARCHAR(50) created_by_domain FK
        VARCHAR(254) email
        VARCHAR(50) name
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    PROJECT {
        UUID project_id PK
        VARCHAR(50) project_name
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
        VARCHAR(50) created_by_user_id FK
        VARCHAR(50) created_by_domain FK
        JSONB details
    }

    USER_PROJECT_ROLE_LINKING {
        UUID id PK
        VARCHAR(50) user_id FK
        VARCHAR(50) domain FK
        UUID project_id FK
        user_role role "(admin, operator, <br>maintainer, researcher)"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    TRIAL {
        UUID uuid PK
        VARCHAR(255) trial_name
        VARCHAR(50) user_id FK
        VARCHAR(50) user_domain FK
        UUID project_id FK
        TIMESTAMPTZ birth_timestamp
        TIMESTAMPTZ death_timestamp
        BOOLEAN clean_exit
        JSONB metadata
        TEXT[] data_topics
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    GRAPH_EDGES {
        UUID edge_id PK
        UUID source_trial_id FK
        VARCHAR(255) target_entity_id
        VARCHAR(50) target_entity_type
    }

    %% Relationships
    %% DDB_USER ||--o{ USER : "created_by (self-referencing)"
    DDB_USER ||--o{ PROJECT : "creates"
    DDB_USER ||--o{ USER_PROJECT_ROLE_LINKING : "has_role_in"
    PROJECT ||--o{ USER_PROJECT_ROLE_LINKING : "assigned_to"
    DDB_USER ||--o{ TRIAL : "runs"
    PROJECT ||--o{ TRIAL : "contains"
    TRIAL ||--o{ GRAPH_EDGES : "is_source_for"
```