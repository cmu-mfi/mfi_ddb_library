# Retrieval Web Service (RWS)

The RWS is implemented as a RESTful web service that handles incoming requests from users, processes them using the metadata, and retrieves the relevant data from the database nodes. It provides endpoints for querying the data based on various parameters and returns the results in a structured format (e.g., JSON).

## API Endpoints

Following are the API endpoints for the Retrieval Web Service (RWS):

## Type 0: Get Endpoints Info

**Endpoint**: GET `/type0`

**Request parameters**: empty

**Response**: JSON object containing the list of available endpoints and their descriptions.

## Type 1: Search Trials

**Endpoint**: POST `/type1`

**Request parameters**: json object with the following keys:
- `enterprise` (string, required): The enterprise to search within.
- `time_start` (string, required): The start time (accepted formats: ISO 8601, "birth", "now", ...).
- `time_end` (string, required): The end time (accepted formats: ISO 8601, "birth", "now", ...).
- `user_id` (string, required): Your user ID.
- `user_domain` (string, optional): Your user domain ("" by default).
- `data_format` (string, optional): The desired data format for the response (default: "json", options: "json", "csv").
- `site` (string, optional): Filter by site.
- `device` (string, optional): Filter by device.
- `trial_id` (string, optional): Filter by trial ID.
- `project_id` (string, optional): Filter by project ID.
- `project_name` (string, optional): Filter by project name.
- `search_terms` (list of strings, optional): A list of search terms to filter trials.

**Response**: A list of trial UUIDs that match the search criteria.

## Type 2: Get Trial Data

**Endpoint**: POST `/type2`

**Request parameters**: json object with the following keys:
- `trial_uuid` (string, required): The UUID of the trial to retrieve data for.
- `time_start` (string, optional): The start time for data retrieval (default: "birth").
- `time_end` (string, optional): The end time for data retrieval (default: "now").
- `user_id` (string, required): Your user ID.
- `user_domain` (string, optional): Your user domain ("" by default).
- `frequency` (number, optional): The frequency in Hz for data retrieval (-1 for latest value only, 0 for default).
- `data_format` (string, optional): The desired data format for the response (default: "json", options: "json", "csv").

**Response**: The data for the specified trial UUID in the requested format.

## Type 3: Search Trials with Data Retrieval

**Endpoint**: POST `/type3`

**Request parameters**: json object with the following keys:
- `enterprise` (string, required): The enterprise to search within.
- `time_start` (string, required): The start time (accepted formats: ISO 8601, "birth", "now", ...).
- `time_end` (string, required): The end time (accepted formats: ISO 8601, "birth", "now", ...).
- `user_id` (string, required): Your user ID.
- `user_domain` (string, optional): Your user domain ("" by default).
- `data_format` (string, optional): The desired data format for the response (default: "json", options: "json", "csv").
- `site` (string, optional): Filter by site.
- `device` (string, optional): Filter by device.
- `trial_id` (string, optional): Filter by trial ID.
- `project_id` (string, optional): Filter by project ID.
- `project_name` (string, optional): Filter by project name.
- `search_terms` (list of strings, optional): A list of search terms to filter trials.
- `frequency` (number, optional): The frequency in Hz for data retrieval (-1 for latest value only, 0 for default).

**Response**: The data in requested format, if the search returns unique trial UUID. Otherwise, a list of trial UUIDs that match the search criteria.

## Type 4: Download file/bytes data

(TBD)

## Users: registered user directory

Read-only lookups of the users registered in the metadata store (`ddb_user`),
for "pick a person" lists such as project members or "created by". Only
`user_id`, `domain` and `name` are returned, never email.

**Who is asking** goes in headers, not the body:

```
X-User-Id: ssterenr
X-User-Domain: ANDREW
```

The requester must be a registered user (`superadmin` is always allowed);
otherwise `401` (no header) or `403` (unknown requester). With single sign-on,
the login proxy in front of RWS sets these headers from the verified login,
so the endpoints do not change.

**Search**: GET `/users?search=<text>&limit=25`
Matches `search` (case-insensitive) in user ID, name or domain; `limit` 1-100.

```json
{"status": "success", "message": "Found 1 user(s).",
 "users": [{"user_id": "ssterenr", "domain": "ANDREW", "name": "Stephanie Steren-Ruta"}]}
```

**Check one user**: GET `/users/{domain}/{user_id}`
`200` with `{"user": {...}}` if registered, `404` if not.

Tests that need no database: `pytest -v test_rws/test_rws_users.py`