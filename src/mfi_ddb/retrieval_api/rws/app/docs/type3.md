# Type 3: Search Trials with Data Retrieval

Endpoint: `POST /type3`

Description:
Search for trials in the metadata store and retrieve trial data when the search resolves to a single unique trial.
If multiple trials match, the endpoint returns the list of matching trial UUIDs.

Request body:
- `enterprise_id` (string, required): enterprise identifier to search within.
- `time_start` (string, required): start of the search window. Supports ISO 8601,  `now`.
- `time_end` (string, required): end of the search window. Supports ISO 8601,  `now`.
- `user_id` (string, required): requesting user ID.
- `user_domain` (string, optional): requesting user domain.
- `data_format` (string, optional): desired response format (`json` or `csv`).
- `site` (string, optional): metadata site filter.
- `device` (string, optional): metadata device filter.
- `trial_id` (string, optional): trial ID or name filter.
- `project_id` (string, optional): project UUID filter.
- `project_name` (string, optional): project name filter.
- `search_terms` (array of strings, optional): search terms applied against trial name and metadata content.
- `frequency` (integer, optional): data sampling frequency in Hz. `-1` for latest only, `0` for default.

Response:
- If a single trial matches:
  - `status`: `success`
  - `message`: informational status message
  - `data`: retrieved trial data payload
- If multiple trials match:
  - `status`: `success`
  - `message`: informational status message
  - `trial_uuids`: list of matching trial UUID strings

Example request:
```json
{
  "enterprise_id": "enterprise-123",
  "time_start": "2024-01-01T00:00:00Z",
  "time_end": "2024-12-31T23:59:59Z",
  "user_id": "alice",
  "search_terms": ["startup"],
  "frequency": 0,
  "data_format": "json"
}
```

Example response when unique trial found:
```json
{
  "status": "success",
  "message": "Unique trial found and data retrieved.",
  "data": {
    "trial_uuid": "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
    "trial_name": "startup-run-100",
    "metadata": {
      "enterprise": "enterprise-123",
      "site": "plant-1"
    },
    "time_start": "2024-01-01T00:00:00Z",
    "time_end": "2024-12-31T23:59:59Z",
    "frequency": 0,
    "data_format": "json"
  }
}
```

Example response when multiple trials match:
```json
{
  "status": "success",
  "message": "Found 3 trial(s). Return UUID list because the result is not unique.",
  "trial_uuids": [
    "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
    "d96e0c4e-3c56-4c71-834f-abcdef123456",
    "3b5c2d6f-0b12-4ac4-9f82-123abc456def"
  ]
}
```
