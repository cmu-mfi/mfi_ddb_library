# Type 1: Search Trials

Endpoint: `POST /type1`

Description:
Search the metadata store for trials that match the supplied filters, and return the matching trial UUIDs.

Request body:
- `enterprise_id` (string, required): enterprise identifier to search within.
- `time_start` (string, required): start of the search window. Supports ISO 8601, `now`.
- `time_end` (string, required): end of the search window. Supports ISO 8601, `now`.
- `user_id` (string, required): requesting user ID.
- `user_domain` (string, optional): requesting user domain.
- `data_format` (string, optional): desired response format (`json` or `csv`).
- `site` (string, optional): metadata site filter.
- `device` (string, optional): metadata device filter.
- `trial_id` (string, optional): trial ID or name filter.
- `project_id` (string, optional): project UUID filter.
- `project_name` (string, optional): project name filter.
- `search_terms` (array of strings, optional): search terms applied against trial name and metadata content.

Response:
- `status`: `success` or `error`
- `message`: informational status message
- `trial_uuids`: list of matching trial UUID strings

Example request:
```json
{
  "enterprise_id": "enterprise-123",
  "time_start": "2024-01-01T00:00:00Z",
  "time_end": "2024-12-31T23:59:59Z",
  "user_id": "alice",
  "user_domain": "example.com",
  "site": "plant-1",
  "device": "pump-4",
  "search_terms": ["startup", "inspection"]
}
```

Example response:
```json
{
  "status": "success",
  "message": "Found 2 trial(s).",
  "trial_uuids": [
    "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
    "d96e0c4e-3c56-4c71-834f-abcdef123456"
  ]
}
```
