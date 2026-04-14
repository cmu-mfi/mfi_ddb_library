# Type 2: Get Trial Data

Endpoint: `POST /type2`

Description:
Retrieve detailed data for a specific trial UUID using metadata from the metadata store and the data retrieval layer.

Request body:
- `trial_uuid` (string, required): UUID of the trial to retrieve.
- `time_start` (string, optional): retrieval window start. Supports ISO 8601, `birth`, `now`.
- `time_end` (string, optional): retrieval window end. Supports ISO 8601, `birth`, `now`.
- `user_id` (string, required): requesting user ID.
- `user_domain` (string, optional): requesting user domain.
- `frequency` (integer, optional): data sampling frequency in Hz. `-1` for latest only, `0` for default.
- `data_format` (string, optional): desired output format (`json` or `csv`).

Response:
- `status`: `success` or `error`
- `message`: informational status message
- `data`: retrieved data payload for the trial

Example request:
```json
{
  "trial_uuid": "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
  "time_start": "birth",
  "time_end": "now",
  "user_id": "alice",
  "frequency": 0,
  "data_format": "json"
}
```

Example response:
```json
{
  "status": "success",
  "message": "Trial data retrieved successfully.",
  "data": {
    "trial_uuid": "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
    "trial_name": "startup-run-100",
    "metadata": {
      "enterprise": "enterprise-123",
      "site": "plant-1",
      "device": "pump-4"
    },
    "time_start": "2024-01-01T00:00:00Z",
    "time_end": "2024-04-01T12:00:00Z",
    "frequency": 0,
    "data_format": "json",
    "retrieved_at": "2026-04-14T12:00:00Z",
    "source": "dws_placeholder"
  }
}
```
