# Type 4: Download file/bytes data

Endpoint: `POST /type4`

Description:
Download file/bytes data for a specific trial. File values are retrieved from the data retrieval layer (DWS) and returned for download. If the trial contains multiple files, they are bundled in a single flat `.zip` archive; otherwise the single file is returned as is. File metadata is included in the `X-File-Metadata` response header (JSON encoded).

Request body:
- `trial_uuid` (string, required): UUID of the trial to retrieve files from.
- `time_start` (string, optional): retrieval window start. Supports ISO 8601, `birth`, `now`. Defaults to the trial's birth time.
- `time_end` (string, optional): retrieval window end. Supports ISO 8601, `birth`, `now`. Defaults to the current time.
- `user_id` (string, required): requesting user ID.
- `user_domain` (string, optional): requesting user domain.
- `frequency` (integer, optional): data sampling frequency in Hz. `-1` for latest only, `0` for default. Not used for file downloads.
- `data_format` (string, optional): unused for file downloads (`json` or `csv`).

Response:
- Binary file download (`application/octet-stream`), or a `.zip` archive when multiple files match.
- `X-File-Metadata` (header): JSON encoded object containing trial metadata and a `files` list; each entry contains `topic`, `timestamp` and `filename` of a downloaded file.

Errors:
- `404`: trial not found, or no file data found for the trial within the request window.
- `503`: not enough system resources (memory/CPU) available to serve the download; retry later.

Resource guard:
Before retrieving file contents, the service checks available memory (default minimum: 256 MB) and CPU utilization (default maximum: 90%). If the resources stay below the limits for more than 5 seconds, a `503` is returned. The limits are currently hardcoded; moving them to a configuration file is planned.

Example request:
```json
{
  "trial_uuid": "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
  "time_start": "birth",
  "time_end": "now",
  "user_id": "alice"
}
```

Example `X-File-Metadata` header (JSON decoded):
```json
{
  "trial_uuid": "4a7bf4e3-9fbd-4b5c-8952-1234567890ab",
  "trial_name": "startup-run-100",
  "files": [
    {
      "topic": "mfi-v1.0-blob/cam1/images",
      "timestamp": "2024-01-01T00:00:05",
      "filename": "img_0001.jpg"
    }
  ]
}
```
