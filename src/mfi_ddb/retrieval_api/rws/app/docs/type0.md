# Type 0: Get Endpoints Info

Endpoint: `GET /type0`

Description:
Returns the list of available Retrieval API endpoints and a short description for each entry.

Request:
- No request body.

Response:
- `status`: `success` or `error`
- `message`: informational status message
- `endpoints`: list of endpoint objects containing:
  - `endpoint`: the API path
  - `description`: a short description of what the endpoint does

Example response:
```json
{
  "status": "success",
  "message": "Available endpoints returned.",
  "endpoints": [
    {
      "endpoint": "/type0",
      "description": "Get information about the available Retrieval API endpoints."
    },
    {
      "endpoint": "/type1",
      "description": "Search trials using metadata store filters."
    },
    {
      "endpoint": "/type2",
      "description": "Retrieve data for a single trial UUID."
    },
    {
      "endpoint": "/type3",
      "description": "Search trials and retrieve data when a unique trial is found."
    }
  ]
}
```
