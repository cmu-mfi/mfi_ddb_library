# Data Adapter App - Frontend

React (Vite) UI for creating, editing, and monitoring `mfi_ddb` data adapter connections through the [backend](../backend/README.md) REST API.

## Getting Started

**Via Docker** (recommended - see the [top-level README](../README.md)): `docker compose up --build -d` from `data_adapter_app/`, then open `http://localhost:3000`. In this mode, nginx (inside the frontend container) proxies all `/api/` requests to the backend container internally - the frontend never needs to know the backend's address directly.

**Standalone dev server** (hot-reload, no Docker):
```bash
npm install
VITE_API_URL=http://localhost:8000 npm run dev
```
Setting `VITE_API_URL` points the frontend directly at a backend you're running separately (see [backend/README.md](../backend/README.md)), bypassing the nginx proxy entirely - this works because the backend already has CORS wide open. Without it, `API_BASE_URL` (in `src/data/defaults.js`) falls back to `/api`, which only resolves correctly behind the Docker/nginx setup.

## Project Structure

```
src
├── App.jsx                      # Top-level state: fetches /connections/all on an interval,
│                                 # renders the connection list + modal
├── api.js                       # All backend fetch calls
├── main.jsx                     # Vite/React entry point
├── data
│   └── defaults.js              # API_BASE_URL resolution
├── components
│   ├── ConnectionList.jsx       # Renders the list of ConnectionItem cards
│   ├── ConnectionItem.jsx       # One connection's status banner + Pause/Resume/
│   │                             # Stop/Reconnect/Edit/Delete actions
│   ├── ConnectionModal.jsx      # New/Edit connection form (YAML textareas today)
│   ├── SchemaForm.jsx           # Generic JSON-Schema -> form renderer (text/number/
│   │                             # boolean/enum/nested-object fields). Not yet wired
│   │                             # into ConnectionModal - see note below.
│   └── Modal.jsx                # Generic modal shell
└── lib/utils.js
```

There is no client-side persistence (no localStorage) - `App.jsx` treats the backend's `GET /connections/all` response as the single source of truth for what connections exist and their live status, re-fetching it on an interval.

### `SchemaForm.jsx`

Every adapter's config schema is already available as real JSON Schema from the backend (`GET /connections/adapters`'s `configSchema` field, and `GET /connections/streamer` for the streamer config) - `SchemaForm` is a self-contained renderer for that schema (required fields shown by default, optional fields behind a "Show advanced" toggle, nested objects as collapsible sections). It exists but isn't imported anywhere yet; `ConnectionModal.jsx` still uses raw YAML textareas for adapter/streamer config. Swapping to `SchemaForm` there would remove the need to hand-write and keep in sync a YAML example for every adapter. Note it wouldn't automatically enforce every validation rule on its own (e.g. a schema-level "exactly one of these two fields" constraint) - `/connections/validate/adapter` should stay as the authoritative check regardless of which input method is used.
