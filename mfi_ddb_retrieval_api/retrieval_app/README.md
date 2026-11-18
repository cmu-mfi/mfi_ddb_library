# MFI DDB Retrieval App (Data Explorer)

A simple web UI for the MFI DDB **Retrieval API (RWS)**: find a trial, look at
its metadata, and download its data (CSV or JSON) or its stored files. All
retrieval is done by the API; the app only sends requests and saves what comes
back. Use a dedicated tool (e.g. Grafana) to chart or analyse the data.

It is a single container (React + nginx) that only needs to reach an RWS over
HTTP. Nothing else from the pipeline has to run on the same machine.

| Page | What it does | RWS endpoint |
|------|--------------|--------------|
| **Trials** | Search by time range, trial ID, project ID/name and search terms, then open a trial from the list. | `POST /type1` |
| Trial page | Metadata and data topics. Pick a time window (the whole trial by default), then **Download data** (CSV or JSON) or **Download files** (one file as is, several as a `.zip`). | `POST /type2`, `POST /type4` |
| **API console** | Endpoint list from RWS, raw requests to any endpoint, inspect/download the response | `/type0`-`/type4` |

## Quick start

```bash
cd mfi_ddb_retrieval_api/retrieval_app
docker compose up -d --build
```

Open http://localhost:3003.

By default the UI talks to an RWS published on this machine (`http://host.docker.internal:8000`).
To use another one, copy `.env.example` to `.env` and set `RWS_URL`, or:

```bash
RWS_URL=http://rws-host.example:8000 docker compose up -d --build
```

On a Docker network shared with RWS, set `RWS_URL=http://mfi-rws-app:8000` (the
RWS container name); that is also the image's default.

## How it works

nginx serves the built app and proxies the API, so the browser never needs
CORS or the RWS address:

| Browser path | Forwarded to |
|--------------|--------------|
| `/api/health` | `${RWS_URL}/` (health) |
| `/api/<endpoint>` | `${RWS_URL}/mfi-ddb/<endpoint>` |


Responses are gzipped and the proxy waits up to 300 s, since `/type2` returns
every datapoint in the window as one JSON document.

The proxy resolves the RWS host on every request (Docker's DNS,
`resolver 127.0.0.11`) rather than once at startup, so it keeps working when
the RWS container is recreated with a new IP. A literal host in `proxy_pass`
would keep the old address and answer 502 ("Retrieval API: offline") until
this container restarted. Docker's DNS does not know names that exist only in
the container's `/etc/hosts` (e.g. `host.docker.internal` from `extra_hosts`
on Linux), so `rws-hosts.envsh` replaces such a host in `RWS_URL` with its IP
at startup.

### Things to know

- **Who is asking ("Requesting as"):** you enter your user ID and domain. It is
  not remembered: every visit starts empty. RWS only returns trials that user owns
  or reaches through a project role. Search and downloads are disabled while
  the user ID is empty. **With single sign-on**, "Requesting as" comes from the
  login (`src/lib/identity.js` is the one place to change) and the fields go
  away.
- **Times** are entered and shown in your local time zone and sent as UTC.
- **Downloads** are saved once RWS has gathered the whole window. High-rate
  trials hold a lot of data, so long windows can take minutes, and one that
  takes over the proxy's 300 s fails with a 504: use shorter windows.
- **Files** only exist for file (blob) topics; RWS answers 404 when the window
  has none, and 503 when it is short on memory or CPU.
## Development without Docker

```bash
cd frontend
npm install
npm run dev          # http://localhost:5173, proxies /api to http://localhost:8000
VITE_RWS_URL=http://other-host:8000 npm run dev
```

## Layout

```
mfi_ddb_retrieval_api/retrieval_app/
├── compose.yaml              # standalone: just the UI container
├── .env.example
└── frontend/
    ├── Dockerfile            # node build -> nginx
    ├── nginx.conf.template   # /api proxy to ${RWS_URL}
    ├── rws-hosts.envsh       # startup: /etc/hosts-only RWS host -> IP
    └── src/
        ├── App.jsx
        ├── api.js            # RWS calls
        ├── lib/              # time, download, identity
        └── components/       # TrialSearch, TrialDetail, ApiConsole, Common
```
