# MFI DDB Metadata Form

A small web app for entering metadata (projects, users, trial tags, or anything
you define) that is published to the MFI DDB through the **mfi_ddb KeyValue
data adapter**. Forms are defined in one YAML file, so each deployment can
shape them to its own needs without touching code.

```
Browser ──► frontend (nginx, :3002) ──/api──► backend (FastAPI, :8002)
                                                 │ validate against kv.json
                                                 ▼
                                    KvDataAdapter ─► Streamer (topic family "kv")
                                                 │
                                                 ▼
                       MQTT  mfi-v1.0-kv/<enterprise>/<site>/<msg_type>/data
                                                 │
                                   KV-PSQL / metadata store nodes
```

Like `mfi_ddb_data_adapter/data_adapter_app`, it is self-contained: the
backend installs `mfi_ddb` from PyPI and nothing else in the repo is needed.

## Quick start (Docker)

```bash
cd mfi_ddb_metadata_form
cp .env.example .env          # optional: broker address, credentials, identity
docker compose up -d --build
```

- UI: <http://localhost:3002>
- API docs: <http://localhost:8002/docs>

By default it publishes to an MQTT broker on the host
(`host.docker.internal:1883`). Set `FORM_DRY_RUN=true` in `.env` to try the
forms without a broker.

### As part of the full pipeline

The service is also available in `docker/` under the `mfa` profile, on the
shared `mfi_network`, publishing to the pipeline's broker:

```bash
cd docker
docker compose --profile mfa --profile mqtt-broker up -d --build
```

## Creating forms in the browser (form builder)

Click **Form builder** at the top right of the page, then **New form**:

1. Give the form a name.
2. Add one row per **key**: a label (what people see), the key name (filled
   in from the label automatically), the type of value (text, long text,
   number, whole number, yes/no, dropdown, date, date & time), whether it's
   required, and optionally choices, a default, min/max and a hint.
3. Click **Create form**. It appears as a new tab under **Fill in forms**.

People using the form only enter the **values**. Each submission is sent as a
`msg_type: data` payload with your keys as top-level keys, plus `trial_id`
(always required, because the KV adapter drops `data` without one) and an
automatic `timestamp`.

Builder forms are stored by the backend in `data/custom_forms.json` on the
data volume, separately from the YAML file. Forms defined in the YAML can only
be changed in the YAML. To restrict who can create, edit or delete forms, set
`FORM_ADMIN_TOKEN` in `.env`; the builder then asks for it. To turn the
builder off, set `builder.enabled: false` in the config.

## Configuring the forms

Everything lives in [`config/form-config.yaml`](config/form-config.yaml). The
`config/` folder is mounted into the backend, and the file is re-read when it
changes. **Edit it, then reload the page**; no rebuild or restart is needed.
If an edit is invalid, the app keeps serving the last valid version and shows
the error at the top of the page (and in `docker logs mfi-metadata-form-backend`).

| Section    | What it controls |
|------------|------------------|
| `app`      | Page title and subtitle |
| `adapter`  | KV adapter instance name and the default `trial_id` |
| `publish`  | `dry_run`, connect/publish timeouts, `settle_seconds` |
| `history`  | Local log of submissions shown under the form |
| `streamer` | mfi_ddb Streamer config: `user` (publisher identity) and `mqtt` (broker, `enterprise`, `site`) |
| `forms`    | The forms. Each one produces one KV payload of its `msg_type` (`project`, `user`, `tp-tag`, `data`) |

Any string can reference environment variables as `${VAR}` or
`${VAR:-default}`. Keep secrets such as `MQTT_PASSWORD` in `.env`.

### Field reference

```yaml
- name: project_id          # key in the payload (required)
  label: Project ID         # UI label
  type: text                # text | textarea | email | number | integer | select |
                            # multiselect | checkbox | date | datetime | list | json
  required: true
  default: ...              # initial value; datetime: "now"
  placeholder: p-550
  help: Hint under the input
  pattern: "^[a-z0-9-]+$"   # regex for text values
  min: 0                    # number/integer bounds
  max: 100
  options: [a, b, c]        # select/multiselect (or {value, label} items)
  hidden: true              # not shown; `default` is always sent
  auto: now                 # filled server-side: now | uuid | trial_id |
                            #   streamer.<path> | env.<VAR>
  editable: true            # with auto: prefill the auto value but allow changes
  fields: [...]             # type list: fields of each item
  min_items: 1              # list bounds
  max_items: 10
  item_label: member        # "+ Add member"
```

Payloads can carry extra keys beyond what `kv.json` requires, so add whatever
your site needs. `schema_version` and `msg_type` are added automatically. Set
`enabled: false` to hide a form. The disabled `trial-note` form in the default
config shows a custom `data` form.

On startup, and in `/health`, the backend lists **configuration warnings**,
for example a form that is missing a field the kv schema requires for its
`msg_type`.

## How a submission is published

1. Each field is coerced to its type and checked (`required`, `options`,
   `pattern`, `min`/`max`). `auto` fields are filled in on the server.
2. The payload is validated against the library's `kv.json`, the same check
   `KvDataAdapter` applies, and errors are shown next to the relevant inputs.
   **Preview payload** stops at this step.
3. The backend does a quick MQTT connect with a timeout, so a wrong broker
   address or bad credentials fail fast instead of hanging.
4. `KvDataAdapter({adapter_name, trial_id, payloads: [payload]})` is handed to
   `Streamer`. The Streamer publishes birth metadata plus the payload, waits
   `settle_seconds`, then disconnects, which publishes death metadata.
   `trial_id` is taken from the payload if present, otherwise from `adapter.trial_id`.

Submissions are published one at a time. Each outcome (`published`,
`dry_run`, `failed`) is stored in SQLite on the `metadata_form_data` volume.

## API

| Method | Path | |
|--------|------|-|
| GET  | `/health` | Status, config path, config error/warnings |
| GET  | `/config` | Forms and target info for the UI (never includes the password) |
| POST | `/forms/{id}/preview` | `{"values": {...}}` → built payload; nothing is published |
| POST | `/forms/{id}/submit`  | `{"values": {...}}` → validate and publish |
| GET  | `/submissions?limit=25` | Recent submissions |

Validation errors return `422` with `errors: [{field, message}]`. `field` is
a dotted path such as `user_roles.0.role`.

## Development without Docker

```bash
# backend (Python 3.12)
cd backend
python -m venv .venv && .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt pytest httpx
FORM_CONFIG_PATH=../config/form-config.yaml FORM_DRY_RUN=true \
  uvicorn app.main:app --port 8002 --reload
python -m pytest                                     # tests

# frontend (Node 22) – proxies /api to localhost:8002
cd frontend
npm install
npm run dev
```

Tests can also run in Docker:
`docker build -f backend/Dockerfile --target test -t mfi-metadata-form-test . && docker run --rm mfi-metadata-form-test`

## Layout

```
mfi_ddb_metadata_form/
├── compose.yaml            standalone stack (backend + frontend)
├── .env.example
├── config/form-config.yaml the file you edit
├── backend/                FastAPI app (config, payload builder, KV publisher, history)
└── frontend/               React + Vite + Tailwind, served by nginx
```
