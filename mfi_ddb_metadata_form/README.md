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
backend installs `mfi_ddb` from PyPI. The one optional dependency is the
**Retrieval API (RWS)**, used to look up registered users for the "pick a
person" fields; without it those fields are plain text boxes.

## At a glance

| Feature | Where |
|---------|-------|
| Forms for projects, users, trial tags and custom data, validated against the library's `kv.json` and published through `KvDataAdapter` | [Configuring the forms](#configuring-the-forms), [How a submission is published](#how-a-submission-is-published) |
| Form builder: create forms in the browser (text, numbers, dropdowns, multiple choice, dates, people, sections), with Preview and Duplicate | [Form builder](#creating-forms-in-the-browser-form-builder) |
| Sections that group fields, collapsible, re-opening when they hold an error | [Form builder](#creating-forms-in-the-browser-form-builder), [Field reference](#field-reference) |
| "Pick a person" fields backed by the registered users; only registered people can be named; every form has a required **Created by** | [User directory](#user-directory-pick-a-person-fields) |
| Settings page: broker, enterprise and a site per form, without editing files | [Settings page](#settings-page-broker-and-site-per-form) |
| Admin token for the builder, settings and reserved platform accounts (until single sign-on) | [Access control](#access-control-until-single-sign-on) |
| Local history of submissions | [How a submission is published](#how-a-submission-is-published) |

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
forms without a broker. To enable the "pick a person" fields, set `RWS_URL` to a
running Retrieval API (e.g. `http://host.docker.internal:8000`).

### As part of the full pipeline

The service is also available in `docker/` under the `mfa` profile, on the
shared `mfi_network`, publishing to the pipeline's broker. Add the `retrieval`
profile for the user directory (metadata store + Retrieval API):

```bash
cd docker
docker compose --profile mfa --profile mqtt-broker --profile retrieval up -d --build
```

### Environment variables

All optional; set them in `.env` (standalone) or `docker/.env` (pipeline).

| Variable | Default | Purpose |
|----------|---------|---------|
| `MQTT_BROKER_ADDRESS` / `MQTT_BROKER_PORT` | `host.docker.internal` (standalone), `mqtt-broker` (pipeline) / `1883` | Broker the KV adapter publishes to |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | empty | Broker credentials |
| `MQTT_ENTERPRISE` / `MQTT_SITE` | `CMU` / `M19` | Topic levels; each form can override the site |
| `FORM_USER_ID` / `FORM_USER_DOMAIN` | `metadata-form` / `ANDREW` | App identity (publisher of last resort) |
| `FORM_ADMIN_TOKEN` | empty | Token for builder, settings and reserved accounts (see [Access control](#access-control-until-single-sign-on)) |
| `FORM_DRY_RUN` | `false` | Validate and log, never publish |
| `RWS_URL` | empty (standalone), `http://mfi-rws-app:8000` (pipeline) | Retrieval API for the user directory; empty = off |
| `DIRECTORY_USER_ID` / `DIRECTORY_USER_DOMAIN` | `superadmin` / `superadmin` | Identity the app uses for user lookups (until single sign-on) |

## Creating forms in the browser (form builder)

Click **Form builder** at the top right of the page, then **New form**:

1. Give the form a name.
2. Add one row per **key**: a label (what people see), the key name (filled
   in from the label automatically), the type of value (text, long text,
   email, number, whole number, yes/no, dropdown, multiple choice, date,
   date & time, person), whether it's required, and optionally choices, a
   default, min/max and a hint. **Person** is a picker of registered users; it
   sends the user ID as `<key>` and their domain as `<key>_domain`.
3. Optionally **Add section**: a heading (and description) that groups the
   keys after it, up to the next section. Sections are display only, never
   data. When a form has sections, the automatic "Created by" fields appear
   under their own **Submitted by** heading. People filling in the form can
   click a heading to collapse its section; a collapsed section that holds an
   error (or a warning) opens again automatically.
4. Use **Duplicate** on a row to copy it, the arrows to reorder, and
   **Preview** (top of the editor) to try the form exactly as people will see
   it before saving. Cancel asks before discarding unsaved changes.
5. Click **Create form**. It appears as a new tab under **Fill in forms**.

To start a new form from an existing one, use **Duplicate** in the form list.

People using the form only enter the **values**. Each submission is sent as a
`msg_type: data` payload with your keys as top-level keys, plus three things
every builder form gets automatically:

- `trial_id`: always required, because the KV adapter drops `data` without one;
- `created_by_user_id` / `created_by_domain`: required, picked from registered
  users like on the other forms (reserved accounts need the admin token);
- `timestamp`: filled in when sent.

These names can't be used as keys of your own.

Builder forms are stored by the backend in `data/custom_forms.json` on the
data volume, separately from the YAML file. Forms defined in the YAML can only
be changed in the YAML. To restrict who can create, edit or delete forms, set
`FORM_ADMIN_TOKEN` in `.env`; the builder then asks for it. To turn the
builder off, set `builder.enabled: false` in the config.

## Settings page: broker and site per form

The **Settings** tab lets an admin change, without editing any file:

- **MQTT broker**: address, port, enterprise, and optional username / password,
  with a **Test connection** button.
- **Site per form**: each form publishes to
  `mfi-v1.0-kv/<enterprise>/<site>/<msg_type>/data` under its own site.

Overrides are stored in the data volume (`data/settings.json`, or `FORM_SETTINGS`)
and win over `form-config.yaml`; empty values and **Reset to defaults** fall back
to the file. A form's site resolves as: Settings page → the form's `site:` in the
YAML (or the Site field in the form builder) → `streamer.mqtt.site`.
The password is write-only and never sent to the browser. If
`builder.admin_token` is set, saving settings needs that token too.

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
                            # multiselect | checkbox | date | datetime | list | json |
                            # section (see below)
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
  lookup: user              # pick from registered users (see "User directory")
  domain_field: created_by_domain   # with lookup: sibling field filled with the user's domain

- type: section             # heading that groups the fields after it; no data
  label: Machine settings   #   heading (required); `name` is optional
  help: Shown under the heading
```

### User directory ("pick a person" fields)

Fields with `lookup: user` (Created by, project Members, trial Operator in the
default config) become type-ahead pickers of the users registered in the
metadata store. Picking a person fills in the user ID **and** its domain, and
the field shows whether the typed user is registered.

Lookups go through the Retrieval API's `/users` endpoints, configured in the
`directory` section (`RWS_URL`; empty = off, fields become plain text). Until
single sign-on, the app looks users up as `DIRECTORY_USER_ID` /
`DIRECTORY_USER_DOMAIN`, which must be a registered user (default `superadmin`).

**Reserved accounts** (`directory.reserved_users`, default `superadmin/superadmin`)
are platform accounts, not people: they never appear in the pickers, and a form
that names one in a user field is refused unless the request carries the admin
token (`FORM_ADMIN_TOKEN`; platform admins enter it on the Settings tab). With
no admin token configured, nobody can use them through the form.

Only **registered** users can be named. An unregistered user is an error, like a
missing required field: the field turns red, **Submit** is disabled, and the
backend refuses the form (preview and submit) without publishing anything. If
the user directory is configured but unreachable, forms are refused too, since
registration can't be confirmed. Register people first with the **New user**
form. "Created by" has no pre-filled value: people pick themselves (or whoever
created the record) from the list.

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
| GET  | `/settings` | Broker settings, defaults and each form's site (never the password) |
| PUT  | `/settings` | `{"mqtt": {...}, "form_sites": {"<form id>": "<site>"}}` → save overrides |
| DELETE | `/settings` | Reset all overrides to the configuration file |
| POST | `/settings/test` | `{"mqtt": {...}}` (optional, unsaved values) → try connecting |
| GET  | `/directory/users?search=` | Registered users for pickers (via the Retrieval API) |
| GET  | `/directory/users/{domain}/{user_id}` | Is this user registered? (`registered`, `reserved`) |
| GET/POST/PUT/DELETE | `/builder/forms[/{id}]` | Forms created in the browser |

Validation errors return `422` with `errors: [{field, message}]`. `field` is
a dotted path such as `user_roles.0.role`. A reserved account without the admin
token returns `403`.

## Access control (until single sign-on)

The app does not log people in yet; it is designed so CMU single sign-on can
be added in front of it without changing the forms:

- **Admin token** (`FORM_ADMIN_TOKEN`): when set, the form builder, the
  Settings page and the use of reserved accounts (e.g. `superadmin`) require
  it. Platform admins enter it once on the Settings tab (kept for the browser
  session). When empty, the builder and settings are open and reserved
  accounts cannot be used at all.
- **Who can be named**: only users registered in the metadata store (through
  the Retrieval API `/users`), and never a reserved account without the token.
- **With single sign-on**: a login proxy in front of the apps would provide the
  user; "Created by" would come from the login, and the token would become a
  platform-admin role. The Retrieval API already takes the requester from the
  `X-User-Id` / `X-User-Domain` headers such a proxy sets.

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
├── backend/
│   ├── app/
│   │   ├── main.py          API endpoints
│   │   ├── config.py        YAML config model (forms, fields, directory, ...)
│   │   ├── payload.py       values -> kv.json-valid payload
│   │   ├── publisher.py     KvDataAdapter + Streamer publishing
│   │   ├── directory.py     registered-user lookups (Retrieval API /users)
│   │   ├── custom_forms.py  form-builder forms
│   │   ├── settings.py      Settings page overrides
│   │   └── history.py       submission log (SQLite)
│   └── tests/
└── frontend/src/           React + Vite + Tailwind, served by nginx
    └── components/         MetadataForm, FieldInput, UserPicker, Sections,
                            FormBuilder, FormEditor, Settings, SubmissionHistory
```

Related: the Retrieval API's user endpoints are documented in
[`mfi_ddb_retrieval_api/rws/README.md`](../mfi_ddb_retrieval_api/rws/README.md#users-registered-user-directory).
