# Data Adapter Frontend Improvements

Review scope: the data adapter frontend on `feature/performance-testing`, including backend behavior that affects frontend controls and deployment.


| Issue title | Location | Why is it a problem? | Expected result after fixing it | Benefit |
| --- | --- | --- | --- | --- |
| **Fixed — High — Pause / Resume does not restart polling** | [AdapterFactory.connect_and_stream / resume_streaming / pause_streaming / disconnect](mfi_ddb_data_adapter/data_adapter_app/backend/app/services/adapter_factory.py), [Lifecycle regression tests](mfi_ddb_data_adapter/data_adapter_app/backend/tests/test_adapter_factory.py) | Previously, pausing allowed the polling thread to exit. Resume set the event and `is_streaming` flag without restarting the worker, so the UI could show streaming while no data was being polled. | The worker now waits on a condition while paused and wakes on Resume. A separate shutdown event stops it on Disconnect. Each worker retains its own shutdown event and streamer so it cannot reactivate against a replacement connection. | Polling resumes on the existing worker without duplicate workers, and paused workers can shut down cleanly. Verified by four lifecycle regression tests. |
| **Fixed — High — Edits do not update the running connection** | [ConnectionModal.handleSave](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [ConnectionManager.updateConnection](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js) | Previously, editing only updated browser storage. The backend kept its previous configuration, and polling mode and frequency changes were discarded. | Edits now disconnect and replace the backend instance using the selected adapter, YAML configurations, mode, and frequency. Settings are persisted after success. Failures keep the modal open and attempt to restore the previous saved configuration. This browser's restoration logic skips connections being edited. | Saved settings match the replacement connection. Stopped adapters stay disconnected, paused polling adapters are paused again after reconnecting, and failed edits retain the previous saved settings. Verified by 13 frontend regression tests at implementation. |
| **Fixed — High — Failed connection attempts are saved as successful** | [ConnectionModal.handleSave](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [ConnectionManager.createConnection / connectWithState](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js) | Previously, HTTP failures still saved an adapter and closed the modal because the connect response was ignored. | Creation now checks HTTP success, valid JSON, and explicit connected/streaming flags before persisting settings or closing the form. Failures stay visible for retry. Confirmed connections are disconnected if saving to browser storage fails, with cleanup failures reported explicitly. | Prevents false success, preserves the form on failure, and allows retry after a rejected connection. Verified by 16 regression tests and a modal-handler probe at implementation. |
| **Fixed — High — Callback Stop / Reconnect state is inconsistent** | [ConnectionItem actions and status polling](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [ConnectionManager lifecycle methods](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js), [App restoration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx) | Previously, Stop's `stopped` state was overwritten by the pause callback, late status responses could overwrite the stopped display, and restoration could reconnect stopped adapters. | Stop now persists only `stopped`; Reconnect persists `streaming` after confirmed success. Initial row state uses saved intent, obsolete status results are ignored, and verification/restoration skip stopped adapters. Legacy callback `paused` records are interpreted as stopped. | Stopped adapters retain a working Reconnect control after refresh and stay inactive until explicit reconnection. Failed actions remain visible and retryable. Verified by 16 lifecycle tests and component-handler probes at implementation. |
| **Fixed — Medium — Recovery fails when a backend connection is missing** | [ConnectionManager verification, restoration, and Resume](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js), [Status and Resume API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [App restoration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx), [ConnectionItem actions](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [Recovery tests](mfi_ddb_data_adapter/data_adapter_app/frontend/tests/connection-recovery.test.mjs) | Previously, one rejected status request aborted the entire verification batch. A paused adapter lost during a backend restart could not resume because its backend instance no longer existed. | Verification returns independent results per ID. Restoration handles failures per connection, and recreation follows a confirmed 404. Explicit Resume recreates missing instances from saved configuration, confirms streaming, and then updates saved intent. Paused/stopped adapters remain inactive during automatic restoration. | Missing adapters can recover without blocking healthy adapters, and paused adapters can resume after a backend restart. Failed recovery retains saved intent and remains retryable. Verified by 25 recovery tests and App/row handler probes. |
| **Fixed — Improvement — Validation errors lack useful detail** | [Validation API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [ConnectionModal.validateConfiguration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [Field error formatting](mfi_ddb_data_adapter/data_adapter_app/frontend/src/lib/validation.js), [Validation endpoints](mfi_ddb_data_adapter/data_adapter_app/backend/app/api/v0/router.py), [Configuration parsing and schema validation](mfi_ddb_data_adapter/data_adapter_app/backend/app/utils/utils.py) | Previously, boolean validation results hid field paths and YAML positions, and request failures were presented as invalid configuration. | Responses retain `is_valid` and add structured errors with field paths, messages, and error types. YAML syntax errors include line/column. The form groups adapter and streamer errors, displays request failures separately, and allows retry without editing valid YAML. | Users can locate invalid fields directly and distinguish configuration problems from service failures. Verified by 17 backend validation tests, 15 frontend tests, and modal-handler probes; all 4 existing backend lifecycle tests also pass. |
| **Fixed — Improvement — Stale validation responses can overwrite newer results** | [ConnectionModal validation and debounce](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [Validation request tracking](mfi_ddb_data_adapter/data_adapter_app/frontend/src/lib/validation-requests.js), [Validation API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [Regression tests](mfi_ddb_data_adapter/data_adapter_app/frontend/tests/validation-requests.test.mjs) | Previously, cancelling debounce timers left requests in flight. Older responses could overwrite current errors, release a newer request's loading state, or affect Save after input changed. | Edits immediately abort and invalidate the previous validation batch. Only the current batch may update errors, loading state, or the validation cache. Closing cancels validation; Save supersedes background validation and blocks pending debounce callbacks. | Validation results reflect current input, and stale successful responses cannot authorize saving obsolete settings. Verified by 13 regression tests and modal-handler probes. |
| **Fixed — Improvement — Most API requests have no timeout or cancellation** | [Shared request helper](mfi_ddb_data_adapter/data_adapter_app/frontend/src/lib/request.js), [Frontend API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [App health and verification](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx), [ConnectionModal](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [ConnectionItem](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [Regression tests](mfi_ddb_data_adapter/data_adapter_app/frontend/tests/request.test.mjs) | Previously, most requests had no deadline, including while reading response bodies. Slow requests could leave controls waiting, overlap status polls, and finish after their UI was removed. | Every API helper now uses a shared 15-second deadline, with a 3-second health deadline, caller cancellation, and timer/listener cleanup. Obsolete reads are aborted, polling and verification skip overlapping runs, and timeout errors release action/submission guards without saving unconfirmed success. | Bounded waits and retryable controls during slow or unavailable backend conditions. Verified by 28 regression tests and component-handler probes. |
| **Fixed — Improvement — Action failures are only visible in the console** | [Row actions and status checks](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [Save and adapter loading](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [Restoration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx), [Restoration error panel](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionList.jsx), [API errors](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [Regression tests](mfi_ddb_data_adapter/data_adapter_app/frontend/tests/action-errors.test.mjs) | Previously, several failures only logged errors or discarded useful backend details. Failed adapter loading left an empty selector; a failed status lookup was displayed as a confirmed disconnection. Earlier fixes exposed save and action failures, but retries and loading failures were incomplete. | Adapter loading, status checks, row actions, Save, and restoration now have visible errors and retry paths. API errors preserve HTTP status and backend detail, failed status checks retain the last confirmed state, retries preserve form input, and processing guards release in `finally`. | Users can troubleshoot and retry without refreshing or opening developer tools. Verified by 26 regression tests and component-handler probes. |
| **Fixed — Improvement — Modal keyboard accessibility is incomplete** | [Modal](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/Modal.jsx), [ConnectionModal](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx) | Previously, the overlay lacked dialog semantics, initial focus, keyboard focus containment, Escape handling, and focus restoration. Keyboard focus could move into the background. | A native modal dialog has an accessible title, an initial focus target, explicit Tab/Shift+Tab wrapping, and an inactive background. Escape, Close, and backdrop dismissal respect the Save guard. Closing restores focus and scrolling. | Reliable keyboard navigation and dialog semantics, verified with the actual React components in Chrome, including disabled-control and pending-Save cases. |
| **Fixed — Improvement — Unused frontend dependencies and components add maintenance work** | [package.json](mfi_ddb_data_adapter/data_adapter_app/frontend/package.json), [package-lock.json](mfi_ddb_data_adapter/data_adapter_app/frontend/package-lock.json), [main.jsx](mfi_ddb_data_adapter/data_adapter_app/frontend/src/main.jsx) | Previously, Axios and js-yaml were unused, React Query had no queries or mutations, and SchemaForm was never rendered. Unreferenced styling helpers and component-generator scaffolding also retained unused packages. | Removed six direct dependencies, the unused QueryClient/provider, SchemaForm, the unreferenced utils helper, and its unused generator configuration. The lockfile drops 32 package entries without changing retained package versions. | Fewer dependencies and unused files to maintain. Clean installation, production build, entry-point lint, and production browser smoke checks pass. |

## Completed fixes

### 1. Pause / Resume polling lifecycle

Updated the backend factory used by the frontend's existing Pause and Resume controls. Pausing keeps the polling worker waiting without repeated wakeups; resuming wakes that worker instead of only changing the status flag. Disconnect signals shutdown and wakes a paused worker, and shutdown also interrupts polling error backoff. Each connection's worker retains its own streamer and shutdown signal so a previous in-flight worker cannot begin polling a replacement connection.

A poll already in progress may finish after Pause; subsequent polls wait until Resume.

Added four network-independent regression tests covering repeated pause/resume cycles, repeated Resume while a poll is in flight, disconnecting a paused worker, and reconnecting before the old worker finishes. The pre-fix run reproduced the missing polling after Resume and the old worker continuing after reconnect. All four tests pass after the fix.

Run from `mfi_ddb_data_adapter/data_adapter_app/backend`:

```sh
python3 -m pytest -p no:cacheprovider tests/test_adapter_factory.py -q
```

Result: **4 passed**. No live MQTT broker, browser session, or complete deployment was used for these regression tests.

### 2. Apply edited adapter settings to the backend

Updated the frontend edit flow to use the existing disconnect, connect, status, and pause endpoints. The connect endpoint reuses an existing connection ID without applying new settings, so the frontend removes the old instance before sending the edited configuration. This change does not modify backend code.

The modal now includes polling mode and frequency in the edited settings, waits for the backend update before closing, and displays save failures without blocking a retry. The connection manager checks both the HTTP response and the returned connection state before saving to browser storage. If replacement fails, it removes any partially created replacement and attempts to reconnect using the previous saved configuration and previous backend streaming state. Rollback failures are reported explicitly; previous settings remain saved. Network failures and non-404 status failures abort the edit rather than being treated as a missing connection.

Stopped adapters remain disconnected when edited. Paused adapters missing from the backend also remain inactive. For existing paused polling adapters, the replacement is paused immediately after connecting. The existing connect endpoint starts streaming, so a paused replacement can briefly stream before the pause request completes. Switching a paused adapter to Callback mode requires resuming it first because callbacks do not support pause. Active replacements require a brief reconnect interruption.

Connections being updated are excluded from this browser's periodic verification and restoration, and duplicate updates for the same ID are rejected. These guards apply within the current browser page.

Added 13 tests using Node's built-in test runner and mocked HTTP responses. Coverage includes changed adapter/configuration/rate, callback mode, paused and stopped adapters, missing connections, disconnect failure, HTTP and network connection failures, unsuccessful connection state, rollback, rollback failure, and duplicate-update/restoration guards. No additional test dependencies are required.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result: **13 passed**. The same command is available through `npm test`. The production build could not run because frontend dependencies are not installed (`vite: command not found`). Browser rendering and a live backend/MQTT deployment were not verified.

The earlier edit-only test file is not present in the current checkout; the result above records its validation when the edit fix was implemented.

### 3. Reject failed new-adapter creation

The new-adapter modal now awaits `ConnectionManager.createConnection` before calling its save/close callbacks. Creation reuses the connection checks used by editing and only writes to browser storage after an HTTP success with valid JSON and boolean `is_connected: true` and `is_streaming: true`. Backend error details are shown when available; non-JSON error responses retain their HTTP status, and malformed successful responses produce a readable error.

A failure leaves the modal open with its configuration intact and Save available for retry. Duplicate submissions for an ID already being created or updated are rejected, and an already saved ID cannot be overwritten by creation. Cancel no longer sends a disconnect request for an ID whose connection was never confirmed. If connection success is confirmed but browser storage fails, creation attempts to disconnect that connection and explicitly reports whether cleanup succeeded.

Added 16 tests covering successful creation, HTTP failures, false/missing/non-boolean connection flags, malformed responses, network errors, retry, storage failure and cleanup, duplicate submissions, and existing saved IDs. An isolated probe of the actual modal handler also verified that a failed attempt stays open and unsaved, while a successful retry saves settings and only then invokes the save/close callbacks. No backend code was changed.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result: **16 passed**. Frontend dependencies remain uninstalled, so a production build and browser rendering were not verified. Tests use mocked HTTP responses rather than a live backend or MQTT broker. Ambiguous network failures may leave an unconfirmed backend instance.

The earlier creation-test file is not present in the current checkout; the result above records its validation when the creation fix was implemented.

### 4. Preserve callback Stop / Reconnect intent

Stop and Reconnect now use separate connection-manager operations, guarded against concurrent edits and restoration. Stop disconnects the backend instance and saves `stopped` once without invoking the polling Pause handler. Reconnect uses the saved configuration, checks the backend's connected/streaming flags, and only then saves `streaming`. A failed Reconnect retains stopped intent. If the streaming state cannot be persisted after a successful reconnect, the manager attempts to disconnect again and reports cleanup failures explicitly.

The row initializes from the saved state before its first status check, so a stopped adapter immediately has a Reconnect control after refresh. Status polling skips stopped or changing connections and discards responses superseded by a later status request, Stop/Reconnect, or component cleanup. Stop/Reconnect failures appear on the row, and processing state is released in `finally` blocks so controls remain usable for retry.

Startup and periodic restoration skip stopped adapters. Restoration rereads intent after status requests to catch a Stop completed while a request was in flight, and the restore action uses the same operation guard as Stop/Reconnect. Legacy records where the old Stop handler saved `paused` for a callback adapter are interpreted as stopped; polling adapters retain the meaning of paused.

Added 16 network-independent lifecycle tests covering Stop and Reconnect success/failure, already missing backend instances, storage failures and cleanup, legacy records, polling pause compatibility, stopped-adapter verification, and concurrent operations. Isolated probes of the actual component handlers also verified stale status success/error responses after Stop, stopped-state initialization after refresh, Reconnect failure/retry, stopped/legacy startup and periodic restoration, and Stop completing during a restoration status lookup. No backend code was changed.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result at implementation: **16 passed**. Frontend dependencies were uninstalled, so production build and browser rendering were not verified. Validation used mocked HTTP responses and component-handler probes rather than a live backend/MQTT deployment. The earlier lifecycle-test file is not present in the current checkout; current recovery validation is recorded below.

### 5. Recover missing backend connections independently

Verification now catches failures for each connection and returns a `not_found` result for HTTP 404 or an `error` result for other failures, alongside successful status responses. Status payloads are checked for boolean connected/streaming flags so invalid data does not trigger recreation. Stopped adapters and connections with an operation in progress remain excluded.

Startup and periodic recovery use `ConnectionManager.restoreConnection`. It acts only on saved streaming intent, rereads intent and configuration after status lookup, and recreates an adapter only after a confirmed 404. Existing inactive instances use Resume; a 404 from that request also permits recreation, covering a backend restart between lookup and Resume. Network failures, other HTTP errors, and malformed responses retain the existing saved configuration and intent. Recovery errors for one ID do not stop attempts for the other IDs. An in-progress restoration guard prevents overlapping restoration runs, and restoration indicators are cleared in `finally`.

The polling Resume button now uses a guarded connection-manager operation. A missing backend instance is recreated with its saved adapter/streamer configuration, polling mode, and rate. Streaming is confirmed before the row and saved intent change. Failure keeps the paused state, displays an error, and releases the controls for retry. Automatic restoration leaves paused adapters inactive until explicit Resume. Pause uses the same per-connection operation guard, preventing a user pause from racing automatic recovery; the manager owns persistence for both actions.

Added 25 tests using mocked HTTP responses, covering mixed verification results, invalid status data, restoration of missing/inactive/healthy instances, restart between lookup and Resume, preservation of paused/stopped intent, transient failures, retry, intent/configuration changes during lookup, explicit Resume recreation, malformed Resume responses, operation guards, and callback Reconnect compatibility. Isolated probes of the actual App and row handlers also verified periodic recovery despite another connection's failure, continuation after a restore error, overlapping-run suppression, and Resume failure/retry while an older status request was still in flight. No backend code was changed.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result: **25 passed**. Frontend dependencies remain uninstalled, so production build and browser rendering were not verified. Validation used mocked HTTP responses and component-handler probes rather than a live backend/MQTT deployment. Request timeouts are addressed in fix 8 below.

### 6. Display useful configuration validation errors

Updated both backend validation endpoints and the frontend form. Successful and schema-invalid responses keep the existing `is_valid` field and include an `errors` array containing field locations, readable messages, and error types. Nested fields and list entries are preserved, such as `mqtt.broker_port` and `topics[0].component_id`. Rejected input values and validator context are excluded from responses. Validation resolves schemas without constructing adapters or streamers.

Malformed YAML returns structured HTTP 400 details with line/column positions, and missing or non-dictionary YAML and unsupported adapters have specific input errors. Uploaded YAML uses the same validation path. File encoding errors are reported as input problems; unexpected parser, file-read, and schema failures return HTTP 500 service errors with useful retry messages. Existing configuration errors are preserved rather than wrapped repeatedly.

The frontend normalizes structured validation responses and earlier boolean-only responses. Adapter and streamer validation results are collected independently and displayed together with clear section labels. Network failures, server errors, malformed responses, and authorization failures produce separate request-error messages. A service failure allows Save to retry validation without changing YAML; saving remains blocked until both validations succeed. Input errors disable Save until corrected. The error panel has alert semantics, and local missing-input errors are visible.

Backend validation tests exercise the real FastAPI routes with test schemas and no network-facing adapter instances. Coverage includes valid configurations, nested/list field errors, custom validator messages, YAML syntax positions, root input errors, unsupported adapters, uploaded YAML, file encoding, and service failures. Frontend tests cover error formatting, response compatibility, field errors, YAML positions, and request-failure classification. Isolated probes of the actual modal handler confirm combined errors, simultaneous input/service failures, retry without edits, required-input messages, and saving only after both validations pass.

Run with backend requirements installed, from `mfi_ddb_data_adapter/data_adapter_app/backend`:

```sh
python3 -m pytest -p no:cacheprovider tests/test_validation.py tests/test_adapter_factory.py -q
```

Result: **21 passed** (17 validation tests and 4 existing lifecycle tests). The test client emitted one dependency deprecation warning. A cached `python-multipart` package was used for local route tests.

From `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test tests/config-validation.test.mjs
```

Result at implementation: **15 passed**. Frontend dependencies remained uninstalled, so production build and browser rendering were not verified. The earlier validation-test file is not present in the current checkout. Stale validation responses are addressed in fix 7 below.

### 7. Ignore superseded configuration validation

Adapter and streamer validation share an abort signal and request generation. Editing either YAML field, clearing configuration, changing adapter, selecting or loading a YAML file, and resetting the form immediately invalidate prior validation, including during the 800 ms debounce delay. A superseded batch cannot change errors, loading state, or the cache of completed validation, even if its responses arrive after cancellation. Closing or unmounting the modal cancels outstanding validation, and cancellation is not displayed as a service failure.

Save starts a fresh validation batch and verifies that its result is still current before creating or updating a connection. A synchronous submission guard prevents duplicate Save calls and stops a pending debounce callback from superseding submission validation. Debounce cleanup clears its timer without cancelling the active Save validation. Only completed, current validation updates the cache.

Added 13 tests using Node's built-in runner covering cancellation, out-of-order responses, loading-state ownership, restart after cleanup, optional API cancellation, and preservation of ordinary network errors. Isolated probes of the actual modal handlers and effects also verify late valid/invalid results, edits during the debounce delay, adapter and file changes, close/unmount cleanup, Save versus background validation, duplicate submissions, and refusal to save superseded input. No backend code was changed.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result at implementation: **13 passed**, with all modal-handler probes passing. Frontend dependencies remained uninstalled, so production build and browser rendering were not verified. Tests used controlled responses rather than a live backend. The earlier validation-request test file is not present in the current checkout. General request timeouts are addressed in fix 8 below.

### 8. Bound API waits and cancel obsolete read requests

All frontend API helpers now share a request wrapper with a 15-second deadline; health checks retain a shorter 3-second deadline. Callers can supply an abort signal and override the deadline. The deadline covers response consumption as well as waiting for headers. Connect now returns parsed data to the connection manager so its JSON body cannot be read outside that deadline. Cancellation and timeouts settle the caller even if a transport ignores abort. Timers and abort listeners are released after success, failure, or cancellation.

Connection rows skip polling ticks while a status request is pending and abort that request before actions or effect cleanup. Adapter-list loading, health checks, verification, and restoration preflight reads also support cleanup cancellation. Health and verification runs cannot overlap themselves, and cancelled checks cannot start restoration. Late results cannot update removed components. Existing validation generation checks remain in place.

Timeouts propagate to the existing form and row error displays. Validation timeouts are request failures, allowing Save to retry without YAML edits. Pause, Resume, Stop, Reconnect, and Save release their existing processing/operation guards on failure. Delete now awaits disconnection, displays failures, releases its processing state in `finally`, and retains saved settings until disconnection succeeds. Timeouts are not treated as a confirmed 404 or a successful backend action. Edit failures still follow the existing cleanup and rollback path.

Requests that change connections have bounded waits and are allowed to finish their persistence or recovery work after their UI is removed. Aborting a browser request cannot undo work already accepted by the backend, so a mutation timeout asks the user to check connection status before retrying. Multi-step edits and recovery can take several per-request deadlines; there are no automatic mutation retries.

Added 28 tests using Node's built-in runner with controlled fetch responses and timers. Coverage includes stalled transports and JSON bodies, ignored cancellation, pre-cancelled requests, cleanup, every API helper, health retry, validation compatibility, creation retry, preservation of saved intent, independent verification failures, cancelled restoration preflight, and edit rollback after timeout. Isolated probes of the actual App, modal, and row handlers/effects verify polling and verification overlap suppression, cleanup, late results, timeout messages, and released controls. The previous modal validation race probes also pass. No backend code was changed.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result at implementation: **28 passed**, with all component-handler probes passing. Frontend dependencies remained uninstalled, so production build and browser rendering were not verified. Tests used mocked HTTP responses rather than a live backend or MQTT deployment. The earlier request-test file is not present in the current checkout.

### 9. Show action and loading failures with retry controls

Earlier fixes already exposed Save and row-action failures and released their processing state. This change completes the remaining error paths and adds explicit retries. API helpers now preserve HTTP status and readable backend details, including when an error response contains HTML rather than JSON. Network failures provide connection/retry guidance. Pause and Disconnect reject unconfirmed success instead of returning a false value that loses the failure detail; the existing connection-manager recovery paths still handle those rejections.

Adapter loading now shows loading, failure, and empty-list states with a Retry loading adapters button. Invalid adapter-list responses become visible load failures. Save waits for the selected adapter's metadata, preventing an edit from guessing callback support after metadata loading fails. Reloading preserves the selected adapter, YAML, polling mode, and rate; cancelled loads stay silent. Save failures retain the form and expose Retry Save, including unexpected rejections without an error message. New-adapter initial data remains stable across App renders so background state updates do not reset unsaved input.

Each row shows contextual action errors and a button that retries the failed action. Delete retry retains the existing confirmation step. A synchronous action guard prevents duplicate clicks from releasing processing state while the original action remains pending. Failed status checks display a separate error and Retry status check control while retaining the last confirmed status; they no longer claim that a request failure confirms disconnection. A successful check clears its error. Action controls release in `finally`, and removed rows ignore late results.

Restoration errors are grouped by connection above the list with Retry restore. Other adapters continue restoring after one failure. Unexpected restoration and retry failures are displayed, and loading state is released. A completed startup check with no restoration needed does not schedule repeated automatic attempts. No backend code was changed.

Added 26 tests using Node's built-in runner covering backend details, HTTP/HTML errors, network failure and retry for every action API, 404 compatibility, unconfirmed success, invalid/empty adapter lists, cancellation, and saved-intent/guard preservation. Isolated probes of the actual App, modal, and row handlers verify adapter-load retry without losing YAML or callback mode, cancelled loads, Save failure/retry, retrying each row action, duplicate-action suppression, status failure/recovery, and partial or unexpected restoration failures.

Run from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
node --test
```

Result at implementation: **26 passed**, with all component-handler probes passing. Frontend dependencies remained uninstalled, so production build and browser rendering were not verified. Tests used controlled responses rather than a live backend or MQTT deployment. The earlier action-error test file is not present in the current checkout.

### 10. Make the modal accessible from the keyboard

Replaced the generic overlay with a native `<dialog>` opened through `showModal()`. Its implicit dialog role, `aria-modal`, and a generated `aria-labelledby` reference expose the visible title as its accessible name. The browser makes background content inactive. Opening focuses the title so the top of the long configuration form remains the starting point; normal state updates do not reset focus.

Tab and Shift+Tab wrap between the currently enabled, visible controls. The list is evaluated on each key press so loading states, retry buttons, and disabled controls are handled as they change. Hidden controls, negative tab indexes, and controls disabled through a fieldset are excluded. With no enabled controls, focus remains on the title. Browser shortcut modifiers are left alone. Native background isolation also prevents programmatic focus from moving to background controls.

Escape uses the same controlled close path as the labelled Close dialog button and backdrop dismissal. `ConnectionModal` supplies its submission state so those dismissal routes and the Close button are disabled while Save is pending; the existing synchronous Save guard remains in place. Starting Save moves focus from the newly disabled control to the title. A failed Save restores dismissal. Cleanup closes the native dialog, restores the previous body overflow setting, and returns focus to the opening element when it still exists. Reopening and React Strict Mode cleanup/setup are supported. No backend code was changed.

Automated checks ran against the actual Modal and ConnectionModal source bundled with React in a temporary fixture, using headless Chrome **155.0.8059.80** and mocked API responses. All four scenario groups passed:

- Named New/Edit dialogs, initial title focus, forward/reverse Tab wrapping, inactive background, Escape, and focus/scroll restoration under Strict Mode.
- Backdrop, Close, and Cancel dismissal; correct focus return to New/Edit opening controls across repeated opens.
- Focus preservation during rerenders, pending Save with all controls disabled, blocked Escape/backdrop dismissal during Save, and dismissal after a rejected Save.
- A dialog with no enabled controls, including hidden and fieldset-disabled controls, plus controlled unmount and no React/page errors.

The browser checks reproduced reverse Tab leaving the initial native-only implementation; explicit wrapping fixed it. Test tooling and generated fixtures were kept outside the repository, using cached dependencies without changing the project's manifests. The components compiled successfully for these checks. The full production build, production stylesheet rendering, Firefox/Safari, a screen reader, and a live backend deployment were not verified during this fix. Production build and stylesheet checks were subsequently completed in fix 11 below.

### 11. Remove unused frontend packages and scaffolding

Checked imports across frontend source and build configuration. Removed `@tanstack/react-query`, `axios`, `js-yaml`, `class-variance-authority`, `clsx`, and `tailwind-merge`. App now renders directly under StrictMode without an unused QueryClient or provider. The remaining direct runtime dependencies are React, React DOM, and Lucide React; the existing build, CSS, and lint tools remain in place.

Deleted the unrendered `SchemaForm.jsx`, the unreferenced `src/lib/utils.js` styling helper, and the unused `components.json` generator configuration that pointed to that helper. The active YAML form and styling remain in place. No imports or active frontend configuration reference the removed packages or files.

Updated package-lock.json, removing 32 locked package entries. Retained package versions are unchanged, and optional build-tool dependencies remain locked. A clean installation succeeded from the npm cache, followed by a successful production build with Vite. No backend code was changed.

Validation from `mfi_ddb_data_adapter/data_adapter_app/frontend`:

```sh
npm ci --offline --no-audit --no-fund
npm run build
./node_modules/.bin/eslint src/main.jsx
```

All three checks pass. Automated headless Chrome checks also loaded the actual production bundle and stylesheet with mocked API responses. Four scenario groups passed: application startup without React Query; modal focus containment, Escape, and focus restoration; connection creation, validation and browser persistence; and Pause, Resume, and confirmed Delete. No browser runtime errors occurred. A live backend/MQTT deployment was not tested.

`npm run lint` now executes and reports **10 errors in unchanged files**: five `react-hooks/set-state-in-effect` findings, three `preserve-caught-error` findings, an unused argument in `conn_ctr.js`, and `__dirname` in the ESM Vite configuration. These findings predate this cleanup; the modified entry point passes lint. They remain separate follow-up work beyond the eleven original review items.

## Original review validation

Source inspection and isolated behavior probes confirmed the failed-save flow, ignored edit settings, conflicting stopped-state writes, restoration of a saved stopped connection, rejection of the verification batch when one connection is missing, and polling remaining stopped after Resume while `is_streaming` is true.

Frontend build and lint could not execute during the original review because the environment returned `SecItemCopyMatching failed -50`. Browser rendering and a complete live deployment were not verified during that review. All eleven retained review issues are now fixed and verified as described above. The production build and mocked browser smoke checks pass; the additional lint findings recorded in fix 11 remain open.
