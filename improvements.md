# Data Adapter Frontend Improvements

Review scope: the data adapter frontend on `feature/performance-testing`, including backend behavior that affects frontend controls and deployment.

Progress: **5 of 13 items fixed**. The remaining items are pending.

| Issue title | Location | Why is it a problem? | Expected result after fixing it | Benefit |
| --- | --- | --- | --- | --- |
| **Fixed — High — Pause / Resume does not restart polling** | [AdapterFactory.connect_and_stream / resume_streaming / pause_streaming / disconnect](mfi_ddb_data_adapter/data_adapter_app/backend/app/services/adapter_factory.py), [Lifecycle regression tests](mfi_ddb_data_adapter/data_adapter_app/backend/tests/test_adapter_factory.py) | Previously, pausing allowed the polling thread to exit. Resume set the event and `is_streaming` flag without restarting the worker, so the UI could show streaming while no data was being polled. | The worker now waits on a condition while paused and wakes on Resume. A separate shutdown event stops it on Disconnect. Each worker retains its own shutdown event and streamer so it cannot reactivate against a replacement connection. | Polling resumes on the existing worker without duplicate workers, and paused workers can shut down cleanly. Verified by four lifecycle regression tests. |
| **Fixed — High — Edits do not update the running connection** | [ConnectionModal.handleSave](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [ConnectionManager.updateConnection](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js) | Previously, editing only updated browser storage. The backend kept its previous configuration, and polling mode and frequency changes were discarded. | Edits now disconnect and replace the backend instance using the selected adapter, YAML configurations, mode, and frequency. Settings are persisted after success. Failures keep the modal open and attempt to restore the previous saved configuration. This browser's restoration logic skips connections being edited. | Saved settings match the replacement connection. Stopped adapters stay disconnected, paused polling adapters are paused again after reconnecting, and failed edits retain the previous saved settings. Verified by 13 frontend regression tests at implementation. |
| **Fixed — High — Failed connection attempts are saved as successful** | [ConnectionModal.handleSave](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [ConnectionManager.createConnection / connectWithState](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js) | Previously, HTTP failures still saved an adapter and closed the modal because the connect response was ignored. | Creation now checks HTTP success, valid JSON, and explicit connected/streaming flags before persisting settings or closing the form. Failures stay visible for retry. Confirmed connections are disconnected if saving to browser storage fails, with cleanup failures reported explicitly. | Prevents false success, preserves the form on failure, and allows retry after a rejected connection. Verified by 16 regression tests and a modal-handler probe at implementation. |
| **Fixed — High — Callback Stop / Reconnect state is inconsistent** | [ConnectionItem actions and status polling](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [ConnectionManager lifecycle methods](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js), [App restoration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx) | Previously, Stop's `stopped` state was overwritten by the pause callback, late status responses could overwrite the stopped display, and restoration could reconnect stopped adapters. | Stop now persists only `stopped`; Reconnect persists `streaming` after confirmed success. Initial row state uses saved intent, obsolete status results are ignored, and verification/restoration skip stopped adapters. Legacy callback `paused` records are interpreted as stopped. | Stopped adapters retain a working Reconnect control after refresh and stay inactive until explicit reconnection. Failed actions remain visible and retryable. Verified by 16 lifecycle tests and component-handler probes at implementation. |
| **Fixed — Medium — Recovery fails when a backend connection is missing** | [ConnectionManager verification, restoration, and Resume](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionManager.js), [Status and Resume API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [App restoration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx), [ConnectionItem actions](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [Recovery tests](mfi_ddb_data_adapter/data_adapter_app/frontend/tests/connection-recovery.test.mjs) | Previously, one rejected status request aborted the entire verification batch. A paused adapter lost during a backend restart could not resume because its backend instance no longer existed. | Verification returns independent results per ID. Restoration handles failures per connection, and recreation follows a confirmed 404. Explicit Resume recreates missing instances from saved configuration, confirms streaming, and then updates saved intent. Paused/stopped adapters remain inactive during automatic restoration. | Missing adapters can recover without blocking healthy adapters, and paused adapters can resume after a backend restart. Failed recovery retains saved intent and remains retryable. Verified by 25 recovery tests and App/row handler probes. |
| **Medium — Connection IDs collide across browsers** | [getConnCtr](mfi_ddb_data_adapter/data_adapter_app/frontend/src/state/conn_ctr.js), [ConnectionModal.handleSave](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [connect_endpoint](mfi_ddb_data_adapter/data_adapter_app/backend/app/api/v0/router.py) | IDs are derived from browser-local storage, so two fresh browsers both start at `101`. For a matching adapter type, the backend can reuse the existing connection and ignore the second browser's configuration. | Use backend-generated IDs or UUIDs, and reject unintended ID collisions rather than silently reusing another connection. | Different browsers can create connections without targeting each other's adapters. |
| **Medium — Standalone Docker quick start is broken** | [Standalone Compose configuration](mfi_ddb_data_adapter/data_adapter_app/compose.yaml), [Application README](mfi_ddb_data_adapter/data_adapter_app/README.md), [Frontend Dockerfile](mfi_ddb_data_adapter/data_adapter_app/frontend/Dockerfile), [Nginx configuration](mfi_ddb_data_adapter/data_adapter_app/frontend/nginx.conf) | Compose references nonexistent Dockerfiles, maps the frontend to container port 3000 although Nginx listens on 80, and supplies `VITE_API_URL` at runtime although Vite reads it during the build. The documented setup also differs from the actual configuration. | Align build contexts, Dockerfile paths, ports, API routing, and documentation with the deployment under `docker/data-adapter-app/`. Use the same-origin proxy or configure the API URL at build time. | A reproducible local setup and fewer deployment configuration failures. |
| **Improvement — Validation errors lack useful detail** | [validateAdapterConfig / validateStreamerConfig](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [ConnectionModal.validateConfiguration](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [Validation endpoints](mfi_ddb_data_adapter/data_adapter_app/backend/app/api/v0/router.py) | Validation is reduced to a boolean and generic error text. Users cannot easily identify which YAML field is invalid or distinguish invalid input from an unavailable service. | Return and display structured field errors and useful request failure details, extending the backend validation responses where necessary. | Faster configuration troubleshooting and fewer trial-and-error edits. |
| **Improvement — Stale validation responses can overwrite newer results** | [ConnectionModal.validateConfiguration and debounced validation effect](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx) | Debouncing cancels pending timers but does not cancel validation requests already in flight. An older response can update the error or validation state after the user changes the configuration. | Cancel superseded requests or use a request version so only the latest configuration's validation can update the form. | Error messages and Save availability reflect the current input. |
| **Improvement — Most API requests have no timeout or cancellation** | [Frontend API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js), [ConnectionModal](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [ConnectionItem](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx) | Outside the health check, requests have no explicit timeout or abort mechanism. Slow requests can leave actions waiting, allow status polls to overlap, and continue after a component closes. | Add shared timeout and cancellation support, abort obsolete requests, and consistently release loading states when requests finish or fail. | Responsive controls and bounded request work during slow or unavailable backend conditions. |
| **Improvement — Action failures are only visible in the console** | [ConnectionItem action handlers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionItem.jsx), [ConnectionModal.handleSave and adapter loading](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx), [App.handleTerminateConnection](mfi_ddb_data_adapter/data_adapter_app/frontend/src/App.jsx), [Frontend API helpers](mfi_ddb_data_adapter/data_adapter_app/frontend/src/api.js) | Pause, resume, reconnect, delete, adapter loading, and save failures often only log an error or return false. Some action paths also lack `try/finally`, so an unexpected rejection can leave controls disabled. | Show contextual errors with retry options, expose adapter loading failures, and reset processing state in `finally` blocks. | Users can understand failures and recover without opening developer tools or refreshing the page. |
| **Improvement — Modal keyboard accessibility is incomplete** | [Modal](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/Modal.jsx), [ConnectionModal](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/ConnectionModal.jsx) | The modal lacks dialog semantics, managed initial focus, a focus trap, Escape handling, and focus restoration. Keyboard focus can move into the background while the modal is open. | Add an accessible dialog name and semantics, contain focus, support Escape when dismissal is allowed, and return focus to the opening control. | More reliable keyboard navigation and screen-reader use. |
| **Improvement — Unused frontend dependencies and components add maintenance work** | [package.json](mfi_ddb_data_adapter/data_adapter_app/frontend/package.json), [main.jsx](mfi_ddb_data_adapter/data_adapter_app/frontend/src/main.jsx), [SchemaForm](mfi_ddb_data_adapter/data_adapter_app/frontend/src/components/SchemaForm.jsx) | Axios and js-yaml are not used by the current frontend source. React Query is initialized without queries or mutations, and SchemaForm is not rendered. These add dependencies or code without supporting a current UI flow. | Remove unused packages, providers, and components, or integrate them where they replace existing duplicated behavior. Update the lockfile with dependency changes. | A clearer codebase, fewer packages to maintain, and potentially smaller production bundles. |

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

Connections being updated are excluded from this browser's periodic verification and restoration, and duplicate updates for the same ID are rejected. These guards apply within the current browser page; coordination across multiple browsers remains part of the pending connection-ID issue.

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

Result: **16 passed**. Frontend dependencies remain uninstalled, so a production build and browser rendering were not verified. Tests use mocked HTTP responses rather than a live backend or MQTT broker. Connection-ID collisions across browsers and ambiguous network failures that may leave an unconfirmed backend instance remain limitations of the existing API/ID scheme; the connection-ID issue remains pending.

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

Result: **25 passed**. Frontend dependencies remain uninstalled, so production build and browser rendering were not verified. Validation used mocked HTTP responses and component-handler probes rather than a live backend/MQTT deployment. Coordination across browsers and request timeouts remain separate pending items.

## Original review validation

Source inspection and isolated behavior probes confirmed the failed-save flow, ignored edit settings, conflicting stopped-state writes, restoration of a saved stopped connection, rejection of the verification batch when one connection is missing, and polling remaining stopped after Resume while `is_streaming` is true.

Frontend build and lint could not execute during the original review because the environment returned `SecItemCopyMatching failed -50`. Browser rendering and a complete live deployment were not verified. The first five issues are now fixed and verified as described above; the remaining items are a review backlog.
