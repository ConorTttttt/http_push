# HTTP push relay protocol, v1 (provisional)

Wire types live in `crates/protocol` (`hc_push_protocol`), which has no async
runtime or HTTP dependency and is shared by relay and client. HTTP semantics
follow RFC 9110. All bodies are JSON (`application/json`), except the SSE
stream (`text/event-stream`).

Three separate mechanisms:

| Mechanism | Endpoint | When | Source of truth? |
|---|---|---|---|
| Foreground notification | `GET /v1/events` (SSE) | App is open | No: latency optimisation only |
| Durable catch-up | `GET /v1/sync` | After reconnect, on wake-up, periodically | **Yes** |
| Wake-up of a suspended app | *(future, optional)* Web Push / APNs / FCM adapter | OS permitting | No: content-free "go sync" nudge |

A missed SSE event or push message can never cause permanent loss. The
client's next `/v1/sync` from its stored cursor returns everything still
retained, or an explicit `410` if it was pruned.

## Concepts

* **Space**: a DNA hash (`uhC0k…`, the Kitsune2 space). Validation:
  1–128 chars of `[A-Za-z0-9_-]`.
* **Journal**: a relay's append-only, bounded event log with a random
  `journal_id` (16 hex chars), created with the database.
* **Sequence (`seq`)**: monotonic per journal, never reused (SQLite
  `AUTOINCREMENT`), and gap-free for accepted events.
* **Cursor**: `v1.<journal_id>.<seq>`, opaque to clients. It means "I have
  processed everything up to and including `seq`." **Cursors belong to one
  journal.** Another relay, or this relay after its database was recreated,
  answers `410 cursor_unknown_journal`.
* **Change key**: relay-independent identity of a change,
  `hex(SHA-256(space 0 kind 0 (ref_type 0 ref_id 0)*)[..16])`. Two relays
  observing the same DHT op produce the same key, so clients dedup across
  relays with it.

## Event envelope

```json
{
  "v": 1,
  "event_id": "b704c3baeab1e9f9-14",
  "cursor": "v1.b704c3baeab1e9f9.14",
  "space": "uhC0kPQEHCyrIo3XGvsHjYMJU9sNw1AekUKvc0ID_8pFYs8EB275g",
  "kind": "op_integrated",
  "change_key": "1de9e922e110dacbdf87672f36031d45",
  "refs": [{ "type": "dht_op", "id": "uhCQkUmU0uwXh5FJRhmV45cGIeEI_r3eeQt149NlTBO9wmzkV5i_Y" }],
  "observed_at_ms": 1795050420681,
  "source": "holochain-op-timings"
}
```

| Field | Meaning |
|---|---|
| `v` | Protocol version |
| `event_id` | `<journal_id>-<seq>`, unique per relay |
| `cursor` | Cursor *at* this event; persist after processing |
| `space` | DNA hash |
| `kind` | `op_integrated`, `op_rejected`, `app_signal`, `mock_change`; unknown kinds deserialize as `unknown` and should be treated as "something changed in `space`" |
| `change_key` | See above |
| `refs` | Opaque references: `dht_op` (DhtOpHash), `digest` (`sha256:<hex>` of a payload the relay does not forward), `mock` |
| `observed_at_ms` | Relay clock, diagnostics only; never order by it |
| `source` | Change source id (`holochain-op-timings`, `holochain-app-signals`, `mock`) |

Envelopes never contain DHT payloads, entry content, agent keys or signal
payloads.

## Endpoints

### `GET /v1/health` (public)

`200 {"status":"ok"|"degraded","journal_id":…,"head_cursor":…,"sources":[{"id","state","last_ok_at_ms"}]}`.
Always `200` while the HTTP layer works. `degraded` means a source is
disconnected (e.g. the conductor is restarting). Error details go to logs
only.

### `GET /v1/capabilities` (public)

Protocol versions, `relay_id`, `journal_id`, served `spaces` (unless
`HC_PUSH_ADVERTISE_SPACES=false`), `sources` with **`fidelity`** (`mock`,
`conductor_op_level`, `conductor_app_signal`), retention limits, request
limits, transports (`["sse","sync"]`) and registration mode (`key`|`open`).
Clients use it to judge coverage. Different relays may serve different
spaces and have different retention.

### `POST /v1/subscriptions`

* Auth: `Authorization: Bearer <registration key>` in `key` mode; none in
  `open` mode. Rate-limited per client address, including failed attempts.
* Body (unknown fields rejected; ≤ 16 KiB):
  `{"spaces":["uhC0k…"],"label":"optional, ≤64 chars"}`.
* `201 Created`, `Location: /v1/subscriptions/{id}`, `Cache-Control: no-store`:

```json
{ "subscription_id":"sub_…", "token":"hcp_…", "spaces":[…],
  "cursor":"v1.<jid>.<head>", "earliest_cursor":"v1.<jid>.<pruned_through>",
  "idle_ttl_secs":2592000 }
```

The token is shown once; the relay stores only its SHA-256. Subscriptions
expire after `idle_ttl_secs` without use. Errors: `400` (malformed, empty or
too many spaces, unknown field), `401`, `403` (space not served), `413`,
`429`, `503 capacity`.

### `DELETE /v1/subscriptions/{id}`

Bearer subscription token. `204`; `403` if the token belongs to another
subscription.

### `GET /v1/sync?cursor=<cursor>&limit=<1..500>`

Bearer subscription token. Returns events with `seq > cursor` in the
subscription's spaces, ascending:

```json
{ "events":[…], "next_cursor":"v1.<jid>.N", "head_cursor":"v1.<jid>.H", "has_more":false }
```

* No `cursor`: start at the oldest retained position.
* `next_cursor` can be ahead of the last returned event when later events
  belong to other spaces. Always persist `next_cursor`.
* `has_more: true`: call again with `next_cursor`. Clients bound pages per
  wake-up (`max_pages_per_sync`).
* Errors:
  * `400`: malformed cursor, bad limit, unknown parameter.
  * `410 {"error":{"code":"cursor_expired","earliest_cursor":…}}`: events
    after the cursor were pruned. The client must reconcile those spaces
    through Holochain, then continue from `earliest_cursor`.
  * `410 cursor_unknown_journal`: the cursor is from another journal, or
    ahead of this relay's head. Same recovery.
  * `401`, `429` (with `Retry-After`).

### `GET /v1/events` (SSE)

Bearer subscription token. Start position: the `Last-Event-ID` header (sent
automatically by reconnecting `EventSource`s), else `?cursor=`, else "now".

| SSE event | `id` | `data` |
|---|---|---|
| `change` | the event's cursor | envelope |
| `sync_required` | none | `{"reason":"backlog_too_large"|"lagged"|"shutdown","cursor":…}`; the stream then ends |
| comment (`: …`) | | keep-alive every 15 s |

* On connect the relay replays up to `sse_replay_limit` (200) events after
  the start cursor. If more are pending it sends `sync_required` instead, so
  bulk catch-up always goes through paginated `/v1/sync`.
* A client that falls behind the live broadcast gets `sync_required:lagged`.
* Limits: a global stream cap and a per-subscription cap (`503 capacity`).
  Streams are excluded from the request timeout.
* Proxies must not buffer: the relay sends `X-Accel-Buffering: no` and the
  Caddy config uses `flush_interval -1`.

## Errors

```json
{ "error": { "code": "cursor_expired", "message": "…", "earliest_cursor": "v1.…" } }
```

Codes: `bad_request`, `unauthorized` (with `WWW-Authenticate: Bearer`),
`forbidden`, `not_found`, `cursor_expired`, `cursor_unknown_journal`,
`payload_too_large`, `rate_limited`, `capacity`, `internal`, `unavailable`.

## Client algorithm (reference: `crates/client`)

```
on wake / reconnect / timer:
  for relay in priority order (or all, for coverage):
    loop up to max_pages:
      page = GET /v1/sync?cursor=state[relay].cursor
        401  -> re-register once; keep the old cursor if journal_id is unchanged
        410  -> emit GapNotice(spaces) to the app; cursor = earliest_cursor
        5xx / network / 429 -> bounded retry with backoff, then next relay
      for e in page.events (ascending):
        if e.change_key not in seen: app.handle(e)    # failure: stop, keep cursor
        seen.add(e.change_key); state[relay].cursor = e.cursor
      state[relay].cursor = page.next_cursor; persist atomically
      break if !page.has_more
  -> idle: no sockets or tasks stay open
```

`seen` is a bounded FIFO (default 10 000 keys), persisted with the cursors.

## Versioning

New optional fields may appear in any v1 response; clients must ignore
unknown fields. Unknown `kind`/`type` values deserialize to `unknown`.
Breaking changes use `/v2` and a new entry in `protocol_versions`.
