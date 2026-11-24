# The conformance contract (version 1)

This page is for anyone writing a Snodra library and for the conformance runner's maintainers: it specifies the
**conformance service** a library under test runs, the fixed `conformance.*` message types it handles, and how the
runner, `snodra-conformance`, proves each check of [feature level 1](https://snodra.damoreira.com/library-authors/feature-levels/#level-1-core)
with them. After reading it you can write the conformance service for your library and know what every check does to
it. How to run the runner and read its report is in [the conformance page](https://snodra.damoreira.com/library-authors/conformance/).

The JSON Schema of every message type (its payload, its answer, and for a stream or a subscription its items) is in
`contract/`, one file per type, named after the type (for example
[`conformance.Send.json`](../contract/conformance.Send.json)). [`hub.proto`](https://github.com/snodra/snodra-server/blob/main/proto/hub.proto) stays the source of truth for
the protocol; this page adds only what the conformance service must do on top of it.

## Versions

The contract has a version, `1`; the runner prints the version it checks and its own. A change that a conformance
service written for the current version would fail is a new contract version: a type or a field the service must
handle, a changed behaviour or answer, a renamed variable. Adding a check that uses only what the service already
does, tightening a check to the rule it proves, or an optional field is a new runner version only.

## How a run works

The runner starts the library's conformance service itself, once or more per check, with a fresh instance id each
time, in a process group of its own. Before any check a **pre-flight** start must register on a fake hub and stop on
SIGTERM within 5 s; a service that cannot ends the run before the checks. When a check ends, the runner stops its
instances the same way: SIGTERM to the group, a kill after 5 s. An instance that dies during a check fails it at once
with its exit status and the path of its log.

Each check connects the instance to one of:

- **a real hub** (`--hub`, plain text, access control off), for what only a hub shows: routing among instances and
  services, scatter-gather fan-out, shared types, eviction; the runner registers its own raw instance there (service
  `conformance-runner`) to send to the service and to serve the runner's types;
- **fake hubs**, gRPC servers speaking hub.proto inside the runner. A fake hub sees every frame the library writes and
  does what the check scripts: it acknowledges or not, goes silent, refuses a `Register`, closes the stream, refuses
  connections for a while (a restart answering `UNAVAILABLE`, and a process gone, the port refusing TCP), sends
  requests, `Flow` and `Cancel`, and answers the library's own requests with any error code, a part gap or a short
  end. Elsewhere it behaves as snodra-server does (the handshake and its refusal statuses, the echo after the ack,
  forwarding within the library's permits and queueing the rest, later and empty `Register`s, `NO_HANDLER` for a type
  nobody serves, no `Cancel` for a command). It forwards requests snodra-server would refuse, such as a request of a
  subscription type in another mode, so the library's own answer is checked; the remaining differences are listed in
  [the conformance page](https://snodra.damoreira.com/library-authors/conformance/#how-the-runners-hub-differs-from-snodra-server), and
  the runner's tests run one script of frames against both to keep them equal.

The runner drives the library in two directions. As a **sender** it sends the service's `conformance.*` types and reads
the answers. To make the **library** send, it sends `conformance.Send`: the service sends a request through its library,
as an application would, and reports the outcome in a fixed shape. The runner serves the targets of those sends, the
`conformance.runner.*` types.

## Starting the service

The runner runs `--service-cmd` without a shell (words split at blanks, `'...'` and `"..."` quoting, `\` escaping;
no variables or redirections: a wrapper script does those) with these variables; the service maps them onto its
library's configuration. A variable that is absent is not configured. The service must stop within 5 s of SIGTERM:
the runner stops every instance that way, and its pre-flight ends the run when the service does not.

| Variable | Meaning |
|---|---|
| `CONFORMANCE_SERVICE_NAME` | `Register.service_name` (`conformance-<library>`) |
| `CONFORMANCE_INSTANCE_ID` | `Register.instance_id` |
| `CONFORMANCE_HUBS` | the hubs, comma-separated `host:port`, in order; connect to every one |
| `CONFORMANCE_CAPACITY` | the handlers the instance runs at once: `Register.initial_permits` with one hub |
| `CONFORMANCE_MAX_SUBSCRIPTIONS` | the subscriptions the instance serves at once (its own limit, beyond which it refuses with `RESOURCE_EXHAUSTED`) |
| `CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS` | how long a streaming handler waits for credit before it ends with `TIMEOUT` |
| `CONFORMANCE_RETRY_MAX_ATTEMPTS`, `CONFORMANCE_RETRY_INITIAL_BACKOFF_MS` | the library's retry policy for every send (attempts in all, the first backoff) |
| `CONFORMANCE_HUB_TOKEN` | a static token, sent in `Register.credentials.token` |
| `CONFORMANCE_OAUTH2_TOKEN_URL`, `CONFORMANCE_OAUTH2_CLIENT_ID`, `CONFORMANCE_OAUTH2_CLIENT_SECRET` | the service's JWT: client credentials at this token endpoint (`POST`, form `grant_type=client_credentials`, the answer's `access_token` and `expires_in`) |
| `CONFORMANCE_TLS_CA`, `CONFORMANCE_TLS_SERVER_NAME` | TLS to the hubs: a PEM file of the CA to trust and the name to verify |
| `CONFORMANCE_TLS_CLIENT_CERT`, `CONFORMANCE_TLS_CLIENT_KEY` | mutual TLS: the client certificate (PEM, `snodra://service/<name>`) and its key; the runner replaces the files to renew them |
| `CONFORMANCE_JWKS_URL`, `CONFORMANCE_ISSUER` | callers' tokens (the `authorization` header) are validated against this JWKS (RS256) and issuer; only `L1-IDENTITY` (which calls `conformance.Guarded`) and `L1-PROPAGATION` (which relays a caller's token) set them |

The service registers every type of [the next section](#the-services-types) at start, with one exception:
`conformance.Guarded` is registered only when `CONFORMANCE_JWKS_URL` is set. A handler that requires an authenticated
caller needs a JWKS to verify the caller's token, and a library refuses to start one without it
([messages.md, Forwarded identity](https://snodra.damoreira.com/library-authors/messages/#forwarded-identity)). Without
a JWKS the service serves no `conformance.Guarded`, so a request of it gets `NO_HANDLER`.

The service has no event handler, no event store and no message store, writes its logs to standard output or standard
error (the runner keeps them per instance), and stops on SIGTERM within 5 s. It must register within 30 s of its start
(`--startup-timeout` raises it).

## The service's types

| Type | Kind, mode | Behaviour |
|---|---|---|
| `conformance.Echo` | command, unary | answers `{payload, headers, instanceId, requestId}`: the payload unchanged, every request header the handler saw, its instance id, the request id when the library exposes it |
| `conformance.EchoQuery` | query, unary | the same, as a query |
| `conformance.Done` | command, unary | a command without a result: answers `{}` |
| `conformance.Sleep` | command, unary | sleeps `ms`, then answers `{instanceId, sleptMs}` |
| `conformance.SleepQuery` | query, unary | the same, as a query: the target the runner cancels as a scatter-gather target (snodra-server cancels only queries) |
| `conformance.Fail` | command, unary | fails as a handler raising an application error: `HANDLER_ERROR`, the message containing `message`, the details the JSON text of `details` |
| `conformance.Strict` | command, unary | its payload is `{count: integer}`; a payload that does not decode to it is the library's `INVALID_REQUEST` |
| `conformance.Guarded` | command, unary | only with `CONFORMANCE_JWKS_URL`; requires an authenticated caller; refuses the subject `conformance-refused` (`PERMISSION_DENIED`); payload as `conformance.Strict`; answers `{count, user}` |
| `conformance.Relay` | command, unary | inside its handler sends `payload` as a command to `conformance.runner.Probe` (`target` `probe`) or `conformance.runner.Missing` (`missing`), with default options, and fails as an application does when that call fails: it rethrows the library's error, never maps it itself |
| `conformance.SharedQuery` | query, unary, **shared** | registered in `Register.shared_query_types`; answers `{service, instanceId}` |
| `conformance.Stream` | query, streaming | `count` items `{n}`, n = 1, 2, ..., each after `delayMs` |
| `conformance.Watch` | query, subscription | by `topic`; the initial result `{topic, value: 0, instanceId}`; updates `{topic, value}` from `conformance.Emit`; the service's only subscription type, so its `Register.subscription_query_types` is `["conformance.Watch"]` |
| `conformance.Emit` | command, unary | emits each of `values` to every `conformance.Watch` of `topic` this instance serves, then completes them (`complete`) or fails them (`fail`, a message); answers `{}` |
| `conformance.Send` | command, unary | sends a request through the library ([below](#conformancesend)) |
| `conformance.Outcome` | query, unary | the outcome of an asynchronous `conformance.Send`, waiting up to `waitMs`; `{pending: true}` when it is not over, `{unknownOp}` for an id it does not know |
| `conformance.View` | query, unary | `{service, instanceId, hubIds, lastRejection, subscriptionsServed, retry}`: the hub ids the instance counts as registered (acknowledged), the latest `Register` refusal the library exposes, the subscriptions it serves, whether a retry policy applies to its sends (required: an absent `retry` fails `L1-RETRY` as a contract violation) |

`conformance.View.lastRejection` may come from any means the library offers to expose a refusal: an API, or its log.
The official libraries expose it through an API: Kotlin `com.damoreira.snodra.messaging.hub.HubConnections.latestRejection`
(read by the Kotlin conformance service), Rust `snodra_messaging::SnodraMessaging::latest_rejection()` (read by the
reference service of this repository, `reference-service/`) and Go `snodra.App.Rejection()` (read by the Go conformance
service). A library that exposes refusals nowhere fails `L1-REJECTION`.

Everything else is the library's own behaviour: the conformance service writes these handlers the way an application
would, with the library's API, and never works around the library (no hand-written frames, no error mapping of its
own beyond reporting outcomes).

## The runner's types

| Type | Kind, mode | Served by |
|---|---|---|
| `conformance.runner.Probe` | command, unary | the runner |
| `conformance.runner.ProbeQuery` | query, unary | the runner |
| `conformance.runner.ProbeStream` | query, streaming | the runner |
| `conformance.runner.ProbeWatch` | query, subscription | the runner |
| `conformance.runner.Missing` | command, unary | nobody (`NO_HANDLER`) |

### Scripted answers

On a fake hub, the runner answers a unary request of these types as its payload says; the service passes the payload
through unchanged, so the runner scripts its own answers through `conformance.Send` and `conformance.Relay`:

| Key | Answer |
|---|---|
| `reply` | this payload |
| `hubError`, `message` | `ServerMessage.error` with this `ErrorCode` number (any number, unknown ones included) |
| `replyError` (`code`, `message`, `details`) | a `Reply` carrying this handler error |
| `silent` | nothing, ever |
| `close` | the stream the request came on is closed (`UNAVAILABLE`) |
| `delayMs` | the answer after this delay (dropped if the library cancelled the request meanwhile) |
| `case`, `steps` | one script per attempt of the same `case`, the last one repeated (retries) |
| `byHub` | a script per fake hub id (several hubs) |
| none | `{hub, id}`: the fake hub's id and the request id |

Streaming and subscription requests of the runner's types are answered by the check itself, part by part. Any other
type is answered `NO_HANDLER` by the fake hub, as by a hub where nobody registered it.

## `conformance.Send`

The payload ([schema](../contract/conformance.Send.json)):

| Field | Meaning |
|---|---|
| `op` | `unary`, `scatter` (a scatter-gather), `stream`, `subscribe` |
| `target` | `probe`, `probe-query`, `missing`, `probe-stream`, `probe-watch` (the runner's types); `echo`, `echo-query`, `shared-query`, `stream`, `watch` (the service's own, [below](#sends-to-the-services-own-types)) |
| `payload` | the request's payload |
| `routingKey`, `timeoutMs`, `headers` | the caller's routing key, its timeout (absent: the caller chose none) and headers added through the library's generic header option |
| `retry` | optional: `{maxAttempts, initialBackoffMs}` for this call only, for a library that takes a policy per call (the runner sets the policy for every send with `CONFORMANCE_RETRY_*`) |
| `initialCredit`, `maxResults` | for `stream` and `subscribe` (and `maxResults` for `scatter`) |
| `read`, `readDelayMs` | stop after `read` items (the subscription's initial result counts) and drop the stream or subscription, which cancels it; wait between reads |
| `async` | answer `{opId}` at once and run the call; `conformance.Outcome` reports it |

The outcome ([schema](../contract/conformance.Send.json), `$defs/outcome`): `result` `ok`, `error` or `unsupported`
(with `reason`: the library cannot do what was asked; the check that asked fails), `elapsedMs`, and per `op`: the
decoded `payload` (unary), the `items` and how they `end`ed (`completed`, `stopped`), the `parts` with `failed` and
`timedOut` (scatter). A failure carries `error`:

| `error.kind` | When |
|---|---|
| `never-sent` | the request was never handed to a stream's transport: no acknowledged stream, or still in the library's own buffer when the stream broke |
| `outcome-unknown` | handed to the transport, then the hub stream broke before the answer; also a stream or a subscription the library sent whose hub stream was lost |
| `code` | an `ErrorCode` reached the caller, from the hub or a handler; the library's local guard timeout is `TIMEOUT` here too ([the protocol page](https://snodra.damoreira.com/library-authors/protocol/) lists the sender's local guard among the producers of `ErrorCode.TIMEOUT`). `error.code` its name, `error.message`, `error.details` (UTF-8 text) as the caller sees them |
| `lost-parts` | the library detected lost parts: a gap in `ReplyPart.seq`, or a `ReplyEnd.parts` above the parts received |
| `refused` | the library refused the call before sending it (a header it owns, ...) |
| `local` | any other local failure |

The service classifies by the library's typed errors where it has them; where it has none, it says so (the Rust
library reports lost parts and a refused header as one error variant, `MessagingError::InvalidMessage`, told apart by its text).

### Sends to the service's own types

By default a library answers a command, and a point query of a type that is not shared, in-process when the instance
handles the type itself: nothing goes through the hub, and the routing key is ignored
([protocol.md, Requests a service answers itself](https://snodra.damoreira.com/library-authors/protocol/#requests-a-service-answers-itself)).
So a unary `conformance.Send` to `echo` or `echo-query` never reaches the hub unless the service turns on its library's
"always route through the hub" setting. A scatter-gather, a stream, a subscription and a query of a shared type always
go through the hub.

No level-1 check sends to the service's own types: the checks send only to the runner's types, which the service never
handles, so the setting changes no verdict. The interoperability harness (`interop/`) does send to them, to make one
language call another through the hub. A conformance service should therefore turn the setting on, or let it be
turned on through its environment. The reference service and the Go conformance service turn it on themselves; the
Kotlin conformance service keeps the library's default, and the harness turns it on through the service's
environment (`SNODRA_MESSAGING_HUB_ALWAYSROUTETHROUGHHUB=true`). A service that cannot route these sends through the
hub answers them itself, and the harness reports them as not exercised (its `LOCAL_SENDERS`).

## How each check is proved

"Fake" is a fake hub, "hub" the real one. The runner's checks are the ids of
[feature levels](https://snodra.damoreira.com/library-authors/feature-levels/#level-1-core); each is independent and bounded in time. Every
time bound comes from the rule the check proves, with a tolerance for scheduling; none from the reference's speed.

| Id | Where | What the runner does and expects |
|---|---|---|
| `L1-REGISTER-FIRST` | fake | the first two frames of a stream, and of the stream after the fake drops it: a `Register` (service name, instance id, `Register.initial_permits` of at least 1), then a `Heartbeat` |
| `L1-HANDSHAKE-ACK` | fake | no `RegisterAck`, heartbeats echoed (the check's deliberate deviation from snodra-server; one heartbeat is enough, no interval applies before the ack): `conformance.View` (served before the ack) reports no hub and a `conformance.Send` does not go out on that stream; the stream is closed (within 30 s: the rule names no bound) and reopened; once acknowledged, `conformance.View` names the hub |
| `L1-EARLY-REQUESTS` | fake | capacity 1, the ack held back: two `conformance.Echo` before it are both served (the fake forwards the second only once the first one's permit came back); three permits for three requests |
| `L1-REJECTION` | fake | four `Register`s refused with `ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED` and `PERMISSION_DENIED`: the process keeps running, each next attempt comes 0.9 to 6 s later (at least 1 s, at most the 5 s cap), and after the fifth is accepted `conformance.View` exposes the last refusal's message |
| `L1-HEARTBEAT` | fake, hub | heartbeats at the ack's 1700 ms interval: mean gap within 20 %, no gap above 1.5 intervals; on the hub, an idle instance stays registered (not stale, same session) past the hub's heartbeat timeout |
| `L1-SILENT-HUB` | fake | the fake stops echoing (interval 1 s): the library itself drops the stream on the tick after the third unanswered heartbeat, so the stream carries three heartbeats after the last echo (the `RegisterAck` when nothing was echoed), or four when one followed a later `Register`; the drop comes no earlier than three intervals after that echo and before four intervals plus 2 s (a bound against hanging, not a timing rule); then it reconnects |
| `L1-RECONNECT` | fake | a restart answering `UNAVAILABLE` for 8 s (at least two attempts, waits at most 5.6 s, fewer than 20 attempts: no tight loop), then a process gone (TCP refused) for 8 s: each time registered again within 12 s with the identical `Register` |
| `L1-UNARY` | hub | two instances: `conformance.Echo`, `conformance.EchoQuery` (payload and header back), `conformance.Done` answers `{}`, one routing key reaches one instance; `conformance.Send` of a command (with a routing key, seen by the runner's instance) and of a query come back decoded, `{}` too |
| `L1-REQUEST-ID` | fake | 15 sends, 10 pending at once: 15 distinct, non-empty ids |
| `L1-TIMEOUT` | fake | `RegisterAck.max_timeout_ms` 2000, a type override of 1500, the runner silent: `Request.timeout_ms` is 0 without a caller timeout and the caller's 1200 with one; the library gives up after the bound of [the protocol page](https://snodra.damoreira.com/library-authors/protocol/) (2000, 1500, 1200) plus 1 s, not 150 ms earlier, not 2.5 s later |
| `L1-ERRORS` | fake | `ServerMessage.error` with every `ErrorCode`, 0 and 42: each its own code, 0 and 42 as `HANDLER_ERROR`; a handler's `HANDLER_ERROR`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `INVALID_REQUEST` keep their message, `HANDLER_ERROR` its details |
| `L1-NEVER-SENT` | fake | before any ack: `never-sent`; a request the fake takes and answers by closing the stream: `outcome-unknown` |
| `L1-RETRY` | fake | with `CONFORMANCE_RETRY_*` (3 attempts): after `NO_HANDLER` a second attempt with a fresh id and the remaining budget (`Request.timeout_ms` > 0, below the caller's 5000; also > 0 without a caller timeout); one attempt only after `TIMEOUT`, `HANDLER_DISCONNECTED` and a stream broken after the write. Skipped when `conformance.View.retry` is false |
| `L1-ARRIVAL-STREAM` | fake | two hubs: a reply and a stream's parts and end go on the stream the request came from; the answer to a request whose stream was lost appears on no other stream |
| `L1-ONE-TERMINAL` | fake | one `Reply` for `<id>` and `<id>#<n>`, one `ReplyEnd` for a stream (`<id>#1`) and a completed subscription target, nothing for a stream or a scatter-gather target (`conformance.SleepQuery`) after the hub's `Cancel` (a later `Flow` included) |
| `L1-HANDLER-ERRORS` | fake | `conformance.Fail`: `HANDLER_ERROR`, short message without a line break; `conformance.Relay` whose call gets `NO_HANDLER`: `HANDLER_ERROR`; a payload that does not decode, a payload that is not JSON, a unary request to a streaming handler and to a subscription handler (not `#0`): `INVALID_REQUEST`; a type not served: `NO_HANDLER`; every other cell of the table of [the protocol page](https://snodra.damoreira.com/library-authors/protocol/#handling-a-request) the service's types reach (a query to a command, a command to each kind of query handler, a stream and a subscription to each handler and to a type not served, a unary request of kind `REQUEST_KIND_UNSPECIFIED`): its code, in one `Reply` for a unary request and in one `ReplyEnd` without parts otherwise; the stream stays up |
| `L1-IDENTITY` | fake | `conformance.Guarded` with the runner's JWKS: no token, a non-JWT, an expired token, a token signed by a key the JWKS does not publish, one of another issuer: `UNAUTHENTICATED`; `conformance-refused`: `PERMISSION_DENIED`; no token and a bad payload: `UNAUTHENTICATED` (decided before decoding); a valid token and a bad payload: `INVALID_REQUEST`; a valid token: answered, also with the scheme written `bearer`. The token is checked before the handler lookup: `conformance.Echo` (no caller needed) with a non-JWT, an expired token or an `authorization` header that is not `Bearer <token>`, and a type not served with an expired token: `UNAUTHENTICATED`; `conformance.Echo` with a valid token: answered. A second instance without a JWKS: `conformance.Echo` with a non-JWT, an unsigned token (`alg` `none`) or a header that is not `Bearer <token>`: `UNAUTHENTICATED`; with a signed token: answered (the token is only carried on) |
| `L1-PROPAGATION` | fake | `conformance.Relay` with `correlationId`, `authorization` and `traceparent`: the nested request carries the same correlation id and token and a traceparent of the same trace; without them it carries the handled `Request.id` as correlation id, no authorization, and a traceparent only if well formed; a `conformance.Send` setting `authorization`, `traceparent`, `tracestate`, `correlationId`, `correlation-id` (also in other letter cases) is `refused` and nothing is sent (the rule of the [messages page](https://snodra.damoreira.com/library-authors/messages/#request-headers)) |
| `L1-PERMITS` | fake | capacity 4 offers 4; capacity 1 (the fake forwards only with a permit): the permit comes back at once after each reply (median below 35 ms, against 50 ms batching); capacity 64, after 5 warm-up requests: each `Permits` for the 10 next completions carries 16 grants or leaves at least 40 ms after the earliest completion it covers (batched at 16 or 50 ms, 10 ms tolerance), and all 10 come back; on two hubs, permits go back to the arrival stream |
| `L1-SHARED` | hub | the service and the runner own `conformance.SharedQuery` shared (topology: shared, both owners); point queries reach the service; a gather reaches both |
| `L1-SCATTER` | hub, fake | the library's gather over two runner targets (one failing) returns both parts, `failed` 1; a gather on `conformance.EchoQuery` is answered by both instances; on a fake, a `conformance.SleepQuery` target `<id>#<n>` cancelled by the hub sends nothing more |
| `L1-STREAM` | fake | handler: parts 1..5 under credits 2 + 2 + 1, nothing beyond the credit, nothing after `Cancel`, `TIMEOUT` when no credit comes (`CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS` 2000); sender: `Request.initial_credit` as asked, credit refilled to read 12 parts, `Cancel` after `read` 2, a gap and a short end reported as `lost-parts` |
| `L1-SUBSCRIBE` | fake | the `Register` declares `Register.subscription_query_types` `["conformance.Watch"]`, a type also in a query list; handler: `<id>#1` then `<id>#0`, an emit right after the initial result reaches the subscription, completion ends it, `Cancel` removes it, a third subscription beyond `CONFORMANCE_MAX_SUBSCRIPTIONS` 2 ends with `RESOURCE_EXHAUSTED`; sender: `Request.initial_credit` as asked, the initial result and 5 updates under a credit of 2 (refilled), a gap and a short end reported as `lost-parts` |
| `L1-LOST-STREAM` | fake | capacity 1: after the stream of a waiting streaming handler and a subscription is lost, every `InstanceStatus` on the new stream (after a 1 s settle) reports no handler in flight, a request on the new stream runs, emits reach nothing, `conformance.View` serves no subscription; a stream and a subscription the library sent through a lost stream fail as `outcome-unknown` |
| `L1-TOKEN` | fake | `Register.credentials.token` on every stream to both hubs, also after a reconnect |
| `L1-JWT` | fake | the runner's token endpoint issues 10 s tokens: the `Register` carries one; for 25 s each token in force is replaced by `ClientMessage.credentials` before it expires, each new token sent once; after an `UNAUTHENTICATED` close the next `Register` carries a token fetched after it |
| `L1-MTLS` | fake | the fake requires client certificates of the runner's CA (one of another CA gets no stream: this proves the fixture verifies, not the library); a certificate valid for 60 s, renewed in the files 2 s after the first stream, must be presented on a new stream before its notAfter, so the library picks the replaced files up within about 55 s, without a restart; after the fake's `UNAUTHENTICATED` "certificate expired" close, a new stream within 6 s and `conformance.View` does not show it as a refusal |
| `L1-TLS` | fake | the fake serves TLS with a CA the runner made: the service registers with `CONFORMANCE_TLS_CA` and `CONFORMANCE_TLS_SERVER_NAME`; a hub certificate of another CA, and one for another name: no stream in 6 s |
| `L1-HUBS` | fake, two hubs | three targets, two answering one hub id: one stable stream for that id, `Register`s identical apart from `initial_permits` (each stream's share, `L1-HUB-PERMITS`), `conformance.View` knows two hubs; with two real hubs, the topology of each shows the instance reporting both hub ids |
| `L1-HUB-ROUTING` | fake | hubs `hub-1` and `hub-2` (the test vectors' ids): keyed sends land on the rendezvous winner (`order-42` on `hub-2`), among them keys a signed comparison of the scores would send elsewhere; unkeyed ones alternate (round-robin over the two acknowledged hubs) |
| `L1-HUB-PERMITS` | fake | capacity 5 offers 3 to each; one hub gone: the other gets `Permits{count 2, grant 5}`; the hub back gets 3 |
| `L1-HUB-FAILOVER` | fake | `NO_HANDLER` from the first hub (rendezvous order for a key, round-robin otherwise): the same id on the other hub; both declining: `NO_HANDLER` within 1.5 s |
| `L1-STATUS` | fake | two hubs: an `InstanceStatus` at least every 5 s (no two more than 6.5 s apart, and none missing for 6.5 s at the end of the observation; extra reports are allowed) on each stream, naming both hub ids and the two handlers running |

What no check can observe from outside is listed in
[the conformance page](https://snodra.damoreira.com/library-authors/conformance/#what-the-runner-checks).

## Levels 2 to 4

Levels 2 to 4 are proved by the event-store and message-store conformance kits that ship with both official libraries
([feature levels](https://snodra.damoreira.com/library-authors/feature-levels/)): the runner checks level 1 only. The namespace
`conformance.events.*` is reserved for a later runner version that checks events, processors and sagas from outside.
