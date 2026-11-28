# Cross-language interoperability

A protocol is what makes a Kotlin service and a Rust service interchangeable. The conformance runner
proves each library alone against the protocol; this harness proves the libraries **against each other**:
one conformance service per official library (Kotlin, Rust, Go, Python), all registered on one real Snodra
Server as the service `conformance-interop` (instance id = the language), and a driver that makes every
service send to every other through the hub.

```sh
interop/run.sh                                  # LANGUAGES=go,rust,kotlin,python
SNODRA_HUB_PORT=17000 interop/run.sh            # macOS: the AirPlay Receiver holds port 7000
LANGUAGES=go,python interop/run.sh --profile stores   # the brokers and stores, Go and Python
```

It needs Docker with compose (it builds four images and starts the hub image
`snodra/snodra-server:${SNODRA_SERVER_VERSION:-0.6.0}`) and a Rust toolchain for the driver. It prints one
line per exchange and stops everything it started.

Every exchange runs through the hub and must pass. With the four official services the last line reads:

```text
interop: 4 languages, 32 exchanges passed through the hub, trace <id>
```

**Read the summary line, not only the exit code**, when a service answers its own types in-process (see "Sends
to the sender's own types"): its required exchanges are then **not exercised**, the line names them, and the
exit code is still 0. `INTEROP_STRICT=1 interop/run.sh` (the driver's `--strict`) turns any not-exercised
exchange into a failure with a non-zero exit. Two runs on one host need different
`SNODRA_HUB_PORT` values (the compose project name is unique per run, `INTEROP_PROJECT` overrides it).

## What runs

| Image | Built from |
|---|---|
| `images/rust` | `reference-service` of this repository, on the `snodra-messaging` release pinned in `Cargo.lock` |
| `images/kotlin` | `examples/conformance-service` of [snodra-messaging](https://github.com/snodra/snodra-messaging) at the tag `KOTLIN_REF` (default `v0.6.1`) |
| `images/go` | `examples/conformance-service` of [snodra-messaging-go](https://github.com/snodra/snodra-messaging-go) at the tag `GO_REF` (default `v0.6.0`) |
| `images/python` | `examples/conformance-service` of [snodra-messaging-python](https://github.com/snodra/snodra-messaging-python) at the tag `PYTHON_REF` (default `v0.7.0`) |

The harness runs the released tags of the three published libraries (Go, Kotlin, Python) by default, and
the Rust reference service on the `snodra-messaging` release this repository pins.

`GO_SRC`, `KOTLIN_SRC` and `PYTHON_SRC` name a local checkout instead of a ref
(`GO_SRC=~/src/snodra-messaging-go interop/run.sh`).

## What is asserted

1. The hub's topology shows one instance of `conformance-interop` per language.
2. For each instance, a routing key whose keyed `conformance.Echo` answers that instance
   (`key-0` .. `key-499`). The protocol promises that the same key for the same type reaches the same
   instance while membership is stable; the driver also relies on that key reaching the same instance for
   the other command types (`Send`, `Emit`, `Relay`), which holds on the pinned hub but is not asserted.
3. Every ordered pair (X, Y), X != Y: X, reached by its key, sends a keyed `conformance.Echo` to Y through
   its library (`conformance.Send`). The payload round-trips unchanged and the answer names Y, carries the
   correlation id, the JWT-shaped `authorization` and a `traceparent` of the run's trace that the driver set
   on the `Send` (one trace id for the whole run, a fresh parent id per call): identity and trace cross from
   one language to another.
4. From every X: a scatter-gather on `conformance.EchoQuery` answers a part from every language, each in the run's
   trace; a unary `conformance.EchoQuery` round-trips its payload in the run's trace (the hub picks the serving
   instance; not exercised for a sender that answers it in-process).
5. From every X: a streaming `conformance.Stream` of 5 items arrives complete and in order. The hub picks the
   serving instance and it is not observable, so which language served is not asserted.
6. From every X: a `conformance.Watch` subscription (initial result plus one update per language). The hub
   subscribes **every** instance, so a keyed `conformance.Emit` on each language reaches it: the initial result
   names one instance, and the updates are exactly one from each language. The emitting language of an
   update is inferred from the routing (each emit is keyed to that language's instance and carries its
   name), not proven in the update itself: the update payload has no instance field. The driver emits only
   once every instance reports serving the subscription (`conformance.View`).
7. From every X: `conformance.Relay` (keyed to X) sends to the driver's own `conformance.runner.Probe` from
   inside the handler; the probe saw the same payload, correlation id, `authorization` and a `traceparent`
   of the run's trace. The token is a signed compact JWT that no service can verify: without a JWKS a
   service only carries it on, and every library answers `UNAUTHENTICATED` for a `Bearer` value that is
   not one, even for a handler that needs no caller.

## The stores profile

`interop/run.sh --profile stores` proves the shared **brokers and stores** instead of the hub's routing,
and then drives the hub's **processor control plane** against the two services. It currently supports
`LANGUAGES=go,python` only: the Kotlin and Rust store-interop services do not exist yet (see "Known
limits"). The saga scenarios need the local L4 checkouts: the released tags carry the level-3 probe
but not the saga commands. With local checkouts:

```sh
unset DOCKER_HOST                          # Docker Desktop; on Rancher Desktop set unix://$HOME/.rd/docker.sock
SNODRA_HUB_PORT=17000 INTEROP_STRICT=1 LANGUAGES=go,python \
  GO_SRC=~/src/snodra-messaging-go PYTHON_SRC=~/src/snodra-messaging-python \
  interop/run.sh --profile stores
```

The stores profile requires both `GO_SRC` and `PYTHON_SRC`; `run.sh` exits 2 when either is unset.
The released tags can be used by the exchange profile; the stores profile's saga scenarios need the
saga probe commands of the local checkouts.

The profile starts one Kafka KRaft broker, RabbitMQ with the stream plugin, PostgreSQL, MySQL and a
single-node MongoDB replica set. The broker and database images are pinned by digest, and the two probe
images pin their base images by multi-architecture index digest. Kafka's pin is the one exception:
`apache/kafka@sha256:76af3d01…` is a `linux/amd64` image manifest, not an index (the same pin the
add-ons' own tests use), so the stores profile needs an amd64 Docker engine or emulation on an arm64
host. RabbitMQ, PostgreSQL, MySQL and MongoDB are multi-architecture index digests.
The profile creates one topic per Kafka family, one two-partition RabbitMQ super stream and its
dead-letter super stream, and runs one `store-interop` process per language, each with one instance per
store family (`<language>-<family>`, registered as the service `store-interop`).

The hub's gRPC port is `SNODRA_HUB_PORT` (7000 by default) and its dashboard HTTP API is
`SNODRA_HUB_HTTP_PORT` (one above it by default), both published on `127.0.0.1`. The driver uses the
HTTP API for the control scenarios; `run.sh` sets `INTEROP_HUB_WRITE_TOKEN` for both the hub
(`SNODRA_SERVER_DASHBOARD_WRITE_TOKEN`) and the driver.

The driver asserts, and prints one `ok` line per scenario; every one is required:

1. **A shared Kafka consumer group.** The `kafka` family appends events to the outbox from both
   languages; the group `store-interop.interop-kafka` delivers every event exactly once, each
   language handles events the other published, both partitions are read, and the outbox drains.
2. **A shared RabbitMQ Streams consumer name.** The same over a two-partition super stream: both
   languages join `store-interop.interop-rabbitmq` with `single-active-consumer`, one instance active
   per partition, and every event is delivered once.
3. **The outbox across languages on PostgreSQL, MySQL and MongoDB.** For each shared store, both
   languages append events to the one outbox; the relays publish them; each language consumes events
   the other appended; the marker of every event is visible in the shared store to both; the outbox
   drains.
4. **A processed marker skips the duplicate in the other language.** One event id is published on
   partition 0 (handled by the language that owns it) and again on partition 1: the other language's
   consumer finds the marker and records a duplicate delivery, one handled delivery in all.
5. **The status view.** `GET /api/v1/processors` reports every family's processor with two instances,
   its subscription and source, its event store (`kafka` or `rabbitmq-streams`), its capabilities and
   its dead-letter mode (`collection`, or `event-store` on RabbitMQ).
6. **Pause and resume through the hub.** `POST …/pause` acknowledges both instances; the status turns
   `PAUSED`; events published while paused are not handled; `POST …/resume` acknowledges both and the
   paused events are delivered. The hub's report, not a probe's guess, is asserted.
7. **A pause Go persists, obeyed by Python.** Go writes `paused = true` (by `go`) into MySQL's shared
   control store; Python's state reads `pausedBy: go` on its own poll, its status becomes `PAUSED`,
   and an event published meanwhile is not handled. The hub's resume releases both.
8. **A pause Python persists, obeyed by Go.** The reverse on MongoDB: Python writes `paused = true`
   (by `python`), Go's state reads `pausedBy: python`, its status becomes `PAUSED` and it handles
   nothing until the hub's resume.
9. **Reset and replay on Go.** The processor is paused, then `POST …/reset {"to":"earliest"}` is
   called with the Go instance's report the freshest (the hub executes on it): the report names
   `go-postgres` as `executedBy`, both instances prepare and finish, the ranges, deleted markers and
   the reset handler are counted, the status reports `replaying` with each range's `replayUntil`, the
   committed positions moved to each range's `from`, and after `POST …/resume` the replay closes while
   the normal handler runs again and the `@DisallowReplay` handler does not.
10. **Reset and replay on Python.** The same on MySQL with `python-mysql` as the executor and the
    `replay=False` handler's count unchanged.
11. **A dead letter parked by Go, retried by Python.** Go's handler rejects an event on its PostgreSQL
    partition; both languages list the `HANDLER_FAILED` letter in the shared store; both instances
    are allowed the id, the hub's retry goes to the freshest instance, and the response names the
    Python instance, `Retried`; the letter is gone, the marker is visible to both, the retried
    instance recorded the delivery, and the parking instance still has the marker.
12. **A dead letter parked by Python, retried by Go.** The reverse on MongoDB: Python parks, Go
    retries, and the same assertions hold with the languages swapped.
13. **Retry on the parking language.** Two scenarios: MySQL (Go parks and Go retries) and MongoDB
    (Python parks and Python retries), so the hub's retry is proved on each language by itself as
    well as across.
14. **`retry-all` and discard.** Two failing letters on MongoDB are retried in one call: the hub
    reports `failed: 2`, both stay, and `DELETE` removes each; the store's real count is zero
    afterwards.
15. **The event-store dead-letter mode end to end.** The RabbitMQ processor runs in mode
    `event-store`; a record no handler takes is published to `<source>.dlt`, the probe's subscription
    receives it with its key and payload and the six `snodra-dlt-*` headers, and the hub's
    dead-letter call answers `400 INVALID` because there is no collection to list.
16. **A capability the hub refuses.** The RabbitMQ broker has no management API, so the event store
    declares no replay: `POST …/reset` answers `409 UNSUPPORTED` naming
    `rabbitmq-streams`, with no instance asked and no operation executed.
17. **Go starts a saga, Python continues it exactly once.** On PostgreSQL, the start event is
    published on Go's partition and the advanced event on Python's; the final state records
    `startedBy: go`, `advancedBy: python`, `advances: 1`, version 1 and exactly one live instance.
18. **Python starts a saga, Go continues it exactly once.** The reverse on MongoDB, with the state
    read back by the other language and the same version/live assertions.
19. **The dispatch lease is exclusive across languages.** The advanced handler records a slow
    command; its handler tells the driver which language's instance runs it and keeps the send in
    flight until the driver answers. The other language's probe `ClaimLease` on the shared store is
    refused while the entry is pending. After the driver answers, the entry is sent and the lease is
    free; the probe claims it, then releases it through a witness claim. The slow handler ran once.
20. **A deadline scheduled by one language fires in the other.** The start handler schedules
    `saga-timeout`; the shared scheduler claims it, and the state names the language that fired it.
    The scenario shifts the deadline's due time across a full scheduler period, bounded, so the
    other language's poll is earlier for one offset; it asserts `startedBy` and
    `deadlineScheduledBy` in the final state and that `deadlineFiredBy` is a known language.
21. **A command given up in one language produces its notice in the other.** Python records a
    command whose handler returns `INVALID_REQUEST`; the relay that sends it may be either language,
    and the scenario retries fresh sagas until the notice lands in the other. The saga's state
    (`notices: 1`) and its ordinary `SagaCommandFailed` handler record it once, and a raw probe
    subscription to the family source counts every published `snodra.SagaCommandFailed` for the
    saga: the total is zero.

Names carry a run token generated by `run.sh`: the sources are
`snodra.interop.<run>.<family>`, the shared databases `snodra_interop_<run>`, and every topic and
super stream has two partitions (one per language). `INTEROP_RUN` overrides the token. The last line
reads:

```text
stores: 2 languages, 26 scenarios passed across kafka,rabbitmq,postgres,mysql,mongo, run <token>
```

`INTEROP_STRICT=1` is accepted for the harness's contract; every stores scenario is required, so it
changes nothing there.

The two probe processes run one App per family on the library's public API, with the real outbox,
relay, event consumer, event store and message store add-ons. Both register the same saga type
(`interop-saga`) and state fields, a start/advanced handler, `saga-timeout`, the local
`SagaCommandFailed` handler and the ordinary notice handler; the driver's `orders.interop.Saga*`
commands publish the events, read the shared saga state/dispatch/lease and the local notice ledger.
A raw probe subscription to each family source records a published `snodra.SagaCommandFailed` (the
notice is local, so it stays empty) and, on RabbitMQ, a second subscription reads the dead-letter
source. `images/store-go` and `images/store-python` build them from `GO_REF` / `PYTHON_REF` (the
released tags `v0.6.0` and `v0.7.0`), or from `GO_SRC` / `PYTHON_SRC`, which override the tags. The
released tags carry the level-3 probe; the saga scenarios need the local checkouts. The Python probe
reports its status every 4 s (the Go one keeps the 5 s default), so the two report phases drift and
the driver can wait for either language to be the freshest instance the hub picks for a reset or a
dead-letter call. `run.sh` stops every container and anonymous volume it started; the topics,
streams and databases live only in those containers, so a finished run leaves nothing behind.

## Known limits

- **Kotlin and Rust in the stores profile.** Their store-interop services do not exist yet: the
  profile supports `LANGUAGES=go,python` only and refuses any other language set instead of skipping
  scenarios. The follow-up is one probe per language (the same commands, `interop-<family>` processor
  and subscription, an `images/store-<lang>` image), `KOTLIN_REF`/`RUST_REF` and their `_SRC`
  variants, and the four-language generalization: the driver's `PARTITIONS` is two today (one per
  language), so four languages need four partitions and the "each language reads the other's events"
  assertions become per pair. The scenarios keep their shape. The saga records on SQL and MongoDB
  use the same layouts those libraries implement; the cross-language saga run is Go/Python until
  their store-interop services exist.

- **The stores profile requires local checkouts.** The released tags carry the level-3 probe but not
  the `Saga*` commands; `run.sh --profile stores` exits 2 when `GO_SRC` or `PYTHON_SRC` is unset.
  Run the exchange profile with released tags if only the routing matrix is wanted.

- **The event-store dead-letter mode is proved on RabbitMQ.** The `rabbitmq` family publishes to its
  `.dlt` super stream and the probe subscribes there; the Kafka families stay in `collection` mode, so
  the Kafka `.dlt` topic is not created by this profile.

- **A pause in a non-shared store is per instance.** On the `kafka` and `rabbitmq` families the
  message store is in memory, so a pause one language writes is not visible to the other; the
  cross-language pause scenario uses MySQL, whose control document both languages share.

- **Sends to the sender's own types.** Every official library answers a command, and a point query of a
  type that is not shared, in-process when its own instance handles the type: the hub is not used and the
  routing key is ignored. Every service here handles `Echo` and `EchoQuery`, so such a send reaches another
  language only when the service turns on its library's "always route through the hub" setting. The Rust,
  Go and Python services turn it on themselves; the Kotlin service keeps the default, and `compose.yaml`
  turns it on through its environment (`SNODRA_MESSAGING_HUB_ALWAYSROUTETHROUGHHUB=true`). A service that
  cannot be told to route through the hub is named in `LOCAL_SENDERS` (comma-separated, empty by default): its
  keyed `Echo` to another language and its unary `EchoQuery` are then reported as not exercised, never as
  passed, and `INTEROP_STRICT=1` fails on them.
- **Streaming.** The serving instance of a stream is chosen by the hub (routing keys are ignored for
  queries); the harness checks the items, not who produced them.
- **Outcome.** `conformance.Outcome` is a query, so the hub may route it to an instance that does not own the
  operation (`unknownOp`): the driver asks again.

## Another language

Add `images/<lang>/Dockerfile`, a service with that name in `compose.yaml` (`CONFORMANCE_INSTANCE_ID` = the
name) and put the name in `LANGUAGES`, for example `LANGUAGES=go,rust,kotlin,python,java interop/run.sh`. The
driver derives every pair from the list. An image provides an entrypoint that starts the library's
**conformance service** with no arguments (see [the conformance contract](../../../docs/contract.md)) and
stops on SIGTERM within 5 s; `compose.yaml` sets the contract's environment, and the service serves every
`conformance.*` type the matrix uses (`Echo`, `EchoQuery`, `Stream`, `Watch`, `Emit`, `Relay`, `Send` and
`Outcome`).

For the stores profile, the counterpart is one `examples/store-interop` probe per language, an
`images/store-<lang>` image, and one instance `<language>-<family>` per store family on the service
`store-interop`; the probe serves `orders.interop.Who`, `Publish`, `PublishRaw` and `State` and runs the
`orders.OrderPlaced` event consumer in the processor `interop-<family>`. Kotlin and Rust have no such probe
yet; adding them is the follow-up named above. The driver's `PARTITIONS` constant is two (one partition per
language), so a four-language profile also needs four partitions in `kafka-init`, the RabbitMQ topology and
the driver, plus `KOTLIN_REF`/`RUST_REF` (and their `_SRC` variants) in `compose.yaml`.
