# Cross-language interoperability

A protocol is what makes a Kotlin service and a Rust service interchangeable. The conformance runner
proves each library alone against the protocol; this harness proves the libraries **against each other**:
one conformance service per official library (Kotlin, Rust, Go), all registered on one real Snodra
Server as the service `conformance-interop` (instance id = the language), and a driver that makes every
service send to every other through the hub.

```sh
interop/run.sh                                  # LANGUAGES=go,rust,kotlin
SNODRA_HUB_PORT=17000 interop/run.sh            # macOS: the AirPlay Receiver holds port 7000
```

It needs Docker with compose (it builds three images and starts the hub image
`snodra/snodra-server:${SNODRA_SERVER_VERSION:-0.6.0}`) and a Rust toolchain for the driver. It prints one
line per exchange and stops everything it started.

**Read the summary line, not only the exit code.** Every exchange that runs must pass, but some required
exchanges cannot go through the hub with the Kotlin conformance service (see "Known limits"). They are **not
exercised**, and the last line says so:

```text
interop: 3 languages, 17 exchanges passed through the hub, 3 not exercised (kotlin answers its own types
in-process, so these sends never reach the hub): ["kotlin -> go", "kotlin -> rust", "kotlin unary echo-query"], trace <id>
```

Exit 0 is the default for that known case. `INTEROP_STRICT=1 interop/run.sh` (the driver's `--strict`) turns any
not-exercised exchange into a failure with a non-zero exit. Two runs on one host need different
`SNODRA_HUB_PORT` values (the compose project name is unique per run, `INTEROP_PROJECT` overrides it).

## What runs

| Image | Built from |
|---|---|
| `images/rust` | `reference-service` of this repository, on the `snodra-messaging-rs` release pinned in `Cargo.lock` |
| `images/kotlin` | `examples/conformance-service` of [snodra-messaging](https://github.com/snodra/snodra-messaging) at the tag `KOTLIN_REF` (default `v0.6.0`) |
| `images/go` | `examples/conformance-service` of [snodra-messaging-go](https://github.com/snodra/snodra-messaging-go) at the tag `GO_REF` (default `v0.1.0`) |

`GO_SRC` and `KOTLIN_SRC` name a local checkout instead of a ref
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

## Known limits

- **Not exercised: sends the sender answers in-process.** Every official library answers a command, and a
  point query of a type that is not shared, in-process when its own instance handles the type: the hub is
  not used and the routing key is ignored. Every service here handles `Echo` and `EchoQuery`, so such a send
  reaches another language only when the service turns on its library's "always route through the hub"
  setting. The Rust and Go services do; the Kotlin service keeps the default. Kotlin sending a keyed `Echo`
  to Go or Rust, and a unary `EchoQuery`, therefore never leaves the process: these exchanges are required
  but **cannot be exercised through the hub**, and are reported as not exercised, never as passed. `run.sh`
  passes `--local-senders ${LOCAL_SENDERS:-kotlin}` to name the languages that do this; for any other
  language the full assertions apply, and a listed language whose send does reach the hub is asserted in
  full. Kotlin as a sender through the hub is covered by the scatter, stream, subscription and relay
  exchanges (4 to 7). Use `INTEROP_STRICT=1` to fail on them.
- **Streaming.** The serving instance of a stream is chosen by the hub (routing keys are ignored for
  queries); the harness checks the items, not who produced them.
- **Outcome.** `conformance.Outcome` is a query, so the hub may route it to an instance that does not own the
  operation (`unknownOp`): the driver asks again.

## A fifth language

Add `images/<lang>/Dockerfile` (the Python slot, `images/python/README.md`, says what an image must provide),
a service with that name in `compose.yaml` (`CONFORMANCE_INSTANCE_ID` = the name) and put the name in
`LANGUAGES`, for example `LANGUAGES=go,rust,kotlin,python interop/run.sh`. The driver derives every pair from the
list.
