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
`snodra/snodra-server:${SNODRA_SERVER_VERSION:-0.5.4}`) and a Rust toolchain for the driver. It exits 0 only
when every exchange passed, prints one line per exchange, ends with
`interop: <N> languages, <M> exchanges passed, trace <trace id>` and stops everything it started.

## What runs

| Image | Built from |
|---|---|
| `images/rust` | `reference-service` of this repository, on the `snodra-messaging-rs` release pinned in `Cargo.lock` |
| `images/kotlin` | `examples/conformance-service` of [snodra-messaging](https://github.com/snodra/snodra-messaging) at the tag `KOTLIN_REF` (default `v0.5.1`) |
| `images/go` | `examples/conformance-service` of [snodra-messaging-go](https://github.com/snodra/snodra-messaging-go) at `GO_REF` |

`GO_REF` defaults to a pinned commit: the Go library has no release tag yet. **Change that default
in `compose.yaml` (the line marked "To change at release") to the release tag.** `GO_SRC` and
`KOTLIN_SRC` name a local checkout instead of a ref (`GO_SRC=~/src/snodra-messaging-go interop/run.sh`).

## What is asserted

1. The hub's topology shows one instance of `conformance-interop` per language.
2. For each instance, a routing key whose keyed `conformance.Echo` answers that instance
   (`key-0` .. `key-499`; the hub hashes `service:key`, so the key reaches the same instance for any
   command type).
3. Every ordered pair (X, Y), X != Y: X, reached by its key, sends a keyed `conformance.Echo` to Y through
   its library (`conformance.Send`). The answer names Y, carries the correlation id the driver set on the
   `Send` and a `traceparent` of the run's trace (one trace id for the whole run, a fresh parent id per call).
4. From every X: a scatter-gather on `conformance.EchoQuery` answers a part from every language, each in the run's
   trace; a unary `conformance.EchoQuery` succeeds.
5. From every X: a streaming `conformance.Stream` of 5 items arrives complete and in order. The hub picks the
   serving instance and it is not observable, so which language served is not asserted.
6. From every X: a `conformance.Watch` subscription (initial result plus one update per language). The hub
   subscribes **every** instance, so a keyed `conformance.Emit` on each language reaches it: the initial result
   names one instance, and the updates are exactly one from each language.
7. From every X: `conformance.Relay` (keyed to X) sends to the driver's own `conformance.runner.Probe` from
   inside the handler; the probe saw the same correlation id, the same `authorization` and a `traceparent`
   of the run's trace. The token is JWT-shaped (all three libraries answer `UNAUTHENTICATED` for a
   `Bearer` value that is not a JWT, even for a handler that needs no caller).

## Known limits

- **Local handlers.** The Kotlin library answers a unary send of a type its own process handles without
  the hub (a short-circuit that ignores the routing key). Kotlin sending a keyed `Echo` to Go or Rust
  therefore never leaves the process. `run.sh` passes `--local-senders ${LOCAL_SENDERS:-kotlin}`:
  those pairs are reported as `note`, not counted as exchanges. Kotlin as a sender through the hub
  is still covered by steps 4 (scatter), 5, 6 and 7. A language whose pair is *not* local is held to
  the full assertions, and a listed language that does reach the hub is asserted in full.
- **Streaming.** The serving instance of a stream is chosen by the hub (routing keys are ignored for
  queries); the harness checks the items, not who produced them.
- **Outcome.** `conformance.Outcome` is a query, so the hub may route it to an instance that does not own the
  operation (`unknownOp`): the driver asks again.

## A fifth language

Add `images/<lang>/Dockerfile` (the Python slot, `images/python/README.md`, says what an image must provide),
a service with that name in `compose.yaml` (`CONFORMANCE_INSTANCE_ID` = the name) and put the name in
`LANGUAGES`, for example `LANGUAGES=go,rust,kotlin,python interop/run.sh`. The driver derives every pair from the
list.
