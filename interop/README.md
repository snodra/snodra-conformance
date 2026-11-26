# Cross-language interoperability

A protocol is what makes a Kotlin service and a Rust service interchangeable. The conformance runner
proves each library alone against the protocol; this harness proves the libraries **against each other**:
one conformance service per official library (Kotlin, Rust, Go, Python), all registered on one real Snodra
Server as the service `conformance-interop` (instance id = the language), and a driver that makes every
service send to every other through the hub.

```sh
interop/run.sh                                  # LANGUAGES=go,rust,kotlin,python
SNODRA_HUB_PORT=17000 interop/run.sh            # macOS: the AirPlay Receiver holds port 7000
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
| `images/go` | `examples/conformance-service` of [snodra-messaging-go](https://github.com/snodra/snodra-messaging-go) at the tag `GO_REF` (default `v0.1.1`) |
| `images/python` | `examples/conformance-service` of [snodra-messaging-python](https://github.com/snodra/snodra-messaging-python) at the tag `PYTHON_REF` (default `v0.1.0`) |

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

## Known limits

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
