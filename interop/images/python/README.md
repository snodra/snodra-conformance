# The Python slot

No Python image exists yet. To join the matrix, the official Python library's conformance
service adds `interop/images/python/Dockerfile` and a `python` service in `interop/compose.yaml`,
and `python` goes into `LANGUAGES`.

An image must provide:

- an entrypoint that starts the library's **conformance service**
  (see [the conformance contract](../../../docs/contract.md)), with no arguments;
- the contract's environment, as the compose file sets it: `CONFORMANCE_SERVICE_NAME`
  (`conformance-interop`), `CONFORMANCE_INSTANCE_ID` (`python`), `CONFORMANCE_HUBS` (`hub:7000`) and
  `CONFORMANCE_CAPACITY` (`16`);
- a stop on SIGTERM within 5 s;
- every `conformance.*` type of the contract, as the other languages' services serve them: the
  matrix uses `Echo`, `EchoQuery`, `Stream`, `Watch`, `Emit`, `Relay`, `Send` and `Outcome`.
