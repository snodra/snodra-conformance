# proto/

`hub.proto` is a verbatim copy of `proto/hub.proto` from the snodra-server repository, the source of truth. The
runner compiles it (`build.rs`) to speak the protocol in raw frames. Do not edit it here: change it in the server
repository first, then copy it byte for byte. `scripts/check-proto-sync.sh` (and the CI job `proto-sync`) fails when
it drifted. The server repository is public, so the check and its CI job clone it anonymously.
