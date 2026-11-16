# proto/

`hub.proto` is a verbatim copy of `protocol/hub.proto` from kraken's umbrella repository, the source of truth. The
runner compiles it (`build.rs`) to speak the protocol in raw frames. Do not edit it here: change it in the umbrella
repository first, then copy it byte for byte. `scripts/check-proto-sync.sh` (and the CI job `proto-sync`) fails when
it drifted.
