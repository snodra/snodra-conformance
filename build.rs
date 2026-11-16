fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The hub's own copy of hub.proto (kept byte-identical to kraken's by scripts/check-proto-sync.sh).
    println!("cargo:rerun-if-changed=../proto/hub.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // The rpc is named Connect: the transport helper `HubClient::connect` would collide.
        .build_transport(false)
        .compile_protos(&["../proto/hub.proto"], &["../proto"])?;
    Ok(())
}
