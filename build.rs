fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `PROTOC` wins (the CI image and the developer's own protoc); otherwise use the binary
    // bundled with the build dependency, so a cross-compiled release needs no system package.
    if std::env::var_os("PROTOC").is_none()
        && let Ok(path) = protoc_bin_vendored::protoc_bin_path()
    {
        // SAFETY: a build script is single-threaded until it spawns, and this runs first.
        unsafe { std::env::set_var("PROTOC", path) };
    }
    // The vendored copy of snodra-server's proto/hub.proto (kept byte-identical by
    // scripts/check-proto-sync.sh).
    println!("cargo:rerun-if-changed=proto/hub.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // The rpc is named Connect: the transport helper `HubClient::connect` would collide.
        .build_transport(false)
        .compile_protos(&["proto/hub.proto"], &["proto"])?;
    Ok(())
}
