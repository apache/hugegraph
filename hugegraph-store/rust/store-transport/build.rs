fn main() {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../hg-store-grpc/src/main/proto");
    let files: Vec<_> = [
        "healthy.proto",
        "query.proto",
        "graphpb.proto",
        "store_common.proto",
        "store_session.proto",
        "store_state.proto",
        "store_stream_meta.proto",
    ]
    .iter()
    .map(|f| dir.join(f))
    .collect();
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&files, &[dir, std::path::PathBuf::from("/usr/include")])
        .unwrap();
}
