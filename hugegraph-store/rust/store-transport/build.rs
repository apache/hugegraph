fn main() {
    let dir = "../../hg-store-grpc/src/main/proto";
    let files = [
        "healthy.proto",
        "query.proto",
        "graphpb.proto",
        "store_common.proto",
        "store_session.proto",
        "store_state.proto",
        "store_stream_meta.proto",
    ];
    prost_build::Config::new()
        .compile_protos(
            &files
                .iter()
                .map(|f| format!("{dir}/{f}"))
                .collect::<Vec<_>>(),
            &[dir, "/usr/include"],
        )
        .unwrap();
}
