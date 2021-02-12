fn main() {
    println!("cargo:rerun-if-changed=proto/");
    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(&["."]);
    // It would be nice to just add these derives to all the types, but
    // prost automatically adds them already to enums, which causes the
    // extra derives to conflict with itself.
    prost_build.type_attribute(".prometheus.Label", "#[derive(Eq, Hash, Ord, PartialOrd)]");
    prost_build
        .compile_protos(
            &["proto/event.proto", "proto/prometheus-remote.proto"],
            &["proto/"],
        )
        .unwrap();
    built::write_built_file().expect("Failed to acquire build-time information");

    tonic_build::configure()
        .build_server(false)
        .compile(&["proto/acceptor.proto"], &["proto/"])
        .unwrap();
}
