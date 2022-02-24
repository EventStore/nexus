use built::Options;
use std::io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=./src");

    output_build_info()?;

    Ok(())
}

fn output_build_info() -> io::Result<()> {
    let mut opts = Options::default();

    opts.set_git(true)
        .set_compiler(true)
        .set_env(true)
        .set_time(true);

    let src = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let dst = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("built.rs");

    built::write_built_file_with_opts(&opts, src.as_ref(), &dst)
}
