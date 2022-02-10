mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn git_commit_hash() -> String {
    let agent_build = built_info::GIT_COMMIT_HASH
        .unwrap_or("<unknown>")
        .to_string();
    let agent_build = agent_build.chars().take(7).collect();
    agent_build
}

pub fn git_commit_hash_full() -> &'static str {
    built_info::GIT_COMMIT_HASH.unwrap_or("<unknown>")
}

pub fn time() -> &'static str {
    built_info::BUILT_TIME_UTC
}

pub fn compiler() -> &'static str {
    built_info::RUSTC_VERSION
}

pub fn print_build_info() {
    let commit_sha = git_commit_hash_full();
    let time = time();
    let compiler = compiler();

    println!("commit  : {}", commit_sha);
    println!("time    : {}", time);
    println!("compiler: {}", compiler);
}
