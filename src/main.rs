#[macro_use]
extern crate tracing;

pub mod build_info;
pub mod cli;
pub mod vector;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T> = std::result::Result<T, Error>;

use std::collections::HashMap;

fn build_info(_: String, _: Vec<String>) {
    build_info::print_build_info();
}

fn cpu_count(_: String, _: Vec<String>) {
    crate::cli::cpu_count::run();
}

fn show_disk_queue_length(_: String, args: Vec<String>) {
    let rt = tokio::runtime::Runtime::new().expect("couldn't create tokio runtime!");
    rt.block_on(crate::cli::show_disk_queue_length::run(args));
}

fn show_plugins(_: String, _: Vec<String>) {
    crate::vector::app::show_plugins();
}

fn run_internal_nexus() {
    crate::vector::app::run();
}

struct CommandDetails {
    desc: &'static str,
    func: Box<dyn Fn(String, Vec<String>)>,
}

#[cfg(unix)]
fn main() {
    let command_pos = std::env::args().position(|arg| arg == "--command");

    type CommandMap<'a> = HashMap<&'a str, CommandDetails>;
    let mut commands: CommandMap = HashMap::new();
    commands.insert(
        "build-info",
        CommandDetails {
            desc: "shows info on when this was built",
            func: Box::new(build_info),
        },
    );
    commands.insert(
        "cpu-count",
        CommandDetails {
            desc: "shows the number of CPUs (useful to debug the core load issue)",
            func: Box::new(cpu_count),
        },
    );
    commands.insert(
        "show-disk-queue-length",
        CommandDetails {
            desc: "exercises the disk queue length logic",
            func: Box::new(show_disk_queue_length),
        },
    );
    commands.insert(
        "show-plugins",
        CommandDetails {
            desc: "shows which vector plugins are registered in our internal vector",
            func: Box::new(show_plugins),
        },
    );

    match command_pos {
        Some(pos) => {
            if 1 > std::env::args().len() {
                eprintln!("could not find process path as arg 0");
                std::process::exit(1);
            }
            if pos + 1 >= std::env::args().len() {
                eprintln!("Expected a subcommand after `--command`");
                std::process::exit(1);
            }
            let process_path = std::env::args().next().unwrap();
            let command = std::env::args().nth(pos + 1).unwrap();
            let rest = std::env::args().skip(pos + 1);
            let entry = commands.get(command.as_str());
            match entry {
                Some(cmd) => {
                    (cmd.func)(process_path, rest.collect());
                }
                None => {
                    eprintln!("unknown subcommand: {}", command);
                    println!("Commands:");
                    let mut names: Vec<&&str> = commands.keys().collect();
                    names.sort();
                    for name in &names {
                        let cmd = commands.get(*name).unwrap();
                        println!(" - {} - {}", name, cmd.desc);
                    }
                    println!("To run in typical Nexus mode, don't specify command.");
                    std::process::exit(1);
                }
            }
        }
        None => {
            run_internal_nexus();
        }
    }
}
