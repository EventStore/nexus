use crate::vector::sources::disks::disk_queue_length;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "show-disk-queue-length", about = "Shows the disk queue length")]
struct Opt {
    disk_regexes: Vec<String>,
}

pub async fn run(args: Vec<String>) {
    let args = Opt::from_iter(args.iter());
    let mut disk_regexes = Vec::new();
    for regex in args.disk_regexes.iter() {
        disk_regexes.push(regex::Regex::new(regex.as_str()).expect("error constructing regex"));
    }
    let results = disk_queue_length::get_disk_queue_length("/proc/diskstats", &disk_regexes).await;
    if results.is_empty() {
        println!("Couldn't find any disks with the given regexes");
    } else {
        for r in results {
            println!("{} {}", r.disk, r.value);
        }
    }
}
