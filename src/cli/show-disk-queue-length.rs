use nexus::vector::sources::disks::disk_queue_length;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "show-disk-queue-length", about = "Shows the disk queue length")]
struct Opt {
    disk_regexes: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = Opt::from_args();
    let mut disk_regexes = Vec::new();
    for regex in args.disk_regexes.iter() {
        disk_regexes.push(regex::Regex::new(regex.as_str()).expect("error constructing regex"));
    }
    let results = disk_queue_length::get_disk_queue_length("/proc/diskstats", &disk_regexes).await;
    if results.len() < 1 {
        println!("Couldn't find any disks with the given regexes");
    } else {
        for r in results {
            println!("{} {}", r.disk, r.value);
        }
    }
}
