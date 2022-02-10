pub fn run() {
    println!("cpus = {}", num_cpus::get());
    println!("physical = {}", num_cpus::get_physical());
}
