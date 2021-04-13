use vector::app::Application;
use vector::config::TransformDescription;


pub fn run() {
    println!("Available transformers:");
    for transformer in inventory::iter::<TransformDescription> {
        println!("- {}", transformer.type_str);
    }

    let app = Application::prepare().unwrap_or_else(|code| {
        std::process::exit(code);
    });

    app.run();
}