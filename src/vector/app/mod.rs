use crate::vector::sources::eventstoredb::metrics::EventStoreDbConfig;
use vector::app::Application;
use vector::config::SinkDescription;
use vector::config::SourceDescription;
use vector::config::TransformDescription;

inventory::submit! {
    SourceDescription::new::<EventStoreDbConfig>("eventstoredb_metrics")
}

// Vector's default tracing configuration won't pick up our log messages, so
// we change it here so the filter we'll include our modules as "targets".
// See Vector's src/app.rs `prepare_from_opts` function for more details.
pub fn change_log_var() {
    let level: String = match std::env::var("LOG") {
        Ok(value) => {
            // If the user is passing their own complex filter, bail
            if !value.chars().all(|c| c != '=' && c != ',') {
                return;
            }
            // I'm not sure this is a valid log environment variable, but
            // Vector looks for it from it's command line options
            if value.to_lowercase() == "off" {
                return;
            }
            value
        }
        Err(_) => "INFO".to_string(),
    };

    // This value should have no newlines in it
    let log_value = format!(
        "nexus={level},\
         vector={level},\
         codec={level},\
         vrl={level},\
         file_source={level},\
         tower_limit=trace,\
         rdkafka={level}",
        level = level
    );
    println!("setting log value to {}", log_value);
    std::env::set_var("LOG", log_value);
}

pub fn run() {
    change_log_var();

    let app = Application::prepare().unwrap_or_else(|code| {
        std::process::exit(code);
    });

    app.run();
}

pub fn show_plugins() {
    let sinks: Vec<String> = inventory::iter::<SinkDescription>()
        .map(|t| t.type_str.to_string())
        .collect();
    let sources: Vec<String> = inventory::iter::<SourceDescription>()
        .map(|t| t.type_str.to_string())
        .collect();
    let transforms: Vec<String> = inventory::iter::<TransformDescription>()
        .map(|t| t.type_str.to_string())
        .collect();

    println!("sinks={:?}", sinks);
    println!("sources={:?}", sources);
    println!("transforms={:?}", transforms);
}

#[cfg(test)]
mod tests {
    use vector::config::SinkDescription;
    use vector::config::SourceDescription;
    use vector::config::TransformDescription;

    #[test]
    fn ensure_plugins_are_present() {
        let sinks: Vec<String> = inventory::iter::<SinkDescription>()
            .map(|t| t.type_str.to_string())
            .collect();
        let sources: Vec<String> = inventory::iter::<SourceDescription>()
            .map(|t| t.type_str.to_string())
            .collect();
        let transforms: Vec<String> = inventory::iter::<TransformDescription>()
            .map(|t| t.type_str.to_string())
            .collect();

        assert!(sources.iter().any(|s| s == "disk_queue_length"));
        assert!(sources.iter().any(|s| s == "eventstoredb_metrics"));
        assert!(!sources.iter().any(|s| s == "this_does_not_exist"));
    }
}
