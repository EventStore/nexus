use super::{builder::ConfigBuilder, handle_warnings, validation, Config, TransformOuter};
use indexmap::IndexMap;

pub fn compile(raw: ConfigBuilder, deny_warnings: bool) -> Result<Config, Vec<String>> {
    let mut config = Config {
        global: raw.global,
        #[cfg(feature = "api")]
        api: raw.api,
        healthchecks: raw.healthchecks,
        sources: raw.sources,
        sinks: raw.sinks,
        transforms: raw.transforms,
        tests: raw.tests,
        expansions: Default::default(),
    };

    let mut errors = Vec::new();

    expand_macros(&mut config)?;

    if let Err(warn) = handle_warnings(validation::warnings(&config), deny_warnings) {
        errors.extend(warn);
    }

    if let Err(type_errors) = validation::check_shape(&config) {
        errors.extend(type_errors);
    }

    if let Err(type_errors) = validation::typecheck(&config) {
        errors.extend(type_errors);
    }

    if let Err(type_errors) = validation::check_resources(&config) {
        errors.extend(type_errors);
    }

    if errors.is_empty() {
        Ok(config)
    } else {
        Err(errors)
    }
}

/// Some component configs can act like macros and expand themselves into multiple replacement
/// configs. Performs those expansions and records the relevant metadata.
pub(super) fn expand_macros(config: &mut Config) -> Result<(), Vec<String>> {
    let mut expanded_transforms = IndexMap::new();
    let mut expansions = IndexMap::new();
    let mut errors = Vec::new();

    while let Some((k, mut t)) = config.transforms.pop() {
        if let Some(expanded) = match t.inner.expand() {
            Ok(e) => e,
            Err(err) => {
                errors.push(format!("failed to expand transform '{}': {}", k, err));
                continue;
            }
        } {
            let mut children = Vec::new();
            for (name, child) in expanded {
                let full_name = format!("{}.{}", k, name);
                expanded_transforms.insert(
                    full_name.clone(),
                    TransformOuter {
                        inputs: t.inputs.clone(),
                        inner: child,
                    },
                );
                children.push(full_name);
            }
            expansions.insert(k.clone(), children);
        } else {
            expanded_transforms.insert(k, t);
        }
    }
    config.transforms = expanded_transforms;

    if !errors.is_empty() {
        Err(errors)
    } else {
        config.expansions = expansions;
        Ok(())
    }
}
