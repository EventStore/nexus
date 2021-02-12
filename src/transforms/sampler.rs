use crate::{
    conditions::{CheckFieldsConfig, Condition, ConditionConfig},
    config::{DataType, GenerateConfig, TransformConfig, TransformDescription},
    event::Event,
    internal_events::SamplerEventDiscarded,
    transforms::{FunctionTransform, Transform},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SamplerConfig {
    pub rate: u64,
    pub key_field: Option<String>,
    pub exclude: Option<CheckFieldsConfig>,
}

inventory::submit! {
    TransformDescription::new::<SamplerConfig>("sampler")
}

impl GenerateConfig for SamplerConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            rate: 10,
            key_field: None,
            exclude: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "sampler")]
impl TransformConfig for SamplerConfig {
    async fn build(&self) -> crate::Result<Transform> {
        Ok(Transform::function(Sampler::new(
            self.rate,
            self.key_field.clone(),
            self.exclude
                .as_ref()
                .map(|condition| condition.build())
                .transpose()?,
        )))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn transform_type(&self) -> &'static str {
        "sampler"
    }
}

#[derive(Clone)]
pub struct Sampler {
    rate: u64,
    key_field: Option<String>,
    exclude: Option<Box<dyn Condition>>,
    count: u64,
}

impl Sampler {
    pub fn new(rate: u64, key_field: Option<String>, exclude: Option<Box<dyn Condition>>) -> Self {
        Self {
            rate,
            key_field,
            exclude,
            count: 0,
        }
    }
}

impl FunctionTransform for Sampler {
    fn transform(&mut self, output: &mut Vec<Event>, mut event: Event) {
        if let Some(condition) = self.exclude.as_ref() {
            if condition.check(&event) {
                output.push(event);
                return;
            }
        }

        let value = self
            .key_field
            .as_ref()
            .and_then(|key_field| event.as_log().get(key_field))
            .map(|v| v.to_string_lossy());

        let num = if let Some(value) = value {
            seahash::hash(value.as_bytes())
        } else {
            self.count
        };

        self.count = (self.count + 1) % self.rate;

        if num % self.rate == 0 {
            event
                .as_mut_log()
                .insert("sample_rate", self.rate.to_string());
            output.push(event);
        } else {
            emit!(SamplerEventDiscarded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        conditions::check_fields::CheckFieldsPredicateArg, config::log_schema, event::Event,
        test_util::random_lines,
    };
    use approx::assert_relative_eq;
    use indexmap::IndexMap;

    fn condition_contains(pre: &str) -> Box<dyn Condition> {
        condition(log_schema().message_key(), "contains", pre)
    }

    fn condition(field: &str, condition: &str, value: &str) -> Box<dyn Condition> {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            format!("{}.{}", field, condition),
            CheckFieldsPredicateArg::String(value.into()),
        );

        CheckFieldsConfig::new(preds).build().unwrap()
    }

    #[test]
    fn genreate_config() {
        crate::test_util::test_generate_config::<SamplerConfig>();
    }

    #[test]
    fn hash_samples_at_roughly_the_configured_rate() {
        let num_events = 10000;

        let events = random_events(num_events);
        let mut sampler = Sampler::new(
            2,
            Some(log_schema().message_key().into()),
            Some(condition_contains("na")),
        );
        let total_passed = events
            .into_iter()
            .filter_map(|event| sampler.transform_one(event))
            .count();
        let ideal = 1.0f64 / 2.0f64;
        let actual = total_passed as f64 / num_events as f64;
        assert_relative_eq!(ideal, actual, epsilon = ideal * 0.5);

        let events = random_events(num_events);
        let mut sampler = Sampler::new(
            25,
            Some(log_schema().message_key().into()),
            Some(condition_contains("na")),
        );
        let total_passed = events
            .into_iter()
            .filter_map(|event| sampler.transform_one(event))
            .count();
        let ideal = 1.0f64 / 25.0f64;
        let actual = total_passed as f64 / num_events as f64;
        assert_relative_eq!(ideal, actual, epsilon = ideal * 0.5);
    }

    #[test]
    fn hash_consistently_samples_the_same_events() {
        let events = random_events(1000);
        let mut sampler = Sampler::new(
            2,
            Some(log_schema().message_key().into()),
            Some(condition_contains("na")),
        );

        let first_run = events
            .clone()
            .into_iter()
            .filter_map(|event| sampler.transform_one(event))
            .collect::<Vec<_>>();
        let second_run = events
            .into_iter()
            .filter_map(|event| sampler.transform_one(event))
            .collect::<Vec<_>>();

        assert_eq!(first_run, second_run);
    }

    #[test]
    fn always_passes_events_matching_pass_list() {
        for key_field in &[None, Some(log_schema().message_key().into())] {
            let event = Event::from("i am important");
            let mut sampler =
                Sampler::new(0, key_field.clone(), Some(condition_contains("important")));
            let iterations = 0..1000;
            let total_passed = iterations
                .filter_map(|_| sampler.transform_one(event.clone()))
                .count();
            assert_eq!(total_passed, 1000);
        }
    }

    #[test]
    fn handles_key_field() {
        for key_field in &[None, Some(log_schema().timestamp_key().into())] {
            let event = Event::from("nananana");
            let mut sampler = Sampler::new(
                0,
                key_field.clone(),
                Some(condition(log_schema().timestamp_key(), "contains", ":")),
            );
            let iterations = 0..1000;
            let total_passed = iterations
                .filter_map(|_| sampler.transform_one(event.clone()))
                .count();
            assert_eq!(total_passed, 1000);
        }
    }

    #[test]
    fn sampler_adds_sampling_rate_to_event() {
        for key_field in &[None, Some(log_schema().message_key().into())] {
            let events = random_events(10000);
            let mut sampler = Sampler::new(10, key_field.clone(), Some(condition_contains("na")));
            let passing = events
                .into_iter()
                .filter(|s| {
                    !s.as_log()[log_schema().message_key()]
                        .to_string_lossy()
                        .contains("na")
                })
                .find_map(|event| sampler.transform_one(event))
                .unwrap();
            assert_eq!(passing.as_log()["sample_rate"], "10".into());

            let events = random_events(10000);
            let mut sampler = Sampler::new(25, key_field.clone(), Some(condition_contains("na")));
            let passing = events
                .into_iter()
                .filter(|s| {
                    !s.as_log()[log_schema().message_key()]
                        .to_string_lossy()
                        .contains("na")
                })
                .find_map(|event| sampler.transform_one(event))
                .unwrap();
            assert_eq!(passing.as_log()["sample_rate"], "25".into());

            // If the event passed the regex check, don't include the sampling rate
            let mut sampler = Sampler::new(25, key_field.clone(), Some(condition_contains("na")));
            let event = Event::from("nananana");
            let passing = sampler.transform_one(event).unwrap();
            assert!(passing.as_log().get("sample_rate").is_none());
        }
    }

    fn random_events(n: usize) -> Vec<Event> {
        random_lines(10).take(n).map(Event::from).collect()
    }
}
