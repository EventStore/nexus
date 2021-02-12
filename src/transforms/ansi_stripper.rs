use crate::{
    config::{DataType, GenerateConfig, TransformConfig, TransformDescription},
    event::Value,
    internal_events::{ANSIStripperFailed, ANSIStripperFieldInvalid, ANSIStripperFieldMissing},
    transforms::{FunctionTransform, Transform},
    Event, Result,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AnsiStripperConfig {
    field: Option<String>,
}

inventory::submit! {
    TransformDescription::new::<AnsiStripperConfig>("ansi_stripper")
}

impl GenerateConfig for AnsiStripperConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self { field: None }).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "ansi_stripper")]
impl TransformConfig for AnsiStripperConfig {
    async fn build(&self) -> Result<Transform> {
        let field = self
            .field
            .clone()
            .unwrap_or_else(|| crate::config::log_schema().message_key().into());

        Ok(Transform::function(AnsiStripper { field }))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn transform_type(&self) -> &'static str {
        "ansi_stripper"
    }
}

#[derive(Clone, Debug)]
pub struct AnsiStripper {
    field: String,
}

impl FunctionTransform for AnsiStripper {
    fn transform(&mut self, output: &mut Vec<Event>, mut event: Event) {
        let log = event.as_mut_log();

        match log.get_mut(&self.field) {
            None => emit!(ANSIStripperFieldMissing { field: &self.field }),
            Some(Value::Bytes(ref mut bytes)) => {
                match strip_ansi_escapes::strip(&bytes) {
                    Ok(b) => *bytes = b.into(),
                    Err(error) => emit!(ANSIStripperFailed {
                        field: &self.field,
                        error
                    }),
                };
            }
            _ => emit!(ANSIStripperFieldInvalid { field: &self.field }),
        }

        output.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Value};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AnsiStripperConfig>();
    }

    macro_rules! assert_foo_bar {
        ($($in:expr),* $(,)?) => {
            $(
                let mut transform = AnsiStripper {
                    field: "message".into(),
                };

                let event = Event::from($in);
                let event = transform.transform_one(event).unwrap();

                assert_eq!(
                    event.into_log().remove(crate::config::log_schema().message_key()).unwrap(),
                    Value::from("foo bar")
                );
            )+
        };
    }

    #[test]
    fn ansi_stripper_transform() {
        assert_foo_bar![
            "\x1b[3;4Hfoo bar",
            "\x1b[3;4ffoo bar",
            "\x1b[3Afoo bar",
            "\x1b[3Bfoo bar",
            "\x1b[3Cfoo bar",
            "\x1b[3Dfoo bar",
            "\x1b[sfoo bar",
            "\x1b[ufoo bar",
            "\x1b[2Jfoo bar",
            "\x1b[Kfoo bar",
            "\x1b[32mfoo\x1b[m bar",
            "\x1b[46mfoo\x1b[0m bar",
            "foo \x1b[46;41mbar",
            "\x1b[=3hfoo bar",
            "\x1b[=3lfoo bar",
            "foo bar",
        ];
    }
}
