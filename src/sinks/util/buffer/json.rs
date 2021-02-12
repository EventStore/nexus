use super::super::batch::{
    err_event_too_large, Batch, BatchConfig, BatchError, BatchSettings, BatchSize, PushResult,
};
use serde_json::value::{to_raw_value, RawValue, Value};

pub type BoxedRawValue = Box<RawValue>;

/// A `batch` implementation for storing an array of json
/// values.
#[derive(Debug)]
pub struct JsonArrayBuffer {
    buffer: Vec<BoxedRawValue>,
    total_bytes: usize,
    settings: BatchSize<Self>,
}

impl JsonArrayBuffer {
    pub fn new(settings: BatchSize<Self>) -> Self {
        Self {
            buffer: Vec::new(),
            total_bytes: 0,
            settings,
        }
    }
}

impl Batch for JsonArrayBuffer {
    type Input = Value;
    type Output = Vec<BoxedRawValue>;

    fn get_settings_defaults(
        config: BatchConfig,
        defaults: BatchSettings<Self>,
    ) -> Result<BatchSettings<Self>, BatchError> {
        Ok(config
            .use_size_as_bytes()?
            .get_settings_or_default(defaults))
    }

    fn push(&mut self, item: Self::Input) -> PushResult<Self::Input> {
        let raw_item = to_raw_value(&item).expect("Value should be valid json");
        let new_len = self.total_bytes + raw_item.get().len() + 1;
        if self.is_empty() && new_len >= self.settings.bytes {
            err_event_too_large(raw_item.get().len())
        } else if self.buffer.len() >= self.settings.events || new_len > self.settings.bytes {
            PushResult::Overflow(item)
        } else {
            self.total_bytes = new_len;
            self.buffer.push(raw_item);
            PushResult::Ok(
                self.buffer.len() >= self.settings.events || new_len >= self.settings.bytes,
            )
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn fresh(&self) -> Self {
        Self::new(self.settings)
    }

    fn finish(self) -> Self::Output {
        self.buffer
    }

    fn num_items(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::PushResult;
    use super::*;
    use serde_json::json;

    #[test]
    fn multi_object_array() {
        let batch = BatchSettings::default().bytes(9999).events(2).size;
        let mut buffer = JsonArrayBuffer::new(batch);

        assert_eq!(
            buffer.push(json!({
                "key1": "value1"
            })),
            PushResult::Ok(false)
        );

        assert_eq!(
            buffer.push(json!({
                "key2": "value2"
            })),
            PushResult::Ok(true)
        );

        assert!(matches!(buffer.push(json!({})), PushResult::Overflow(_)));

        assert_eq!(buffer.num_items(), 2);
        assert_eq!(buffer.total_bytes, 36);

        let json = buffer.finish();

        let wrapped = serde_json::to_string(&json!({
            "arr": json,
        }))
        .unwrap();

        let expected = serde_json::to_string(&json!({
            "arr": [
                {
                    "key1": "value1"
                },
                {
                    "key2": "value2"
                },
            ]
        }))
        .unwrap();

        assert_eq!(wrapped, expected);
    }
}
