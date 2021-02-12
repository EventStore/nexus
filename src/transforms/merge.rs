use crate::{
    config::{DataType, TransformConfig, TransformDescription},
    event::discriminant::Discriminant,
    event::merge_state::LogEventMergeState,
    event::{self, Event},
    transforms::{TaskTransform, Transform},
};
use futures01::Stream as Stream01;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct MergeConfig {
    /// The field that indicates that the event is partial. A consequent stream
    /// of partial events along with the first non-partial event will be merged
    /// together.
    pub partial_event_marker_field: String,
    /// Fields to merge. The values of these fields will be merged into the
    /// first partial event. Fields not specified here will be ignored.
    /// Merging process takes the first partial event and the base, then it
    /// merges in the fields from each successive partial event, until a
    /// non-partial event arrives. Finally, the non-partial event fields are
    /// merged in, producing the resulting merged event.
    // Deprecated name is merge_fields
    #[serde(alias = "merge_fields")]
    pub fields: Vec<String>,
    /// An ordered list of fields to distinguish streams by. Each stream has a
    /// separate partial event merging state. Should be used to prevent events
    /// from unrelated sources from mixing together, as this affects partial
    /// event processing.
    pub stream_discriminant_fields: Vec<String>,
}

inventory::submit! {
    TransformDescription::new::<MergeConfig>("merge")
}

impl_generate_config_from_default!(MergeConfig);

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            partial_event_marker_field: event::PARTIAL.to_string(),
            fields: vec![crate::config::log_schema().message_key().to_string()],
            stream_discriminant_fields: vec![],
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "merge")]
impl TransformConfig for MergeConfig {
    async fn build(&self) -> crate::Result<Transform> {
        Ok(Transform::task(Merge::from(self.clone())))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn transform_type(&self) -> &'static str {
        "merge"
    }
}

pub struct Merge {
    partial_event_marker_field: String,
    fields: Vec<String>,
    stream_discriminant_fields: Vec<String>,
    log_event_merge_states: HashMap<Discriminant, LogEventMergeState>,
}

impl Merge {
    fn transform_one(&mut self, event: Event) -> Option<Event> {
        let mut event = event.into_log();

        // Prepare an event's discriminant.
        let discriminant = Discriminant::from_log_event(&event, &self.stream_discriminant_fields);

        // TODO: `lua` transform doesn't support assigning non-string values.
        // Normally we'd check for the field value to be `true`, and only then
        // consider event partial, but, to simplify the integration, for now we
        // only check for the field presence. We can switch this to check the
        // value to be `true` when the `lua` supports setting boolean fields
        // easily, as we expect users to rely on `lua` transform to implement
        // custom partial markers.

        // If current event has the partial marker, consider it partial.
        // Remove the partial marker from the event and stash it.
        if event.remove(&self.partial_event_marker_field).is_some() {
            // We got a partial event. Initialize a partial event merging state
            // if there's none available yet, or extend the existing one by
            // merging the incoming partial event in.
            match self.log_event_merge_states.entry(discriminant) {
                hash_map::Entry::Vacant(entry) => {
                    entry.insert(LogEventMergeState::new(event));
                }
                hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().merge_in_next_event(event, &self.fields);
                }
            }

            // Do not emit the event yet.
            return None;
        }

        // We got non-partial event. Attempt to get a partial event merge
        // state. If it's empty then we don't have a backlog of partial events
        // so we just return the event as-is. Otherwise we proceed to merge in
        // the final non-partial event to the partial event merge state - and
        // then return the merged event.
        let log_event_merge_state = match self.log_event_merge_states.remove(&discriminant) {
            Some(log_event_merge_state) => log_event_merge_state,
            None => {
                return Some(Event::Log(event));
            }
        };

        // Merge in the final non-partial event and consume the merge state in
        // exchange for the merged event.
        let merged_event = log_event_merge_state.merge_in_final_event(event, &self.fields);

        // Return the merged event.
        Some(Event::Log(merged_event))
    }
}

impl From<MergeConfig> for Merge {
    fn from(config: MergeConfig) -> Self {
        Self {
            partial_event_marker_field: config.partial_event_marker_field,
            fields: config.fields,
            stream_discriminant_fields: config.stream_discriminant_fields,
            log_event_merge_states: HashMap::new(),
        }
    }
}

impl TaskTransform for Merge {
    fn transform(
        self: Box<Self>,
        task: Box<dyn Stream01<Item = Event, Error = ()> + Send>,
    ) -> Box<dyn Stream01<Item = Event, Error = ()> + Send>
    where
        Self: 'static,
    {
        let mut inner = self;
        Box::new(task.filter_map(move |v| inner.transform_one(v)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{self, Event};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<MergeConfig>();
    }

    fn make_partial(mut event: Event) -> Event {
        event.as_mut_log().insert(event::PARTIAL, true);
        event
    }

    #[test]
    fn merge_passthroughs_non_partial_events() {
        let mut merge = Merge::from(MergeConfig::default());

        // A non-partial event.
        let sample_event = Event::from("hello world");

        // Once processed by the transform.
        let merged_event = merge.transform_one(sample_event.clone()).unwrap();

        // Should be returned as is.
        assert_eq!(merged_event, sample_event);
    }

    #[test]
    fn merge_merges_partial_events() {
        let mut merge = Merge::from(MergeConfig::default());

        let partial_event_1 = make_partial(Event::from("hel"));
        let partial_event_2 = make_partial(Event::from("lo "));
        let non_partial_event = Event::from("world");

        assert!(merge.transform_one(partial_event_1).is_none());
        assert!(merge.transform_one(partial_event_2).is_none());
        let merged_event = merge.transform_one(non_partial_event).unwrap();

        assert_eq!(
            merged_event
                .as_log()
                .get("message")
                .unwrap()
                .as_bytes()
                .as_ref(),
            b"hello world"
        );

        // Merged event shouldn't contain partial event marker.
        assert!(!merged_event.as_log().contains(&*event::PARTIAL));
    }

    #[test]
    fn merge_merges_partial_events_from_separate_streams() {
        let stream_discriminant_field = "stream_name".to_string();

        let mut merge = Merge::from(MergeConfig {
            stream_discriminant_fields: vec![stream_discriminant_field.clone()],
            ..MergeConfig::default()
        });

        let make_event = |message, stream| {
            let mut event = Event::from(message);
            event
                .as_mut_log()
                .insert(stream_discriminant_field.clone(), stream);
            event
        };

        let s1_partial_event_1 = make_partial(make_event("hel", "s1"));
        let s1_partial_event_2 = make_partial(make_event("lo ", "s1"));
        let s1_non_partial_event = make_event("world", "s1");

        let s2_partial_event_1 = make_partial(make_event("lo", "s2"));
        let s2_partial_event_2 = make_partial(make_event("rem ip", "s2"));
        let s2_non_partial_event = make_event("sum", "s2");

        // Simulate events arriving in non-trivial order.
        assert!(merge.transform_one(s1_partial_event_1).is_none());
        assert!(merge.transform_one(s2_partial_event_1).is_none());
        assert!(merge.transform_one(s1_partial_event_2).is_none());
        let s1_merged_event = merge.transform_one(s1_non_partial_event).unwrap();
        assert!(merge.transform_one(s2_partial_event_2).is_none());
        let s2_merged_event = merge.transform_one(s2_non_partial_event).unwrap();

        assert_eq!(
            s1_merged_event
                .as_log()
                .get("message")
                .unwrap()
                .as_bytes()
                .as_ref(),
            b"hello world"
        );

        assert_eq!(
            s2_merged_event
                .as_log()
                .get("message")
                .unwrap()
                .as_bytes()
                .as_ref(),
            b"lorem ipsum"
        );

        // Merged events shouldn't contain partial event marker.
        assert!(!s1_merged_event.as_log().contains(&*event::PARTIAL));
        assert!(!s2_merged_event.as_log().contains(&*event::PARTIAL));
    }
}
