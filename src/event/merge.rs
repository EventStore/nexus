use crate::event::{LogEvent, Value};
use bytes::BytesMut;

/// Merges all fields specified at `fields` from `incoming` to `current`.
pub fn merge_log_event(current: &mut LogEvent, mut incoming: LogEvent, fields: &[impl AsRef<str>]) {
    for field in fields {
        let incoming_val = match incoming.remove(field) {
            None => continue,
            Some(val) => val,
        };
        match current.get_mut(&field) {
            None => {
                current.insert(field, incoming_val);
            }
            Some(current_val) => merge_value(current_val, incoming_val),
        }
    }
}

/// Merges `incoming` value into `current` value.
///
/// Will concatenate `Bytes` and overwrite the rest value kinds.
pub fn merge_value(current: &mut Value, incoming: Value) {
    match (current, incoming) {
        (Value::Bytes(current_bytes), Value::Bytes(ref incoming)) => {
            let mut bytes = BytesMut::with_capacity(current_bytes.len() + incoming.len());
            bytes.extend_from_slice(&current_bytes[..]);
            bytes.extend_from_slice(&incoming[..]);
            *current_bytes = bytes.freeze();
        }
        (current, incoming) => *current = incoming,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn assert_merge_value(
        current: impl Into<Value>,
        incoming: impl Into<Value>,
        expected: impl Into<Value>,
    ) {
        let mut merged = current.into();
        merge_value(&mut merged, incoming.into());
        assert_eq!(merged, expected.into());
    }

    #[test]
    fn merge_value_works_correctly() {
        assert_merge_value("hello ", "world", "hello world");

        assert_merge_value(true, false, false);
        assert_merge_value(false, true, true);

        assert_merge_value("my_val", true, true);
        assert_merge_value(true, "my_val", "my_val");

        assert_merge_value(1, 2, 2);
    }

    #[test]
    fn merge_event_combines_values_accordingly() {
        // Specify the fields that will be merged.
        // Only the ones listed will be merged from the `incoming` event
        // to the `current`.
        let fields_to_merge = vec![
            "merge".to_string(),
            "merge_a".to_string(),
            "merge_b".to_string(),
            "merge_c".to_string(),
        ];

        let current = {
            let mut log = LogEvent::default();

            log.insert("merge", "hello "); // will be concatenated with the `merged` from `incoming`.
            log.insert("do_not_merge", "my_first_value"); // will remain as is, since it's not selected for merging.

            log.insert("merge_a", true); // will be overwritten with the `merge_a` from `incoming` (since it's a non-bytes kind).
            log.insert("merge_b", 123); // will be overwritten with the `merge_b` from `incoming` (since it's a non-bytes kind).

            log.insert("a", true); // will remain as is since it's not selected for merge.
            log.insert("b", 123); // will remain as is since it's not selected for merge.

            // `c` is not present in the `current`, and not selected for merge,
            // so it won't be included in the final event.

            log
        };

        let incoming = {
            let mut log = LogEvent::default();

            log.insert("merge", "world"); // will be concatenated to the `merge` from `current`.
            log.insert("do_not_merge", "my_second_value"); // will be ignored, since it's not selected for merge.

            log.insert("merge_b", 456); // will be merged in as `456`.
            log.insert("merge_c", false); // will be merged in as `false`.

            // `a` will remain as-is, since it's not marked for merge and
            // neither is it specified in the `incoming` event.
            log.insert("b", 456); // `b` not marked for merge, will not change.
            log.insert("c", true); // `c` not marked for merge, will be ignored.

            log
        };

        let mut merged = current;
        merge_log_event(&mut merged, incoming, &fields_to_merge);

        let expected = {
            let mut log = LogEvent::default();
            log.insert("merge", "hello world");
            log.insert("do_not_merge", "my_first_value");
            log.insert("a", true);
            log.insert("b", 123);
            log.insert("merge_a", true);
            log.insert("merge_b", 456);
            log.insert("merge_c", false);
            log
        };

        assert_eq!(merged, expected);
    }
}
