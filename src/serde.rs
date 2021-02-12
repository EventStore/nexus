use indexmap::map::IndexMap;
use serde::{de, Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;

pub fn default_true() -> bool {
    true
}

pub fn default_false() -> bool {
    false
}

pub fn to_string(value: impl serde::Serialize) -> String {
    let value = serde_json::to_value(value).unwrap();
    value.as_str().unwrap().into()
}

/// Answers "Is it possible to skip serializing this value, because it's the
/// default?"
pub(crate) fn skip_serializing_if_default<E: Default + PartialEq>(e: &E) -> bool {
    e == &E::default()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum FieldsOrValue<V> {
    Fields(Fields<V>),
    Value(V),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fields<V>(IndexMap<String, FieldsOrValue<V>>);

impl<V: 'static> Fields<V> {
    pub fn all_fields(self) -> impl Iterator<Item = (String, V)> {
        self.0
            .into_iter()
            .map(|(k, v)| -> Box<dyn Iterator<Item = (String, V)>> {
                match v {
                    // boxing is used as a way to avoid incompatible types of the match arms
                    FieldsOrValue::Value(v) => Box::new(std::iter::once((k, v))),
                    FieldsOrValue::Fields(f) => Box::new(
                        f.all_fields()
                            .map(move |(nested_k, v)| (format!("{}.{}", k, nested_k), v)),
                    ),
                }
            })
            .flatten()
    }
}

/// Enables deserializing from a value that could be a bool or a struct.
/// Example:
/// healthcheck: bool
/// healthcheck.enabled: bool
/// Both are accepted.
pub fn bool_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + From<bool>,
    D: de::Deserializer<'de>,
{
    struct BoolOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> de::Visitor<'de> for BoolOrStruct<T>
    where
        T: Deserialize<'de> + From<bool>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("bool or map")
        }

        fn visit_bool<E>(self, value: bool) -> Result<T, E>
        where
            E: de::Error,
        {
            Ok(value.into())
        }

        fn visit_map<M>(self, map: M) -> Result<T, M::Error>
        where
            M: de::MapAccess<'de>,
        {
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(BoolOrStruct(PhantomData))
}
