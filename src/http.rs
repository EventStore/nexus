use serde::de::Visitor;
use serde::{Deserializer, Serializer};

struct UriVisitor;

impl<'de> Visitor<'de> for UriVisitor {
    type Value = http::Uri;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "http::Uri")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v.parse::<http::Uri>() {
            Ok(uri) => Ok(uri),
            Err(e) => Err(serde::de::Error::custom(format!("{}", e))),
        }
    }
}

pub fn deserialize_uri<'de, D>(deserializer: D) -> Result<http::Uri, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(UriVisitor)
}

pub fn serialize_uri<S>(value: &http::Uri, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(value.to_string().as_str())
}
