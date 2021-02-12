use chrono::{DateTime, Utc};
use remap::Object;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use url::Url;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct CloudEvent {
    pub attributes: Attributes,
    pub data: Option<Data>,
    pub extensions: HashMap<String, ExtensionValue>,
}

impl CloudEvent {
    /// Returns an [`Iterator`] for all the available [CloudEvents Context attributes](https://github.com/cloudevents/spec/blob/master/spec.md#context-attributes) and extensions.
    /// Same as chaining [`Event::iter_attributes()`] and [`Event::iter_extensions()`]
    pub fn iter(&self) -> impl Iterator<Item = (&str, AttributeValue)> {
        self.iter_attributes()
            .chain(self.extensions.iter().map(|(k, v)| (k.as_str(), v.into())))
    }

    /// Returns an [`Iterator`] for all the available [CloudEvents Context attributes](https://github.com/cloudevents/spec/blob/master/spec.md#context-attributes), excluding extensions.
    /// This iterator does not contain the `data` field.
    pub fn iter_attributes(&self) -> impl Iterator<Item = (&str, AttributeValue)> {
        self.attributes.iter()
    }
}

impl Object for CloudEvent {
    fn insert(&mut self, _path: &remap::Path, _value: remap::Value) -> Result<(), String> {
        Ok(())
    }

    fn get(&self, _path: &remap::Path) -> Result<Option<remap::Value>, String> {
        Ok(None)
    }

    fn paths(&self) -> Result<Vec<remap::Path>, String> {
        Ok(Vec::new())
    }

    fn remove(&mut self, _path: &remap::Path, _compact: bool) -> Result<(), String> {
        Ok(())
    }
}

/// Event [data attribute](https://github.com/cloudevents/spec/blob/master/spec.md#event-data) representation
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Data {
    /// Event has a binary payload
    Binary(Vec<u8>),
    /// Event has a non-json string payload
    String(String),
    /// Event has a json payload
    Json(serde_json::Value),
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Data::Binary(vec) => write!(f, "Binary data: {:?}", vec),
            Data::String(s) => write!(f, "String data: {}", s),
            Data::Json(j) => write!(f, "Json data: {}", j),
        }
    }
}

/// Data structure representing [CloudEvents V1.0 context attributes](https://github.com/cloudevents/spec/blob/v1.0/spec.md#context-attributes)
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Attributes {
    pub id: String,
    pub ty: String,
    pub source: String,
    pub data_content_type: Option<String>,
    pub data_schema: Option<String>,
    pub subject: Option<String>,
    pub time: Option<DateTime<Utc>>,
}

impl Attributes {
    pub fn iter(&self) -> impl Iterator<Item = (&str, AttributeValue)> {
        AttributesIter {
            attributes: &self,
            index: 0,
        }
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct AttributesIter<'a> {
    pub attributes: &'a Attributes,
    pub index: usize,
}

impl<'a> Iterator for AttributesIter<'a> {
    type Item = (&'a str, AttributeValue<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => Some(("specversion", AttributeValue::SpecVersion(SpecVersion::V10))),
            1 => Some(("id", AttributeValue::String(&self.attributes.id))),
            2 => Some(("type", AttributeValue::String(&self.attributes.ty))),
            3 => Some(("source", AttributeValue::String(&self.attributes.source))),
            4 => self
                .attributes
                .data_content_type
                .as_ref()
                .map(|v| ("datacontenttype", AttributeValue::String(v))),
            5 => self
                .attributes
                .data_schema
                .as_ref()
                .map(|v| ("dataschema", AttributeValue::String(v))),
            6 => self
                .attributes
                .subject
                .as_ref()
                .map(|v| ("subject", AttributeValue::String(v))),
            7 => self
                .attributes
                .time
                .as_ref()
                .map(|v| ("time", AttributeValue::Time(v))),
            _ => return None,
        };
        self.index += 1;
        if result.is_none() {
            return self.next();
        }
        result
    }
}

impl Default for Attributes {
    fn default() -> Self {
        Attributes {
            id: uuid::Uuid::new_v4().to_string(),
            ty: "type".to_string(),
            source: default_hostname().to_string(),
            data_content_type: None,
            data_schema: None,
            subject: None,
            time: Some(Utc::now()),
        }
    }
}

pub(crate) const V10_ATTRIBUTE_NAMES: [&str; 8] = [
    "specversion",
    "id",
    "type",
    "source",
    "datacontenttype",
    "dataschema",
    "subject",
    "time",
];

/// CloudEvent specification version.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum SpecVersion {
    /// CloudEvents v1.0
    V10,
}

impl SpecVersion {
    /// Returns the string representation of [`SpecVersion`].
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            SpecVersion::V10 => "1.0",
        }
    }

    /// Get all attribute names for this [`SpecVersion`].
    #[inline]
    pub fn attribute_names(&self) -> &'static [&'static str] {
        match self {
            SpecVersion::V10 => &V10_ATTRIBUTE_NAMES,
        }
    }
    /// Get all attribute names for all specification versions.
    /// Note that the result iterator could contain duplicate entries.
    pub fn all_attribute_names() -> impl Iterator<Item = &'static str> {
        vec![SpecVersion::V10]
            .into_iter()
            .flat_map(|s| s.attribute_names().to_owned().into_iter())
    }
}

impl fmt::Display for SpecVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Enum representing a borrowed value of a CloudEvent attribute.
/// This represents the types defined in the [CloudEvent spec type system](https://github.com/cloudevents/spec/blob/v1.0/spec.md#type-system)
#[derive(Debug, PartialEq, Eq)]
pub enum AttributeValue<'a> {
    SpecVersion(SpecVersion),
    String(&'a str),
    URI(&'a Url),
    URIRef(&'a Url),
    Boolean(&'a bool),
    Integer(&'a i64),
    Time(&'a DateTime<Utc>),
}

impl fmt::Display for AttributeValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeValue::SpecVersion(s) => s.fmt(f),
            AttributeValue::String(s) => f.write_str(s),
            AttributeValue::URI(s) => f.write_str(&s.as_str()),
            AttributeValue::URIRef(s) => f.write_str(&s.as_str()),
            AttributeValue::Time(s) => f.write_str(&s.to_rfc3339()),
            AttributeValue::Boolean(b) => f.serialize_bool(**b),
            AttributeValue::Integer(i) => f.serialize_i64(**i),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
/// Represents all the possible [CloudEvents extension](https://github.com/cloudevents/spec/blob/master/spec.md#extension-context-attributes) values
pub enum ExtensionValue {
    /// Represents a [`String`] value.
    String(String),
    /// Represents a [`bool`] value.
    Boolean(bool),
    /// Represents an integer [`i64`] value.
    Integer(i64),
}

pub(crate) fn default_hostname() -> Url {
    url::Url::parse(
        format!(
            "http://{}",
            hostname::get()
                .ok()
                .map(|s| s.into_string().ok())
                .flatten()
                .unwrap_or_else(|| "localhost".to_string())
        )
        .as_ref(),
    )
    .unwrap()
}

impl Serialize for CloudEvent {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serialize_attributes(&self.attributes, &self.data, &self.extensions, serializer)
    }
}

impl std::fmt::Display for CloudEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CloudEvent:\n")?;
        self.iter()
            .map(|(name, val)| write!(f, "  {}: '{}'\n", name, val))
            .collect::<std::fmt::Result>()?;
        match self.data.as_ref() {
            Some(data) => write!(f, "  {}", data)?,
            None => write!(f, "  No data")?,
        }
        write!(f, "\n")
    }
}

fn serialize_attributes<S: Serializer>(
    attributes: &Attributes,
    data: &Option<Data>,
    extensions: &HashMap<String, ExtensionValue>,
    serializer: S,
) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> {
    let num =
        3 + if attributes.data_content_type.is_some() {
            1
        } else {
            0
        } + if attributes.data_schema.is_some() {
            1
        } else {
            0
        } + if attributes.subject.is_some() { 1 } else { 0 }
            + if attributes.time.is_some() { 1 } else { 0 }
            + if data.is_some() { 1 } else { 0 }
            + extensions.len();
    let mut state = serializer.serialize_map(Some(num))?;
    state.serialize_entry("specversion", "1.0")?;
    state.serialize_entry("id", &attributes.id)?;
    state.serialize_entry("type", &attributes.ty)?;
    state.serialize_entry("source", &attributes.source.to_string())?;
    if let Some(data_content_type) = &attributes.data_content_type {
        state.serialize_entry("datacontenttype", data_content_type)?;
    }
    if let Some(data_schema) = &attributes.data_schema {
        state.serialize_entry("dataschema", &data_schema.to_string())?;
    }
    if let Some(subject) = &attributes.subject {
        state.serialize_entry("subject", subject)?;
    }
    if let Some(time) = &attributes.time {
        state.serialize_entry("time", time)?;
    }
    match data {
        Some(Data::Json(j)) => state.serialize_entry("data", j)?,
        Some(Data::String(s)) => state.serialize_entry("data", s)?,
        Some(Data::Binary(v)) => state.serialize_entry("data_base64", &base64::encode(v))?,
        _ => (),
    };
    for (k, v) in extensions {
        state.serialize_entry(k, v)?;
    }
    state.end()
}

impl<'a> From<&'a ExtensionValue> for AttributeValue<'a> {
    fn from(ev: &'a ExtensionValue) -> Self {
        match ev {
            ExtensionValue::String(s) => AttributeValue::String(s),
            ExtensionValue::Boolean(b) => AttributeValue::Boolean(b),
            ExtensionValue::Integer(i) => AttributeValue::Integer(i),
        }
    }
}
