use bytes::Bytes;
use chrono::{DateTime, Local, ParseError as ChronoParseError, TimeZone, Utc};
use snafu::{ResultExt, Snafu};
use std::collections::{HashMap, HashSet};
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

#[derive(Debug, Snafu)]
pub enum ConversionError {
    #[snafu(display("Unknown conversion name {:?}", name))]
    UnknownConversion { name: String },
}

/// `Conversion` is a place-holder for a type conversion operation, to convert
/// from a plain `Bytes` into another type. The inner type of every `Value`
/// variant is represented here.
#[derive(Clone, Debug)]
pub enum Conversion {
    Bytes,
    Integer,
    Float,
    Boolean,
    Timestamp,
    TimestampFmt(String),
    TimestampTZFmt(String),
}

impl FromStr for Conversion {
    type Err = ConversionError;
    /// Convert the string into a type conversion. The following
    /// conversion names are supported:
    ///
    ///  * `"asis"`, `"bytes"`, or `"string"` => As-is (no conversion)
    ///  * `"int"` or `"integer"` => Signed integer
    ///  * `"float"` => Floating point number
    ///  * `"bool"` or `"boolean"` => Boolean
    ///  * `"timestamp"` => Timestamp, guessed using a set of formats
    ///  * `"timestamp|FORMAT"` => Timestamp using the given format
    ///
    /// Timestamp parsing does not yet support time zones.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "asis" | "bytes" | "string" => Ok(Conversion::Bytes),
            "integer" | "int" => Ok(Conversion::Integer),
            "float" => Ok(Conversion::Float),
            "bool" | "boolean" => Ok(Conversion::Boolean),
            "timestamp" => Ok(Conversion::Timestamp),
            _ if s.starts_with("timestamp|") => {
                let fmt = &s[10..];
                // DateTime<Utc> can only convert timestamps without
                // time zones, and DateTime<FixedOffset> can only
                // convert with tone zones, so this has to distinguish
                // between the two types of formats.
                if format_has_zone(fmt) {
                    Ok(Conversion::TimestampTZFmt(fmt.into()))
                } else {
                    Ok(Conversion::TimestampFmt(fmt.into()))
                }
            }
            _ => Err(ConversionError::UnknownConversion { name: s.into() }),
        }
    }
}

/// Helper function to parse a conversion map and check against a list of names
pub fn parse_check_conversion_map(
    types: &HashMap<String, String>,
    names: &[impl AsRef<str>],
) -> Result<HashMap<String, Conversion>, ConversionError> {
    // Check if any named type references a nonexistent field
    let names = names.iter().map(|s| s.as_ref()).collect::<HashSet<_>>();
    for name in types.keys() {
        if !names.contains(name.as_str()) {
            tracing::warn!(
                message = "Field was specified in the types but is not a valid field name.",
                field = &name[..]
            );
        }
    }

    parse_conversion_map(types)
}

/// Helper function to parse a mapping of conversion descriptions into actual Conversion values.
pub fn parse_conversion_map(
    types: &HashMap<String, String>,
) -> Result<HashMap<String, Conversion>, ConversionError> {
    types
        .iter()
        .map(|(field, typename)| {
            typename
                .parse::<Conversion>()
                .map(|conv| (field.clone(), conv))
        })
        .collect()
}

#[derive(Debug, Eq, PartialEq, Snafu)]
pub enum Error {
    #[snafu(display("Invalid boolean value {:?}", s))]
    BoolParseError { s: String },
    #[snafu(display("Invalid integer {:?}: {}", s, source))]
    IntParseError { s: String, source: ParseIntError },
    #[snafu(display("Invalid floating point number {:?}: {}", s, source))]
    FloatParseError { s: String, source: ParseFloatError },
    #[snafu(display("Invalid timestamp {:?}: {}", s, source))]
    TimestampParseError { s: String, source: ChronoParseError },
    #[snafu(display("No matching timestamp format found for {:?}", s))]
    AutoTimestampParseError { s: String },
}

impl Conversion {
    /// Use this `Conversion` variant to turn the given `bytes` into a new `T`.
    pub fn convert<T>(&self, bytes: Bytes) -> Result<T, Error>
    where
        T: From<Bytes> + From<i64> + From<f64> + From<bool> + From<DateTime<Utc>>,
    {
        Ok(match self {
            Conversion::Bytes => bytes.into(),
            Conversion::Integer => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<i64>()
                    .with_context(|| IntParseError { s })?
                    .into()
            }
            Conversion::Float => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<f64>()
                    .with_context(|| FloatParseError { s })?
                    .into()
            }
            Conversion::Boolean => parse_bool(&String::from_utf8_lossy(&bytes))?.into(),
            Conversion::Timestamp => parse_timestamp(&String::from_utf8_lossy(&bytes))?.into(),
            Conversion::TimestampFmt(format) => {
                let s = String::from_utf8_lossy(&bytes);
                let dt = Local
                    .datetime_from_str(&s, &format)
                    .with_context(|| TimestampParseError { s })?;

                datetime_to_utc(dt).into()
            }
            Conversion::TimestampTZFmt(format) => {
                let s = String::from_utf8_lossy(&bytes);
                let dt = DateTime::parse_from_str(&s, &format)
                    .with_context(|| TimestampParseError { s })?;

                datetime_to_utc(dt).into()
            }
        })
    }
}

/// Parse a string into a native `bool`. The built in `bool::from_str`
/// only handles two cases, `"true"` and `"false"`. We want to be able
/// to convert from a more diverse set of strings. In particular, the
/// following set of source strings are allowed:
///
///  * `"true"`, `"t"`, `"yes"`, `"y"` (all case-insensitive), and
///  non-zero integers all convert to `true`.
///
///  * `"false"`, `"f"`, `"no"`, `"n"` (all case-insensitive), and `"0"`
///  all convert to `false`.
///
/// Anything else results in a parse error.
fn parse_bool(s: &str) -> Result<bool, Error> {
    match s {
        "true" | "t" | "yes" | "y" => Ok(true),
        "false" | "f" | "no" | "n" | "0" => Ok(false),
        _ => {
            if let Ok(n) = s.parse::<isize>() {
                Ok(n != 0)
            } else {
                // Do the case conversion only if simple matches fail,
                // since this operation can be expensive.
                match s.to_lowercase().as_str() {
                    "true" | "t" | "yes" | "y" => Ok(true),
                    "false" | "f" | "no" | "n" => Ok(false),
                    _ => Err(Error::BoolParseError { s: s.into() }),
                }
            }
        }
    }
}

/// Does the format specifier have a time zone option?
fn format_has_zone(fmt: &str) -> bool {
    fmt.find("%Z").is_some()
        || fmt.find("%z").is_some()
        || fmt.find("%:z").is_some()
        || fmt.find("%#z").is_some()
        || fmt.find("%+").is_some()
}

/// Convert a timestamp with a non-UTC time zone into UTC
fn datetime_to_utc<TZ: TimeZone>(ts: DateTime<TZ>) -> DateTime<Utc> {
    Utc.timestamp(ts.timestamp(), ts.timestamp_subsec_nanos())
}

/// The list of allowed "automatic" timestamp formats
const TIMESTAMP_FORMATS: &[&str] = &[
    "%F %T",           // YYYY-MM-DD HH:MM:SS
    "%v %T",           // DD-Mmm-YYYY HH:MM:SS
    "%FT%T",           // ISO 8601 / RFC 3339 without TZ
    "%m/%d/%Y:%T",     // ???
    "%a, %d %b %Y %T", // RFC 822/2822 without TZ
    "%a %d %b %T %Y",  // `date` command output without TZ
    "%A %d %B %T %Y",  // `date` command output without TZ, long names
    "%a %b %e %T %Y",  // ctime format
];

/// The list of allowed "automatic" timestamp formats for UTC
const TIMESTAMP_UTC_FORMATS: &[&str] = &[
    "%s",     // UNIX timestamp
    "%FT%TZ", // ISO 8601 / RFC 3339 UTC
];

/// The list of allowed "automatic" timestamp formats with time zones
const TIMESTAMP_TZ_FORMATS: &[&str] = &[
    "%+",                 // ISO 8601 / RFC 3339
    "%a %d %b %T %Z %Y",  // `date` command output
    "%a %d %b %T %z %Y",  // `date` command output, numeric TZ
    "%a %d %b %T %#z %Y", // `date` command output, numeric TZ
];

/// Parse a string into a timestamp using one of a set of formats
pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, Error> {
    for format in TIMESTAMP_FORMATS {
        if let Ok(result) = Local.datetime_from_str(s, format) {
            return Ok(datetime_to_utc(result));
        }
    }
    for format in TIMESTAMP_UTC_FORMATS {
        if let Ok(result) = Utc.datetime_from_str(s, format) {
            return Ok(result);
        }
    }
    if let Ok(result) = DateTime::parse_from_rfc3339(s) {
        return Ok(datetime_to_utc(result));
    }
    if let Ok(result) = DateTime::parse_from_rfc2822(s) {
        return Ok(datetime_to_utc(result));
    }
    for format in TIMESTAMP_TZ_FORMATS {
        if let Ok(result) = DateTime::parse_from_str(s, format) {
            return Ok(datetime_to_utc(result));
        }
    }
    Err(Error::AutoTimestampParseError { s: s.into() })
}

#[cfg(test)]
mod tests {
    use super::parse_bool;
    #[cfg(unix)]
    use super::parse_timestamp;
    use super::Bytes;
    #[cfg(unix)]
    use super::{Conversion, Error};
    #[cfg(unix)]
    use chrono::prelude::*;

    #[cfg(unix)]
    const TIMEZONE: &str = "Australia/Brisbane";

    #[derive(PartialEq, Debug, Clone)]
    enum StubValue {
        Bytes(Bytes),
        Timestamp(DateTime<Utc>),
        Float(f64),
        Integer(i64),
        Boolean(bool),
    }

    impl From<Bytes> for StubValue {
        fn from(v: Bytes) -> Self {
            StubValue::Bytes(v)
        }
    }

    impl From<DateTime<Utc>> for StubValue {
        fn from(v: DateTime<Utc>) -> Self {
            StubValue::Timestamp(v)
        }
    }

    impl From<f64> for StubValue {
        fn from(v: f64) -> Self {
            StubValue::Float(v)
        }
    }

    impl From<i64> for StubValue {
        fn from(v: i64) -> Self {
            StubValue::Integer(v)
        }
    }

    impl From<bool> for StubValue {
        fn from(v: bool) -> Self {
            StubValue::Boolean(v)
        }
    }

    #[cfg(unix)]
    fn dateref() -> DateTime<Utc> {
        Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(981173106, 0))
    }

    #[cfg(unix)]
    fn convert<T>(fmt: &str, value: &'static str) -> Result<T, Error>
    where
        T: From<Bytes> + From<i64> + From<f64> + From<bool> + From<DateTime<Utc>>,
    {
        std::env::set_var("TZ", TIMEZONE);
        fmt.parse::<Conversion>()
            .unwrap_or_else(|_| panic!("Invalid conversion {:?}", fmt))
            .convert(value.into())
    }

    #[cfg(unix)] // https://github.com/timberio/vector/issues/1201
    #[test]
    fn timestamp_conversion() {
        assert_eq!(
            convert::<StubValue>("timestamp", "02/03/2001:14:05:06"),
            Ok(dateref().into())
        );
    }

    #[cfg(unix)] // see https://github.com/timberio/vector/issues/1201
    #[test]
    fn timestamp_param_conversion() {
        assert_eq!(
            convert::<StubValue>("timestamp|%Y-%m-%d %H:%M:%S", "2001-02-03 14:05:06"),
            Ok(dateref().into())
        );
    }

    #[cfg(unix)] // see https://github.com/timberio/vector/issues/1201
    #[test]
    fn parse_timestamp_auto() {
        std::env::set_var("TZ", TIMEZONE);
        assert_eq!(parse_timestamp("2001-02-03 14:05:06"), Ok(dateref()));
        assert_eq!(parse_timestamp("02/03/2001:14:05:06"), Ok(dateref()));
        assert_eq!(parse_timestamp("2001-02-03T14:05:06"), Ok(dateref()));
        assert_eq!(parse_timestamp("2001-02-03T04:05:06Z"), Ok(dateref()));
        assert_eq!(parse_timestamp("Sat, 3 Feb 2001 14:05:06"), Ok(dateref()));
        assert_eq!(parse_timestamp("Sat Feb 3 14:05:06 2001"), Ok(dateref()));
        assert_eq!(parse_timestamp("3-Feb-2001 14:05:06"), Ok(dateref()));
        assert_eq!(parse_timestamp("2001-02-02T22:05:06-06:00"), Ok(dateref()));
        assert_eq!(
            parse_timestamp("Sat, 03 Feb 2001 07:05:06 +0300"),
            Ok(dateref())
        );
    }

    // These should perhaps each go into an individual test function to be
    // able to determine what part failed, but that would end up really
    // spamming the test logs.

    #[test]
    fn parse_bool_true() {
        assert_eq!(parse_bool("true"), Ok(true));
        assert_eq!(parse_bool("True"), Ok(true));
        assert_eq!(parse_bool("t"), Ok(true));
        assert_eq!(parse_bool("T"), Ok(true));
        assert_eq!(parse_bool("yes"), Ok(true));
        assert_eq!(parse_bool("YES"), Ok(true));
        assert_eq!(parse_bool("y"), Ok(true));
        assert_eq!(parse_bool("Y"), Ok(true));
        assert_eq!(parse_bool("1"), Ok(true));
        assert_eq!(parse_bool("23456"), Ok(true));
        assert_eq!(parse_bool("-8"), Ok(true));
    }

    #[test]
    fn parse_bool_false() {
        assert_eq!(parse_bool("false"), Ok(false));
        assert_eq!(parse_bool("fAlSE"), Ok(false));
        assert_eq!(parse_bool("f"), Ok(false));
        assert_eq!(parse_bool("F"), Ok(false));
        assert_eq!(parse_bool("no"), Ok(false));
        assert_eq!(parse_bool("NO"), Ok(false));
        assert_eq!(parse_bool("n"), Ok(false));
        assert_eq!(parse_bool("N"), Ok(false));
        assert_eq!(parse_bool("0"), Ok(false));
        assert_eq!(parse_bool("000"), Ok(false));
    }

    #[test]
    fn parse_bool_errors() {
        assert!(parse_bool("X").is_err());
        assert!(parse_bool("yes or no").is_err());
        assert!(parse_bool("123.4").is_err());
    }
}
