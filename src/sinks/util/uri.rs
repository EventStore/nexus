use crate::http::Auth;
use http::uri::{Authority, PathAndQuery, Scheme, Uri};
use percent_encoding::percent_decode_str;
use serde::{
    de::{Error, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt;
use std::str::FromStr;

/// A wrapper for `http::Uri` that implements the serde traits.
/// Authorization credentials, if exist, will be removed from the URI and stored in `auth`.
/// For example: "http://user:password@example.com".
#[derive(Default, Debug, Clone)]
pub struct UriSerde {
    pub uri: Uri,
    pub auth: Option<Auth>,
}

impl UriSerde {
    /// `Uri` supports incomplete URIs such as "/test", "example.com", etc.
    /// This function fills in empty scheme with HTTP,
    /// and empty authority with "127.0.0.1".
    pub fn with_default_parts(&self) -> Self {
        let mut parts = self.uri.clone().into_parts();
        if parts.scheme.is_none() {
            parts.scheme = Some(Scheme::HTTP);
        }
        if parts.authority.is_none() {
            parts.authority = Some(Authority::from_static("127.0.0.1"));
        }
        if parts.path_and_query.is_none() {
            // just an empty `path_and_query`,
            // but `from_parts` will fail without this.
            parts.path_and_query = Some(PathAndQuery::from_static(""));
        }
        let uri = Uri::from_parts(parts).expect("invalid parts");
        Self {
            uri,
            auth: self.auth.clone(),
        }
    }
}

impl Serialize for UriSerde {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'a> Deserialize<'a> for UriSerde {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_str(UriVisitor)
    }
}

impl fmt::Display for UriSerde {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.uri.authority(), &self.auth) {
            (Some(authority), Some(Auth::Basic { user, password })) => {
                let authority = format!("{}:{}@{}", user, password, authority);
                let authority =
                    Authority::from_maybe_shared(authority).map_err(|_| std::fmt::Error)?;
                let mut parts = self.uri.clone().into_parts();
                parts.authority = Some(authority);
                Uri::from_parts(parts).unwrap().fmt(f)
            }
            _ => self.uri.fmt(f),
        }
    }
}

struct UriVisitor;

impl<'a> Visitor<'a> for UriVisitor {
    type Value = UriSerde;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a string containing a valid HTTP Uri")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        s.parse().map_err(Error::custom)
    }
}

impl FromStr for UriSerde {
    type Err = <Uri as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Uri>().map(Into::into)
    }
}

impl From<Uri> for UriSerde {
    fn from(uri: Uri) -> Self {
        match uri.authority() {
            None => Self { uri, auth: None },
            Some(authority) => {
                let (authority, auth) = get_basic_auth(authority);

                let mut parts = uri.into_parts();
                parts.authority = Some(authority);
                let uri = Uri::from_parts(parts).unwrap();

                Self { uri, auth }
            }
        }
    }
}

fn get_basic_auth(authority: &Authority) -> (Authority, Option<Auth>) {
    // We get a valid `Authority` as input, therefore cannot fail here.
    let mut url = url::Url::parse(&format!("http://{}", authority)).expect("invalid authority");

    let user = url.username();
    if !user.is_empty() {
        let user = percent_decode_str(user).decode_utf8_lossy().into_owned();

        let password = url.password().unwrap_or("");
        let password = percent_decode_str(password)
            .decode_utf8_lossy()
            .into_owned();

        // These methods have the same failure condition as `username`,
        // because we have a non-empty username, they cannot fail here.
        url.set_username("").expect("unexpected empty authority");
        url.set_password(None).expect("unexpected empty authority");

        // We get a valid `Authority` as input, therefore cannot fail here.
        let authority = Uri::from_maybe_shared(url.into_string())
            .expect("invalid url")
            .authority()
            .expect("unexpected empty authority")
            .clone();

        (authority, Some(Auth::Basic { user, password }))
    } else {
        (authority.clone(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_parse(input: &str, expected_uri: &str, expected_auth: Option<(&str, &str)>) {
        let UriSerde { uri, auth } = input.parse().unwrap();
        assert_eq!(
            uri,
            Uri::from_maybe_shared(expected_uri.to_owned()).unwrap()
        );
        assert_eq!(
            auth,
            expected_auth.map(|(user, password)| {
                Auth::Basic {
                    user: user.to_owned(),
                    password: password.to_owned(),
                }
            })
        );
    }

    #[test]
    fn parse_endpoint() {
        test_parse(
            "http://user:pass@example.com/test",
            "http://example.com/test",
            Some(("user", "pass")),
        );

        test_parse("localhost:8080", "localhost:8080", None);

        test_parse("/api/test", "/api/test", None);

        test_parse(
            "http://user:pass;@example.com",
            "http://example.com",
            Some(("user", "pass;")),
        );

        test_parse(
            "user:pass@example.com",
            "example.com",
            Some(("user", "pass")),
        );

        test_parse("user@example.com", "example.com", Some(("user", "")));
    }
}
