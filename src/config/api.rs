use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr};

#[derive(Debug, Deserialize, Serialize, PartialEq, Copy, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default = "default_address")]
    pub address: Option<SocketAddr>,

    #[serde(default = "default_playground")]
    pub playground: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            playground: default_playground(),
            address: default_address(),
        }
    }
}

fn default_enabled() -> bool {
    false
}

/// By default, the API binds to 127.0.0.1:8686. This function should remain public;
/// `vector top`  will use it to determine which to connect to by default, if no URL
/// override is provided
pub fn default_address() -> Option<SocketAddr> {
    Some(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 8686))
}

fn default_playground() -> bool {
    true
}

impl Options {
    pub fn merge(&mut self, other: Self) -> Result<(), String> {
        // Merge options

        // Try to merge address
        let address = match (self.address, other.address) {
            (None, b) => b,
            (Some(a), None) => Some(a),
            (Some(a), Some(b)) if a == b => Some(a),
            // Prefer non default address
            (Some(a), Some(b)) => {
                match (Some(a) == default_address(), Some(b) == default_address()) {
                    (false, false) => {
                        return Err(format!("Conflicting `api` address: {}, {} .", a, b))
                    }
                    (false, true) => Some(a),
                    (true, _) => Some(b),
                }
            }
        };

        let options = Options {
            address,
            enabled: self.enabled | other.enabled,
            playground: self.playground & other.playground,
        };

        *self = options;
        Ok(())
    }
}

#[test]
fn bool_merge() {
    let mut a = Options {
        enabled: true,
        address: None,
        playground: false,
    };

    a.merge(Options::default()).unwrap();

    assert_eq!(
        a,
        Options {
            enabled: true,
            address: default_address(),
            playground: false,
        }
    );
}

#[test]
fn bind_merge() {
    let address = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9000);
    let mut a = Options {
        enabled: true,
        address: Some(address),
        playground: true,
    };

    a.merge(Options::default()).unwrap();

    assert_eq!(
        a,
        Options {
            enabled: true,
            address: Some(address),
            playground: true,
        }
    );
}

#[test]
fn bind_conflict() {
    let mut a = Options {
        address: Some(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9000)),
        ..Options::default()
    };

    let b = Options {
        address: Some(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9001)),
        ..Options::default()
    };

    assert!(a.merge(b).is_err());
}
