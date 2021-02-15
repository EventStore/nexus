//! Vector GraphQL client library, for the Vector GraphQL API server.
//!
//! Contains:
//!
//! 1. A GraphQL query client, for queries/mutations over HTTP(s)
//! 2. A GraphQL subscription client, for long-lived, multiplexed subscriptions over WebSockets
//! 3. GraphQL queries/mutations/subscriptions, defined in `graphql/**/*.graphql` files
//! 4. Extension methods for each client, for executing queries/subscriptions, and returning
//! deserialized JSON responses
//!

#![deny(missing_debug_implementations, missing_copy_implementations)]

mod client;
/// GraphQL queries
pub mod gql;
mod subscription;
pub mod test;

pub use client::*;
pub use subscription::*;
