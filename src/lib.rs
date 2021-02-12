#![recursion_limit = "256"] // for async-stream
#![allow(clippy::approx_constant)]
#![allow(clippy::float_cmp)]
#![allow(clippy::blocks_in_if_conditions)]
#![allow(clippy::match_wild_err_arm)]
#![allow(clippy::new_ret_no_self)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::trivial_regex)]
#![allow(clippy::type_complexity)]
#![allow(clippy::unit_arg)]
#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::trivially_copy_pass_by_ref)]

#[macro_use]
extern crate tracing;

mod buffers;
mod conditions;
#[macro_use]
mod config;
mod dns;
mod event;
mod serde;
mod topology;
#[macro_use]
mod internal_events;
mod http;
mod mapping;
mod shutdown;
mod sink;
mod sinks;
mod sources;
mod stream;
mod tcp;
mod template;
mod tls;
mod trace;
mod transforms;
mod trigger;
mod types;
mod validate;

pub use event::{Event, Value};
pub use pipeline::Pipeline;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
