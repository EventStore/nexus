use crate::config::component::ComponentDescription;
use crate::Event;
use serde::{Deserialize, Serialize};

pub mod check_fields;
pub mod is_log;
pub mod is_metric;
pub mod remap;

pub use check_fields::CheckFieldsConfig;

pub trait Condition: Send + Sync + dyn_clone::DynClone {
    fn check(&self, e: &Event) -> bool;

    /// Provides context for a failure. This is potentially mildly expensive if
    /// it involves string building and so should be avoided in hot paths.
    fn check_with_context(&self, e: &Event) -> Result<(), String> {
        if self.check(e) {
            Ok(())
        } else {
            Err("condition failed".into())
        }
    }
}

dyn_clone::clone_trait_object!(Condition);

#[typetag::serde(tag = "type")]
pub trait ConditionConfig: std::fmt::Debug + Send + Sync + dyn_clone::DynClone {
    fn build(&self) -> crate::Result<Box<dyn Condition>>;
}

dyn_clone::clone_trait_object!(ConditionConfig);

pub type ConditionDescription = ComponentDescription<Box<dyn ConditionConfig>>;

inventory::collect!(ConditionDescription);

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum AnyCondition {
    FromType(Box<dyn ConditionConfig>),
    NoTypeCondition(CheckFieldsConfig),
}

impl AnyCondition {
    pub fn build(&self) -> crate::Result<Box<dyn Condition>> {
        match self {
            Self::FromType(c) => c.build(),
            Self::NoTypeCondition(c) => c.build(),
        }
    }
}
