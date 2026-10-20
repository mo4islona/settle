//! The harness of `spec/13-conformance-tdd.md`: an input simulator, a
//! reference model, structural validators and an input ledger.

pub mod agg;
pub mod driver;
pub mod gaps;
pub mod ledger;
pub mod model;
pub mod rng;
pub mod schema;
pub mod sim;
pub mod target;
pub mod validate;
