//! The conformance suite of `spec/13-conformance-tdd.md`: black-box tests
//! through the public API only. A `ctNN` module holds the tests of test
//! class CT-NN; `acceptance` holds the example tests of the requirements.

mod acceptance;
mod ct01_model;
mod ct02_reopen;
mod ct04_input_faults;
mod ct10_golden;
mod ct11_format;
mod harness;
mod schemas;
mod support;
