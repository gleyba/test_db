#![feature(async_closure)]

#[macro_use]
pub mod errors;
#[macro_use]
pub mod misc_utils;

mod aggregator;
pub mod csv_utils;
pub mod db;
mod filter;
pub mod query;
pub mod query_processor;
pub mod record;

#[macro_use]
extern crate guard;

#[macro_use]
extern crate rocket;
