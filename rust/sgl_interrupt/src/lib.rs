//! `sgl_interrupt` — context-interruption registry for the SGLang scheduler.
//!
//! The scheduler is single-threaded, but the registry is `Send + Sync` so the
//! GIL can be released during batch sweeps and so that a future multi-threaded
//! scheduler (or the Rust router) can share it.

mod controller;
mod python;

pub use controller::{AbortInfo, AbortReason, InterruptController, Stats};