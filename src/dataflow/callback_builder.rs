//! Structures for applying callbacks to bundles of streams.
//!
//! This file loads code generated during build.
//! For more information on how code is generated, see `build.rs` and
//! `scripts/make_callback_builder.py`

include!(concat!(env!("OUT_DIR"), "/callback_builder_generated.rs"));
