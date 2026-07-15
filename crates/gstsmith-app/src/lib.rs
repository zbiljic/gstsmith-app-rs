//! Runtime building blocks for Rust `GStreamer` applications.
//!
//! Applications build their pipeline topology in Rust and hand the resulting
//! [`gst::Pipeline`] to [`PipelineRunner`]. The runner owns the common lifecycle:
//! bus processing, shutdown coordination, and bounded teardown to `Null`.

mod runtime;

pub use gstreamer as gst;
pub use runtime::{PipelineExit, PipelineRunner, ShutdownMode};

/// Initialise the process-wide `GStreamer` runtime.
///
/// This is idempotent and must be called before an application constructs
/// elements or pipelines.
pub fn init() -> anyhow::Result<()> {
    use anyhow::Context as _;

    gst::init().context("initializing GStreamer")
}
