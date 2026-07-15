use anyhow::{Context as _, Result};
use gstsmith_app::gst::prelude::*;
use gstsmith_app::{PipelineExit, PipelineRunner, gst};

pub async fn run() -> Result<PipelineExit> {
    gstsmith_app::init()?;
    let pipeline = build()?;

    PipelineRunner::new(pipeline).run(shutdown_signal()).await
}

fn build() -> Result<gst::Pipeline> {
    let source = gst::ElementFactory::make("videotestsrc")
        .property("is-live", true)
        .build()
        .context("creating videotestsrc")?;
    let convert = gst::ElementFactory::make("videoconvert")
        .build()
        .context("creating videoconvert")?;
    let sink = gst::ElementFactory::make("fakesink")
        .build()
        .context("creating fakesink")?;
    let pipeline = gst::Pipeline::with_name("basic-app");

    pipeline
        .add_many([&source, &convert, &sink])
        .context("adding pipeline elements")?;
    gst::Element::link_many([&source, &convert, &sink])
        .context("linking videotestsrc to fakesink")?;

    Ok(pipeline)
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        eprintln!("failed to listen for Ctrl-C: {err}");
    }
}
