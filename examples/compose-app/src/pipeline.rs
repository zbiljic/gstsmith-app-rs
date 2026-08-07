use anyhow::{Context as _, Result};
use gstsmith_app::gst::prelude::*;
use gstsmith_app::{
    BinBase, PipelineBin, PipelineExit, PipelineRunner, ghost_sink, ghost_src, gst,
};

/// A transform bin that converts and rescales video.
///
/// It embeds a [`BinBase`] so several instances can share one pipeline without
/// colliding: the bin and its children derive their names from it. It exposes
/// static ghost pads named `sink` and `src`, so the caller links it into a
/// pipeline exactly like a plain element.
struct ConvertScale {
    base: BinBase,
}

impl ConvertScale {
    fn new(name: impl Into<String>) -> Self {
        Self {
            base: BinBase::new(name),
        }
    }
}

impl PipelineBin for ConvertScale {
    fn build(&self) -> Result<gst::Bin> {
        let bin = self.base.bin();
        let convert = self.base.make("videoconvert", "convert")?;
        let scale = self.base.make("videoscale", "scale")?;

        bin.add_many([&convert, &scale])
            .context("adding convert-scale elements")?;
        gst::Element::link_many([&convert, &scale])
            .context("linking videoconvert to videoscale")?;

        ghost_sink(&bin, &convert)?;
        ghost_src(&bin, &scale)?;

        Ok(bin)
    }
}

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
    let transform = ConvertScale::new("convert-scale").build()?;
    let sink = gst::ElementFactory::make("fakesink")
        .build()
        .context("creating fakesink")?;
    let pipeline = gst::Pipeline::with_name("compose-app");

    pipeline
        .add_many([&source, transform.upcast_ref(), &sink])
        .context("adding pipeline elements")?;
    gst::Element::link_many([&source, transform.upcast_ref(), &sink])
        .context("linking videotestsrc through the transform bin to fakesink")?;

    Ok(pipeline)
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        eprintln!("failed to listen for Ctrl-C: {err}");
    }
}
