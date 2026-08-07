//! Static composition helpers for code-wired `GStreamer` pipelines.
//!
//! These helpers cover the small, repetitive parts of assembling a pipeline by
//! hand: creating a named element with a useful error, exposing a bin's ghost
//! pads, and deferring a link until a dynamic source pad appears. They perform
//! pure construction only. A helper never adds a bin to a pipeline, never
//! retains the containing pipeline, and never carries application policy.
//!
//! By convention a [`PipelineBin`] exposes static ghost pads named after its
//! role: a transform bin exposes `sink` and `src`, a source bin exposes `src`,
//! and a sink bin exposes `sink`. The caller owns naming instances, adding the
//! bin to a pipeline, and linking it to the rest of the graph.

use anyhow::{Context as _, Result, anyhow, bail};
use gstreamer as gst;
use gstreamer::prelude::*;

/// A group of elements that can be assembled into a [`gst::Bin`].
///
/// An implementation creates its child elements, adds and links them, and
/// exposes any ghost pads, returning a bin that has **not** yet been added to a
/// pipeline. A bin does not own or mutate its containing pipeline; the caller
/// links the returned bin like any other element.
///
/// Naming stays with the implementation, not this trait. `GStreamer` requires
/// element names to be unique within a parent bin, so an implementation that may
/// be built more than once into the same pipeline should take a name (or
/// instance id) in its constructor and derive the bin's and its children's names
/// from it. An implementation that never needs a stable name can leave elements
/// unnamed and let `GStreamer` assign unique ones.
pub trait PipelineBin {
    /// Build the bin with its children added, linked, and ghost pads exposed.
    fn build(&self) -> Result<gst::Bin>;
}

/// A name a [`PipelineBin`] implementation embeds to name itself and its
/// children consistently.
///
/// `BinBase` is composed into a bin (a field the bin *has*), not inherited. It
/// keeps the bin's name in one place, derives child names from it, and creates
/// the bin and named child elements. The implementation still owns its topology
/// in [`PipelineBin::build`]; `BinBase` only removes the repeated name-prefix
/// bookkeeping. It performs construction only: it does not hold or add to a
/// pipeline, manage state, or tear a bin down.
///
/// ```
/// use gstsmith_app::{BinBase, PipelineBin, ghost_sink, ghost_src, gst};
/// use gstsmith_app::gst::prelude::*;
///
/// struct ConvertScale {
///     base: BinBase,
/// }
///
/// impl PipelineBin for ConvertScale {
///     fn build(&self) -> anyhow::Result<gst::Bin> {
///         let bin = self.base.bin();
///         let convert = self.base.make("videoconvert", "convert")?;
///         let scale = self.base.make("videoscale", "scale")?;
///         bin.add_many([&convert, &scale])?;
///         gst::Element::link_many([&convert, &scale])?;
///         ghost_sink(&bin, &convert)?;
///         ghost_src(&bin, &scale)?;
///         Ok(bin)
///     }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct BinBase {
    name: String,
}

impl BinBase {
    /// Create a base that names a bin and its children after `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// The bin's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Derive a child element's name as `"<bin>-<suffix>"`.
    #[must_use]
    pub fn child(&self, suffix: &str) -> String {
        format!("{}-{suffix}", self.name)
    }

    /// Create a new empty bin named after this base.
    #[must_use]
    pub fn bin(&self) -> gst::Bin {
        gst::Bin::with_name(&self.name)
    }

    /// Create a named child element, named `"<bin>-<suffix>"`.
    pub fn make(&self, factory: &str, suffix: &str) -> Result<gst::Element> {
        make(factory, &self.child(suffix))
    }
}

/// Create a named element, reporting the factory and name on failure.
pub fn make(factory: &str, name: &str) -> Result<gst::Element> {
    gst::ElementFactory::make(factory)
        .name(name)
        .build()
        .with_context(|| format!("creating element '{factory}' (named '{name}')"))
}

/// Expose an element's static `src` pad as a ghost `src` pad on the bin.
pub fn ghost_src(bin: &gst::Bin, element: &gst::Element) -> Result<()> {
    ghost_pad(bin, element, "src")
}

/// Expose an element's static `sink` pad as a ghost `sink` pad on the bin.
pub fn ghost_sink(bin: &gst::Bin, element: &gst::Element) -> Result<()> {
    ghost_pad(bin, element, "sink")
}

fn ghost_pad(bin: &gst::Bin, element: &gst::Element, pad: &str) -> Result<()> {
    let target = element.static_pad(pad).ok_or_else(|| {
        anyhow!(
            "element '{}' has no static '{pad}' pad to ghost",
            element.name()
        )
    })?;
    let ghost = gst::GhostPad::builder_with_target(&target)
        .with_context(|| format!("creating {pad} ghost pad for '{}'", element.name()))?
        .name(pad)
        .build();
    bin.add_pad(&ghost)
        .with_context(|| format!("adding {pad} ghost pad to bin '{}'", bin.name()))?;
    Ok(())
}

/// Whether `src`'s factory declares a `Sometimes` source pad template.
///
/// A `Sometimes` src pad is the precondition for a `pad-added` signal; without
/// one, [`connect_dynamic`] would install a callback that can never fire.
fn emits_sometimes_src_pad(src: &gst::Element) -> bool {
    let Some(factory) = src.factory() else {
        return false;
    };
    factory.static_pad_templates().iter().any(|template| {
        template.direction() == gst::PadDirection::Src
            && template.presence() == gst::PadPresence::Sometimes
    })
}

/// Link a dynamic (`Sometimes`) source pad to `sink_pad` when it appears.
///
/// Elements such as `decodebin`, demuxers, and `rtspsrc` produce their source
/// pad only after the pipeline starts. This installs a `pad-added` handler that
/// links the first matching pad to `sink_pad`, optionally filtered by
/// `want_caps`. If the pad has not negotiated current caps yet, the filter uses
/// the caps the pad can produce. It returns an error immediately if `src` has no
/// `Sometimes` source pad, since the callback could then never fire.
///
/// The `pad-added` callback runs on a `GStreamer` streaming thread and cannot
/// return a value, so a link failure is posted to the pipeline bus as an element
/// error rather than returned. `label` identifies the logical link in that
/// message.
pub fn connect_dynamic(
    src: &gst::Element,
    sink_pad: gst::Pad,
    want_caps: Option<gst::Caps>,
    label: impl Into<String>,
) -> Result<()> {
    if !emits_sometimes_src_pad(src) {
        bail!(
            "element '{}' has no dynamic (sometimes) src pad, so pad-added would never fire",
            src.name()
        );
    }

    let label = label.into();
    src.connect_pad_added(move |src, pad| {
        if pad.direction() != gst::PadDirection::Src {
            return;
        }
        if let Some(want) = &want_caps {
            // A newly-added pad does not necessarily have negotiated caps yet.
            // Fall back to the caps it can produce instead of permanently
            // ignoring the only `pad-added` notification for that pad.
            let have = pad.current_caps().unwrap_or_else(|| pad.query_caps(None));
            if !have.can_intersect(want) {
                return;
            }
        }
        if sink_pad.is_linked() {
            return;
        }
        if let Err(err) = pad.link(&sink_pad) {
            gst::element_error!(
                src,
                gst::CoreError::Negotiation,
                ["failed to link dynamic pad for {label}: {err}"]
            );
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PipelineExit, PipelineRunner};

    struct Transform {
        base: BinBase,
    }

    impl Transform {
        fn new(name: &str) -> Self {
            Self {
                base: BinBase::new(name),
            }
        }
    }

    impl PipelineBin for Transform {
        fn build(&self) -> Result<gst::Bin> {
            let bin = self.base.bin();
            let convert = self.base.make("videoconvert", "convert")?;
            let scale = self.base.make("videoscale", "scale")?;
            bin.add_many([&convert, &scale])?;
            gst::Element::link_many([&convert, &scale])?;
            ghost_sink(&bin, &convert)?;
            ghost_src(&bin, &scale)?;
            Ok(bin)
        }
    }

    #[test]
    fn bin_base_names_the_bin_and_its_children() {
        crate::init().expect("GStreamer initializes");

        let base = BinBase::new("source");
        assert_eq!(base.name(), "source");
        assert_eq!(base.child("convert"), "source-convert");
        assert_eq!(base.bin().name(), "source");

        let child = base
            .make("fakesink", "sink")
            .expect("fakesink is available");
        assert_eq!(child.name(), "source-sink");
    }

    #[test]
    fn make_builds_a_named_element() {
        crate::init().expect("GStreamer initializes");

        let element = make("fakesink", "named-sink").expect("fakesink is available");

        assert_eq!(element.name(), "named-sink");
    }

    #[test]
    fn make_reports_the_factory_on_failure() {
        crate::init().expect("GStreamer initializes");

        let err =
            make("definitely-not-a-real-factory", "x").expect_err("unknown factory fails to build");

        assert!(err.to_string().contains("definitely-not-a-real-factory"));
    }

    #[test]
    fn ghost_pads_expose_named_directional_pads() {
        crate::init().expect("GStreamer initializes");

        let bin = Transform::new("transform")
            .build()
            .expect("transform bin builds");

        let sink = bin
            .static_pad("sink")
            .expect("bin exposes a sink ghost pad");
        let src = bin.static_pad("src").expect("bin exposes a src ghost pad");
        assert_eq!(sink.direction(), gst::PadDirection::Sink);
        assert_eq!(src.direction(), gst::PadDirection::Src);
    }

    #[tokio::test]
    async fn composed_bin_runs_to_eos() {
        crate::init().expect("GStreamer initializes");

        let source = gst::ElementFactory::make("videotestsrc")
            .property("num-buffers", 2i32)
            .build()
            .expect("videotestsrc is available");
        let transform = Transform::new("transform")
            .build()
            .expect("transform bin builds");
        let sink = gst::ElementFactory::make("fakesink")
            .property("sync", false)
            .build()
            .expect("fakesink is available");
        let pipeline = gst::Pipeline::with_name("compose-test");

        pipeline
            .add_many([&source, transform.upcast_ref(), &sink])
            .expect("elements are added");
        gst::Element::link_many([&source, transform.upcast_ref(), &sink])
            .expect("elements link through ghost pads");

        let observed = pipeline.clone();
        let exit = PipelineRunner::new(pipeline)
            .run(std::future::pending())
            .await
            .expect("composed pipeline runs to EOS");

        assert_eq!(exit, PipelineExit::Eos);
        assert_eq!(observed.current_state(), gst::State::Null);
    }

    #[test]
    fn connect_dynamic_rejects_static_only_sources() {
        crate::init().expect("GStreamer initializes");

        let src = gst::ElementFactory::make("videotestsrc")
            .build()
            .expect("videotestsrc is available");
        let sink = make("fakesink", "static-sink").expect("fakesink is available");
        let sink_pad = sink.static_pad("sink").expect("fakesink has a sink pad");

        let err = connect_dynamic(&src, sink_pad, None, "videotestsrc -> sink")
            .expect_err("a static-only source is rejected");

        assert!(err.to_string().contains("sometimes"));
    }

    #[test]
    fn connect_dynamic_accepts_dynamic_sources() {
        crate::init().expect("GStreamer initializes");

        let src = gst::ElementFactory::make("decodebin")
            .build()
            .expect("decodebin is available");
        let sink = make("fakesink", "dynamic-sink").expect("fakesink is available");
        let sink_pad = sink.static_pad("sink").expect("fakesink has a sink pad");

        connect_dynamic(&src, sink_pad, None, "decodebin -> sink")
            .expect("a dynamic source is accepted");
    }

    #[test]
    fn connect_dynamic_queries_caps_before_negotiation() {
        crate::init().expect("GStreamer initializes");

        let src = gst::ElementFactory::make("decodebin")
            .build()
            .expect("decodebin is available");
        let sink = make("fakesink", "dynamic-sink").expect("fakesink is available");
        let bin = gst::Bin::new();
        bin.add_many([&src, &sink])
            .expect("dynamic source and sink are added to the same bin");
        let sink_pad = sink.static_pad("sink").expect("fakesink has a sink pad");
        let want = gst::Caps::builder("video/x-raw").build();

        connect_dynamic(&src, sink_pad, Some(want), "decodebin -> sink")
            .expect("a dynamic source is accepted");

        let dynamic_pad = gst::Pad::builder(gst::PadDirection::Src)
            .name("dynamic-src")
            .build();
        assert!(
            dynamic_pad.current_caps().is_none(),
            "the regression requires a pad without negotiated caps"
        );

        src.add_pad(&dynamic_pad)
            .expect("adding the pad emits pad-added");

        assert!(
            dynamic_pad.is_linked(),
            "possible caps are queried when current caps are unavailable"
        );
    }
}
