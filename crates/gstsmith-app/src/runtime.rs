use std::{future::Future, time::Duration};

use anyhow::{Context as _, Result, anyhow, bail};
use futures::{Stream, StreamExt as _};
use gstreamer as gst;
use gstreamer::prelude::*;

/// Why a pipeline runner stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipelineExit {
    /// The pipeline produced an end-of-stream message.
    Eos,
    /// The application-provided shutdown future completed.
    Shutdown,
}

/// How a running pipeline should stop after shutdown is requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    /// Stop immediately by taking the pipeline to `Null`.
    ///
    /// This is appropriate for live outputs that have no trailer to finalise.
    Immediate,
    /// Send EOS and wait up to the supplied timeout for it to return on the bus.
    ///
    /// This is appropriate for muxers and file outputs that need to write a
    /// trailer before the pipeline is stopped.
    Eos { timeout: Duration },
}

/// Drives one `GStreamer` pipeline until EOS, failure, or application shutdown.
pub struct PipelineRunner {
    pipeline: gst::Pipeline,
    shutdown_mode: ShutdownMode,
    state_change_timeout: Duration,
}

impl PipelineRunner {
    /// Create a runner that stops immediately on application shutdown.
    #[must_use]
    pub fn new(pipeline: gst::Pipeline) -> Self {
        Self {
            pipeline,
            shutdown_mode: ShutdownMode::Immediate,
            state_change_timeout: Duration::from_secs(5),
        }
    }

    /// Set how application shutdown should be handled.
    #[must_use]
    pub const fn shutdown_mode(mut self, mode: ShutdownMode) -> Self {
        self.shutdown_mode = mode;
        self
    }

    /// Set the maximum time to wait for the pipeline to reach `Null`.
    #[must_use]
    pub const fn state_change_timeout(mut self, timeout: Duration) -> Self {
        self.state_change_timeout = timeout;
        self
    }

    /// Run the pipeline alongside an application-provided shutdown future.
    ///
    /// The pipeline is always asked to reach `Null` before this method returns,
    /// including when startup or bus processing fails.
    pub async fn run<S>(self, shutdown: S) -> Result<PipelineExit>
    where
        S: Future<Output = ()> + Send,
    {
        let Self {
            pipeline,
            shutdown_mode,
            state_change_timeout,
        } = self;

        let outcome = drive(&pipeline, shutdown, shutdown_mode).await;
        let cleanup = stop(&pipeline, state_change_timeout);

        match (outcome, cleanup) {
            (Ok(exit), Ok(())) => Ok(exit),
            (Err(err), Ok(())) | (Ok(_), Err(err)) => Err(err),
            (Err(err), Err(cleanup_err)) => Err(anyhow!(
                "{err:#}; additionally failed to stop the pipeline: {cleanup_err:#}"
            )),
        }
    }
}

async fn drive<S>(
    pipeline: &gst::Pipeline,
    shutdown: S,
    shutdown_mode: ShutdownMode,
) -> Result<PipelineExit>
where
    S: Future<Output = ()> + Send,
{
    let bus = pipeline.bus().context("getting the pipeline message bus")?;
    let mut messages = bus.stream();

    pipeline
        .set_state(gst::State::Playing)
        .context("setting the pipeline to Playing")?;

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => {
                return handle_shutdown(pipeline, &mut messages, shutdown_mode).await;
            }
            message = messages.next() => {
                return match message {
                    Some(message) => match terminal_message(&message)? {
                        Some(exit) => Ok(exit),
                        None => continue,
                    },
                    None => Err(anyhow!("pipeline message bus closed unexpectedly")),
                };
            }
        }
    }
}

async fn handle_shutdown<M>(
    pipeline: &gst::Pipeline,
    messages: &mut M,
    mode: ShutdownMode,
) -> Result<PipelineExit>
where
    M: Stream<Item = gst::Message> + Unpin,
{
    let ShutdownMode::Eos { timeout } = mode else {
        return Ok(PipelineExit::Shutdown);
    };

    if !pipeline.send_event(gst::event::Eos::new()) {
        bail!("pipeline rejected the shutdown EOS event");
    }

    tokio::time::timeout(timeout, wait_for_eos(messages))
        .await
        .context("timed out while draining the pipeline after shutdown")??;

    Ok(PipelineExit::Shutdown)
}

async fn wait_for_eos<M>(messages: &mut M) -> Result<()>
where
    M: Stream<Item = gst::Message> + Unpin,
{
    loop {
        let message = messages
            .next()
            .await
            .context("pipeline message bus closed while draining EOS")?;

        if terminal_message(&message)?.is_some() {
            return Ok(());
        }
    }
}

fn terminal_message(message: &gst::Message) -> Result<Option<PipelineExit>> {
    match message.view() {
        gst::MessageView::Eos(..) => Ok(Some(PipelineExit::Eos)),
        gst::MessageView::Error(err) => {
            let source = err
                .src()
                .map_or_else(|| "unknown".to_owned(), |src| src.path_string().to_string());
            let debug = err.debug().unwrap_or_default();
            bail!("pipeline error from {source}: {} ({debug})", err.error());
        }
        _ => Ok(None),
    }
}

fn stop(pipeline: &gst::Pipeline, timeout: Duration) -> Result<()> {
    pipeline
        .set_state(gst::State::Null)
        .context("setting the pipeline to Null")?;

    let nanoseconds = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
    let (state_result, current, pending) =
        pipeline.state(gst::ClockTime::from_nseconds(nanoseconds));

    state_result.with_context(|| {
        format!("waiting for the pipeline to reach Null (current={current:?}, pending={pending:?})")
    })?;

    if current != gst::State::Null {
        bail!("pipeline did not reach Null (current={current:?}, pending={pending:?})");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::*;

    #[tokio::test]
    async fn runs_to_eos_and_returns_pipeline_to_null() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(Some(1));
        let observed = pipeline.clone();

        let exit = PipelineRunner::new(pipeline)
            .run(future::pending())
            .await
            .expect("pipeline runs to EOS");

        assert_eq!(exit, PipelineExit::Eos);
        assert_eq!(observed.current_state(), gst::State::Null);
    }

    #[tokio::test]
    async fn stops_immediately_when_shutdown_is_requested() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(None);
        let observed = pipeline.clone();

        let exit = PipelineRunner::new(pipeline)
            .run(future::ready(()))
            .await
            .expect("pipeline handles shutdown");

        assert_eq!(exit, PipelineExit::Shutdown);
        assert_eq!(observed.current_state(), gst::State::Null);
    }

    #[tokio::test]
    async fn drains_eos_when_shutdown_requests_finalization() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(None);
        let observed = pipeline.clone();

        let exit = PipelineRunner::new(pipeline)
            .shutdown_mode(ShutdownMode::Eos {
                timeout: Duration::from_secs(2),
            })
            .run(future::ready(()))
            .await
            .expect("pipeline drains EOS");

        assert_eq!(exit, PipelineExit::Shutdown);
        assert_eq!(observed.current_state(), gst::State::Null);
    }

    #[tokio::test]
    async fn reports_bus_errors_and_still_returns_pipeline_to_null() {
        crate::init().expect("GStreamer initializes");
        let pipeline = error_pipeline();
        let observed = pipeline.clone();

        let err = PipelineRunner::new(pipeline)
            .run(future::pending())
            .await
            .expect_err("identity produces a pipeline error");

        assert!(err.to_string().contains("pipeline error"));
        assert_eq!(observed.current_state(), gst::State::Null);
    }

    fn test_pipeline(num_buffers: Option<i32>) -> gst::Pipeline {
        let mut source = gst::ElementFactory::make("videotestsrc").property("is-live", true);
        if let Some(num_buffers) = num_buffers {
            source = source.property("num-buffers", num_buffers);
        }
        let source = source.build().expect("videotestsrc is available");
        let sink = gst::ElementFactory::make("fakesink")
            .property("sync", false)
            .build()
            .expect("fakesink is available");
        let pipeline = gst::Pipeline::with_name("runtime-test");

        pipeline
            .add_many([&source, &sink])
            .expect("elements are added");
        source.link(&sink).expect("elements link");

        pipeline
    }

    fn error_pipeline() -> gst::Pipeline {
        let source = gst::ElementFactory::make("videotestsrc")
            .property("num-buffers", 10i32)
            .build()
            .expect("videotestsrc is available");
        let fail = gst::ElementFactory::make("identity")
            .property("error-after", 1i32)
            .build()
            .expect("identity is available");
        let sink = gst::ElementFactory::make("fakesink")
            .build()
            .expect("fakesink is available");
        let pipeline = gst::Pipeline::with_name("runtime-error-test");

        pipeline
            .add_many([&source, &fail, &sink])
            .expect("elements are added");
        gst::Element::link_many([&source, &fail, &sink]).expect("elements link");

        pipeline
    }
}
