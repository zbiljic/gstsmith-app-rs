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
    /// Allow up to the supplied timeout to send EOS and receive it on the bus.
    ///
    /// This is appropriate for muxers and file outputs that need to write a
    /// trailer before the pipeline is stopped. Subsequent teardown to `Null`
    /// is awaited separately and has no deadline.
    Eos { timeout: Duration },
}

/// Drives one `GStreamer` pipeline until EOS, failure, or application shutdown.
pub struct PipelineRunner {
    pipeline: gst::Pipeline,
    shutdown_mode: ShutdownMode,
}

impl PipelineRunner {
    /// Create a runner that stops immediately on application shutdown.
    #[must_use]
    pub fn new(pipeline: gst::Pipeline) -> Self {
        Self {
            pipeline,
            shutdown_mode: ShutdownMode::Immediate,
        }
    }

    /// Set how application shutdown should be handled.
    #[must_use]
    pub const fn shutdown_mode(mut self, mode: ShutdownMode) -> Self {
        self.shutdown_mode = mode;
        self
    }

    /// Run the pipeline alongside an application-provided shutdown future.
    ///
    /// The pipeline is always asked to reach `Null` before this method returns,
    /// including when startup or bus processing fails.
    /// Native lifecycle calls run on `GStreamer` worker threads. Teardown is
    /// awaited without a deadline because native state changes cannot be cancelled.
    ///
    /// Dropping this future after it has been polled schedules cleanup, which
    /// can finish after the future is dropped. If startup is still in progress,
    /// cleanup follows it. Use the shutdown future and await `run` for graceful
    /// EOS finalization and to observe cleanup errors.
    pub async fn run<S>(self, shutdown: S) -> Result<PipelineExit>
    where
        S: Future<Output = ()> + Send,
    {
        let Self {
            pipeline,
            shutdown_mode,
        } = self;

        let guard = PipelineCleanup {
            pipeline,
            armed: true,
        };
        // Keep cleanup ownership inside startup until the native call finishes,
        // so cancellation cannot stop the pipeline before startup sets it Playing.
        let (mut guard, startup) = guard
            .pipeline
            .clone()
            .call_async_future(move |pipeline| {
                let startup = pipeline
                    .set_state(gst::State::Playing)
                    .context("setting the pipeline to Playing");
                (guard, startup)
            })
            .await;

        let outcome = match startup {
            Ok(_) => drive(&guard.pipeline, shutdown, shutdown_mode).await,
            Err(err) => Err(err),
        };
        // Scheduling transfers cleanup ownership before the next cancellation point.
        let cleanup = guard.pipeline.call_async_future(stop);
        guard.armed = false;
        let cleanup = cleanup.await;

        match (outcome, cleanup) {
            (Ok(exit), Ok(())) => Ok(exit),
            (Err(err), Ok(())) | (Ok(_), Err(err)) => Err(err),
            (Err(err), Err(cleanup_err)) => Err(anyhow!(
                "{err:#}; additionally failed to stop the pipeline: {cleanup_err:#}"
            )),
        }
    }
}

struct PipelineCleanup {
    pipeline: gst::Pipeline,
    armed: bool,
}

impl Drop for PipelineCleanup {
    fn drop(&mut self) {
        if self.armed {
            self.pipeline.call_async(|pipeline| {
                // stop logs failures even when cancellation leaves no caller.
                let _cleanup = stop(pipeline);
            });
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

    tokio::time::timeout(timeout, async {
        if !pipeline
            .call_async_future(|pipeline| pipeline.send_event(gst::event::Eos::new()))
            .await
        {
            bail!("pipeline rejected the shutdown EOS event");
        }
        wait_for_eos(messages).await
    })
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

fn stop(pipeline: &gst::Pipeline) -> Result<()> {
    pipeline
        .set_state(gst::State::Null)
        .map(|_| ())
        .context("setting the pipeline to Null")
        .inspect_err(|err| {
            gst::error!(
                gst::CAT_RUST,
                obj = pipeline,
                "pipeline cleanup failed: {err:#}"
            );
        })
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{Mutex, mpsc},
    };

    use futures::channel::oneshot;

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

    #[tokio::test]
    async fn cancelling_a_running_pipeline_returns_it_to_null() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(None);
        let observed = pipeline.clone();
        let task = tokio::spawn(PipelineRunner::new(pipeline).run(future::pending()));

        wait_for_state(&observed, gst::State::Playing).await;
        task.abort();
        assert!(task.await.expect_err("runner is cancelled").is_cancelled());
        wait_for_state(&observed, gst::State::Null).await;
    }

    #[tokio::test]
    async fn cancellation_during_startup_cleans_up_after_startup_finishes() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(None);
        let observed = pipeline.clone();
        let (entered, started) = oneshot::channel();
        let (release, blocked) = mpsc::channel();
        let gate = Mutex::new(Some((entered, blocked)));
        pipeline
            .bus()
            .expect("pipeline has a bus")
            .set_sync_handler(move |_, message| {
                if matches!(message.view(), gst::MessageView::StateChanged(_)) {
                    let gate = gate.lock().expect("startup gate is not poisoned").take();
                    if let Some((entered, blocked)) = gate {
                        entered.send(()).expect("test waits for startup");
                        // Bounded so a regression cannot leave the native worker stuck.
                        let _released = blocked.recv_timeout(Duration::from_secs(5));
                    }
                }
                gst::BusSyncReply::Pass
            });
        let task = tokio::spawn(PipelineRunner::new(pipeline).run(future::pending()));

        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .expect("startup begins")
            .expect("startup signals the test");
        task.abort();
        assert!(task.await.expect_err("runner is cancelled").is_cancelled());
        release
            .send(())
            .expect("startup has not blocked the executor");
        wait_for_state(&observed, gst::State::Null).await;
    }

    #[tokio::test]
    async fn slow_teardown_keeps_the_executor_responsive_and_survives_cancellation() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(None);
        let observed = pipeline.clone();
        let source = pipeline
            .iterate_sources()
            .next()
            .expect("source exists")
            .expect("source is readable");
        let (entered, streaming) = oneshot::channel();
        let (release, blocked) = mpsc::channel();
        let gate = Mutex::new(Some((entered, blocked)));
        source
            .static_pad("src")
            .expect("source has a pad")
            .add_probe(gst::PadProbeType::BUFFER, move |_, _| {
                let (entered, blocked) = gate
                    .lock()
                    .expect("streaming gate is not poisoned")
                    .take()
                    .expect("probe runs once");
                entered.send(()).expect("shutdown waits for a buffer");
                let _released = blocked.recv_timeout(Duration::from_secs(5));
                gst::PadProbeReturn::Remove
            });
        let (requested, shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(PipelineRunner::new(pipeline).run(async {
            streaming.await.expect("source starts streaming");
            requested.send(()).expect("test waits for shutdown");
        }));

        tokio::time::timeout(Duration::from_secs(5), shutdown_requested)
            .await
            .expect("shutdown begins")
            .expect("shutdown signals the test");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !task.is_finished(),
            "teardown still waits for the streaming thread"
        );
        task.abort();
        assert!(task.await.expect_err("runner is cancelled").is_cancelled());
        release
            .send(())
            .expect("the executor runs before the probe's timeout");
        wait_for_state(&observed, gst::State::Null).await;
    }

    #[tokio::test]
    async fn eos_timeout_still_returns_pipeline_to_null() {
        crate::init().expect("GStreamer initializes");
        let pipeline = test_pipeline(None);
        let observed = pipeline.clone();
        let sink = pipeline
            .iterate_sinks()
            .next()
            .expect("sink exists")
            .expect("sink is readable");
        sink.static_pad("sink").expect("sink has a pad").add_probe(
            gst::PadProbeType::EVENT_DOWNSTREAM,
            |_, info| {
                if info
                    .event()
                    .is_some_and(|event| event.type_() == gst::EventType::Eos)
                {
                    gst::PadProbeReturn::Drop
                } else {
                    gst::PadProbeReturn::Ok
                }
            },
        );

        let err = PipelineRunner::new(pipeline)
            .shutdown_mode(ShutdownMode::Eos {
                timeout: Duration::from_millis(20),
            })
            .run(future::ready(()))
            .await
            .expect_err("EOS cannot reach the bus");

        assert!(err.to_string().contains("timed out while draining"));
        assert_eq!(observed.current_state(), gst::State::Null);
    }

    async fn wait_for_state(element: &impl IsA<gst::Element>, state: gst::State) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (_, current, pending) = element.state(gst::ClockTime::ZERO);
                if current == state && pending == gst::State::VoidPending {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("element reaches the expected state");
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
