# gstsmith-app-rs

`gstsmith-app` is a small runtime and starter for building GStreamer applications
in Rust. Applications keep their pipeline topology in Rust; `gstsmith-app` owns
the repetitive lifecycle around it:

- initialize GStreamer
- start a pipeline and consume its message bus asynchronously
- stop on EOS, a pipeline error, or an application-provided shutdown future
- optionally drain EOS so muxers and file outputs can finalize
- always take the pipeline to `Null` before returning

It is deliberately not a configurable pipeline graph, service framework, or
GStreamer plugin framework.

## Requirements

- Rust 1.97 or newer
- GStreamer 1.24 or newer, including development headers and the base plugins

On macOS with Homebrew:

```sh
brew install gstreamer
```

On Ubuntu or Debian:

```sh
sudo apt-get install \
  libgstreamer1.0-dev \
  libgstreamer-plugins-base1.0-dev \
  gstreamer1.0-plugins-base \
  gstreamer1.0-tools
```

## Quick start

Run the included headless application and stop it with Ctrl-C:

```sh
cargo run -p gstsmith-basic-app
```

The example builds this pipeline directly through the Rust API:

```text
videotestsrc is-live=true ! videoconvert ! fakesink
```

The essential application pattern is:

```rust
use gstsmith_app::{PipelineRunner, gst};
use gstsmith_app::gst::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    gstsmith_app::init()?;

    let source = gst::ElementFactory::make("videotestsrc")
        .property("num-buffers", 30i32)
        .build()?;
    let sink = gst::ElementFactory::make("fakesink").build()?;
    let pipeline = gst::Pipeline::new();

    pipeline.add_many([&source, &sink])?;
    source.link(&sink)?;

    let exit = PipelineRunner::new(pipeline)
        .run(std::future::pending())
        .await?;

    println!("pipeline stopped: {exit:?}");
    Ok(())
}
```

`gstsmith-app` re-exports its GStreamer dependency as `gstsmith_app::gst` so an
application does not accidentally mix incompatible GStreamer crate versions.

## Composition helpers

Assembling a pipeline by hand repeats a few small pieces: creating a named
element with a useful error, exposing a bin's ghost pads, and deferring a link
until a dynamic source pad appears. `gstsmith-app` provides these as optional,
pure-construction helpers. They never add a bin to a pipeline and never carry
application policy.

Naming stays with the bin, not the `PipelineBin` trait. `GStreamer` requires
element names to be unique within a parent bin, so a bin that may be built more
than once takes a name in its constructor and derives its children's names from
it (or leaves them unnamed and lets `GStreamer` assign unique ones). `BinBase` is
an optional helper for that bookkeeping: embed it as a field, and it holds the
bin's name and creates the bin and its named children.

```rust
use gstsmith_app::{BinBase, PipelineBin, ghost_sink, ghost_src, gst};
use gstsmith_app::gst::prelude::*;

struct ConvertScale {
    base: BinBase,
}

impl PipelineBin for ConvertScale {
    fn build(&self) -> anyhow::Result<gst::Bin> {
        let bin = self.base.bin();
        let convert = self.base.make("videoconvert", "convert")?;
        let scale = self.base.make("videoscale", "scale")?;

        bin.add_many([&convert, &scale])?;
        gst::Element::link_many([&convert, &scale])?;

        // A transform bin exposes ghost pads named `sink` and `src`, so the
        // caller links it into a pipeline exactly like a plain element.
        ghost_sink(&bin, &convert)?;
        ghost_src(&bin, &scale)?;

        Ok(bin)
    }
}
```

The free `make(factory, name)` helper is still available for elements built
outside a bin (or when you want to name a child yourself).

For elements whose source pad appears only after the pipeline starts
(`decodebin`, demuxers, `rtspsrc`), `connect_dynamic` links the first matching
pad to a sink pad. It rejects elements whose factory cannot emit a dynamic
source pad and posts link failures to the pipeline bus:

```rust,ignore
gstsmith_app::connect_dynamic(&source, sink_pad, Some(caps), "rtspsrc -> depay")?;
```

The helper does not decide how long an application should wait for a matching
pad. Applications that need a bounded startup failure should keep that timeout
and shutdown policy locally.

Run the example that composes a pipeline from a `PipelineBin` and stop it with
Ctrl-C:

```sh
cargo run -p gstsmith-compose-app
```

These helpers are optional. An application can keep building elements and bins
directly with native GStreamer APIs when that is already clear.

## Shutdown behavior

The default shutdown mode immediately takes the pipeline to `Null`. This is the
right default for live pipelines whose outputs have nothing to finalize.

File writers and muxers usually need EOS to write their trailer:

```rust
use std::time::Duration;
use gstsmith_app::{PipelineRunner, ShutdownMode};

let runner = PipelineRunner::new(pipeline).shutdown_mode(ShutdownMode::Eos {
    timeout: Duration::from_secs(4),
});
```

After either shutdown path, the runner waits up to five seconds for the pipeline
to reach `Null`. The timeout can be changed with `state_change_timeout`.

## Using a service runtime

The core crate accepts any `Future<Output = ()>` and does not depend on a process
or service framework. The basic application uses Ctrl-C, while a service runtime
can pass its own cancellation future. CLI, configuration, telemetry, and process
policy remain in the application. Finite media tools can instead exit naturally
when the pipeline reaches EOS.

## Development

With mise:

```sh
mise install
mise run fmt:check
mise run lint
mise run check
mise run test
mise run pre-commit
```

Or use the corresponding Cargo commands directly:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --all-targets
cargo test --workspace --all-targets
```

## License

Apache-2.0
