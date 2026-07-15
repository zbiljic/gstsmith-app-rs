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
