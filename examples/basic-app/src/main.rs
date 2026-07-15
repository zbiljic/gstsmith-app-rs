mod pipeline;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let exit = pipeline::run().await?;
    println!("pipeline stopped: {exit:?}");
    Ok(())
}
