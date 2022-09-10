use datafusion::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  let now = Instant::now();
  let fusion = SessionContext::new();
  let df = fusion.read_csv("data/*.csv", CsvReadOptions::new()).await?;

  let df = df.aggregate(vec![col("member_casual")], vec![count(col("ride_id"))])?;

  df.show_limit(100).await?;
  let elapsed = now.elapsed();
  println!("Elapsed: {:.2?}", elapsed);
  Ok(())
}
