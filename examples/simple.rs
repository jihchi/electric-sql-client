extern crate electric_sql_client;

use futures::pin_mut;
use futures::StreamExt;
use std::env::var;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = var("ELECTRIC_HOST").expect("ELECTRIC_HOST should set to the electric host");
    let table = var("ELECTRIC_TABLE").expect("ELECTRIC_TABLE should set to the electric table");
    let mut stream = electric_sql_client::ShapeStream::new(&host, &table, None);
    let logs = stream.run();

    pin_mut!(logs);

    while let Some(Ok(log)) = logs.next().await {
        println!(">>> {}", &log[..100]);
    }

    Ok(())
}
