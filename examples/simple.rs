extern crate electric_sql_client;

use std::env::var;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = var("ELECTRIC_HOST").expect("ELECTRIC_HOST should set to the electric host");
    let table = var("ELECTRIC_TABLE").expect("ELECTRIC_TABLE should set to the electric table");
    let mut stream = electric_sql_client::ShapeStream::new(&host, &table, None);

    while let Ok(messages) = stream.fetch().await {
        println!("number of messages: {}", messages.len());
    }

    Ok(())
}
