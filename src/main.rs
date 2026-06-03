use std::{path::PathBuf, sync::Arc};

use rafka::{broker::Broker, server};
use tokio::sync::Mutex;

/// Usage: rafka [address] [data-dir]
///
/// Defaults: address = 127.0.0.1:9092, data-dir = ./data
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:9092".to_string());
    let data_dir = PathBuf::from(args.next().unwrap_or_else(|| "data".to_string()));

    let broker = Broker::open(&data_dir)?;
    let broker = Arc::new(Mutex::new(broker));

    server::run(&addr, broker).await
}
