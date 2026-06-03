use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::BufReader,
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{broker::Broker, protocol::Request};

/// Start listening for client connections and serve them indefinitely.
///
/// Each accepted connection gets its own tokio task. The `Broker` is shared
/// across all tasks behind an `Arc<Mutex<Broker>>`.
///
/// The mutex is held only for the duration of a single request dispatch —
/// reading/writing the TCP stream happens outside the lock.
pub async fn run(addr: &str, broker: Arc<Mutex<Broker>>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    run_with_listener(listener, broker).await
}

/// Accept connections from an already-bound listener.
///
/// Useful in tests where the OS assigns the port (`bind("0.0.0.0:0")`).
pub async fn run_with_listener(
    listener: TcpListener,
    broker: Arc<Mutex<Broker>>,
) -> std::io::Result<()> {
    tracing_log(&format!("rafka broker listening on {}", listener.local_addr()?));

    loop {
        let (socket, peer) = listener.accept().await?;
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer, broker).await {
                tracing_log(&format!("connection error [{peer}]: {e}"));
            }
        });
    }
}

/// Handle a single client connection: read requests, dispatch, write responses.
async fn handle_connection(
    socket: TcpStream,
    peer: SocketAddr,
    broker: Arc<Mutex<Broker>>,
) -> std::io::Result<()> {
    tracing_log(&format!("client connected: {peer}"));

    // Split the socket so we can hold a write reference while reading.
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        // Read one request. Returns None on clean client disconnect.
        let Some(request) = Request::read_from(&mut reader).await? else {
            tracing_log(&format!("client disconnected: {peer}"));
            break;
        };

        // Acquire the broker lock, dispatch, release immediately.
        let response = {
            let mut broker = broker.lock().await;
            broker.handle(request)
        };

        response.write_to(&mut write_half).await?;
    }

    Ok(())
}

/// Minimal log output without pulling in a tracing dependency yet.
fn tracing_log(msg: &str) {
    eprintln!("[rafka] {msg}");
}
