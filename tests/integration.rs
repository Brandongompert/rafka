//! Integration tests: spin up a real Rafka server, connect over TCP, run requests.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use rafka::{
    broker::Broker,
    client::Client,
    protocol::{Request, Response},
    server,
};
use tokio::{io::BufReader, net::TcpStream, sync::Mutex};

/// Start a broker on a random OS-assigned port and return the address.
/// The server task runs in the background for the lifetime of the test.
async fn start_server(data_dir: PathBuf) -> SocketAddr {
    // Bind to port 0 → OS assigns a free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let broker = Broker::open(&data_dir).unwrap();
    let broker = Arc::new(Mutex::new(broker));

    // Hand the already-bound listener to the server.
    tokio::spawn(server::run_with_listener(listener, broker));

    addr
}

/// Open a client connection and return split (reader, writer).
async fn connect(
    addr: SocketAddr,
) -> (
    BufReader<tokio::net::tcp::OwnedReadHalf>,
    tokio::net::tcp::OwnedWriteHalf,
) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (r, w) = stream.into_split();
    (BufReader::new(r), w)
}

/// Send one request and receive one response.
async fn roundtrip(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    req: Request,
) -> Response {
    req.write_to(writer).await.unwrap();
    Response::read_from(reader).await.unwrap().unwrap()
}

fn tmp_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("rafka_integration_{}_{}", std::process::id(), name));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_topic() {
    let addr = start_server(tmp_dir("create_topic")).await;
    let (mut r, mut w) = connect(addr).await;

    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::CreateTopic {
            name: "events".into(),
            partition_count: 2,
        },
    )
    .await;

    assert_eq!(resp, Response::Ok);
}

#[tokio::test]
async fn duplicate_topic_returns_error() {
    let addr = start_server(tmp_dir("dup_topic")).await;
    let (mut r, mut w) = connect(addr).await;

    roundtrip(
        &mut r,
        &mut w,
        Request::CreateTopic {
            name: "t".into(),
            partition_count: 1,
        },
    )
    .await;

    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::CreateTopic {
            name: "t".into(),
            partition_count: 1,
        },
    )
    .await;
    assert!(matches!(resp, Response::Error { .. }));
}

#[tokio::test]
async fn produce_and_fetch_end_to_end() {
    let addr = start_server(tmp_dir("produce_fetch")).await;
    let (mut r, mut w) = connect(addr).await;

    // Create topic.
    roundtrip(
        &mut r,
        &mut w,
        Request::CreateTopic {
            name: "orders".into(),
            partition_count: 3,
        },
    )
    .await;

    // Produce.
    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::Produce {
            topic: "orders".into(),
            key: Some(b"customer-1".to_vec()),
            payload: b"order-payload".to_vec(),
        },
    )
    .await;

    let (pid, offset) = match resp {
        Response::Produced {
            partition_id,
            offset,
        } => (partition_id, offset),
        other => panic!("expected Produced, got {:?}", other),
    };

    // Fetch back.
    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::Fetch {
            topic: "orders".into(),
            partition_id: pid,
            offset,
            max_messages: 1,
        },
    )
    .await;

    match resp {
        Response::Fetched { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].offset, offset);
            assert_eq!(messages[0].payload, b"order-payload");
        }
        other => panic!("expected Fetched, got {:?}", other),
    }
}

#[tokio::test]
async fn fetch_unknown_topic_returns_error() {
    let addr = start_server(tmp_dir("fetch_unknown")).await;
    let (mut r, mut w) = connect(addr).await;

    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::Fetch {
            topic: "ghost".into(),
            partition_id: 0,
            offset: 0,
            max_messages: 10,
        },
    )
    .await;

    assert!(matches!(resp, Response::Error { .. }));
}

#[tokio::test]
async fn multiple_messages_ordered() {
    let addr = start_server(tmp_dir("ordered")).await;
    let (mut r, mut w) = connect(addr).await;

    roundtrip(
        &mut r,
        &mut w,
        Request::CreateTopic {
            name: "log".into(),
            partition_count: 1,
        },
    )
    .await;

    // All keyless → all go to partition 0 (single partition topic).
    for i in 0u8..5 {
        roundtrip(
            &mut r,
            &mut w,
            Request::Produce {
                topic: "log".into(),
                key: None,
                payload: vec![i],
            },
        )
        .await;
    }

    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::Fetch {
            topic: "log".into(),
            partition_id: 0,
            offset: 0,
            max_messages: 10,
        },
    )
    .await;

    match resp {
        Response::Fetched { messages } => {
            assert_eq!(messages.len(), 5);
            for (i, msg) in messages.iter().enumerate() {
                assert_eq!(msg.offset, i as u64);
                assert_eq!(msg.payload, vec![i as u8]);
            }
        }
        other => panic!("expected Fetched, got {:?}", other),
    }
}

#[tokio::test]
async fn two_clients_concurrent() {
    let addr = start_server(tmp_dir("concurrent")).await;

    // Pre-create the topic from one connection.
    {
        let (mut r, mut w) = connect(addr).await;
        roundtrip(
            &mut r,
            &mut w,
            Request::CreateTopic {
                name: "shared".into(),
                partition_count: 1,
            },
        )
        .await;
    }

    // Spin up two producer tasks simultaneously.
    let t1 = tokio::spawn(async move {
        let (mut r, mut w) = connect(addr).await;
        for _ in 0..10 {
            let resp = roundtrip(
                &mut r,
                &mut w,
                Request::Produce {
                    topic: "shared".into(),
                    key: None,
                    payload: b"from-t1".to_vec(),
                },
            )
            .await;
            assert!(matches!(resp, Response::Produced { .. }));
        }
    });

    let t2 = tokio::spawn(async move {
        let (mut r, mut w) = connect(addr).await;
        for _ in 0..10 {
            let resp = roundtrip(
                &mut r,
                &mut w,
                Request::Produce {
                    topic: "shared".into(),
                    key: None,
                    payload: b"from-t2".to_vec(),
                },
            )
            .await;
            assert!(matches!(resp, Response::Produced { .. }));
        }
    });

    t1.await.unwrap();
    t2.await.unwrap();

    // All 20 messages should be visible.
    let (mut r, mut w) = connect(addr).await;
    let resp = roundtrip(
        &mut r,
        &mut w,
        Request::Fetch {
            topic: "shared".into(),
            partition_id: 0,
            offset: 0,
            max_messages: 100,
        },
    )
    .await;

    match resp {
        Response::Fetched { messages } => assert_eq!(messages.len(), 20),
        other => panic!("expected Fetched, got {:?}", other),
    }
}

// ── Client API tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn client_create_and_produce_and_fetch() {
    let addr = start_server(tmp_dir("client_basic")).await;
    let mut client = Client::connect(addr).await.unwrap();

    client.create_topic("events", 3).await.unwrap();

    let (pid, offset) = client
        .produce("events", Some(b"user-99"), b"click-event")
        .await
        .unwrap();

    let messages = client.fetch("events", pid, offset, 10).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].offset, offset);
    assert_eq!(messages[0].payload, b"click-event");
}

#[tokio::test]
async fn client_same_key_same_partition() {
    let addr = start_server(tmp_dir("client_keyed")).await;
    let mut client = Client::connect(addr).await.unwrap();

    client.create_topic("orders", 4).await.unwrap();

    let mut seen_partitions = std::collections::HashSet::new();
    for _ in 0..8 {
        let (pid, _) = client
            .produce("orders", Some(b"cust-1"), b"order")
            .await
            .unwrap();
        seen_partitions.insert(pid);
    }

    assert_eq!(
        seen_partitions.len(),
        1,
        "keyed messages must always hit the same partition"
    );
}

#[tokio::test]
async fn client_fetch_pagination() {
    let addr = start_server(tmp_dir("client_pagination")).await;
    let mut client = Client::connect(addr).await.unwrap();

    client.create_topic("log", 1).await.unwrap();

    for i in 0u8..20 {
        client.produce("log", None, &[i]).await.unwrap();
    }

    // Page through in batches of 5.
    let mut all = Vec::new();
    let mut offset = 0u64;
    loop {
        let batch = client.fetch("log", 0, offset, 5).await.unwrap();
        if batch.is_empty() {
            break;
        }
        offset = batch.last().unwrap().offset + 1;
        all.extend(batch);
    }

    assert_eq!(all.len(), 20);
    for (i, msg) in all.iter().enumerate() {
        assert_eq!(msg.offset, i as u64);
        assert_eq!(msg.payload, vec![i as u8]);
    }
}

#[tokio::test]
async fn client_error_on_duplicate_topic() {
    let addr = start_server(tmp_dir("client_dup_topic")).await;
    let mut client = Client::connect(addr).await.unwrap();

    client.create_topic("t", 1).await.unwrap();
    let err = client.create_topic("t", 1).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn client_error_on_unknown_topic_produce() {
    let addr = start_server(tmp_dir("client_unknown_produce")).await;
    let mut client = Client::connect(addr).await.unwrap();

    let err = client.produce("ghost", None, b"data").await;
    assert!(err.is_err());
}
