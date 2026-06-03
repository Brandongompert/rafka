# Rafka

A Kafka-inspired message queue written in Rust, with a Node.js native addon via napi-rs.

## What it is

Rafka implements the core ideas behind Apache Kafka from scratch:

- **Append-only segmented log** — durable, ordered message storage on disk
- **Topics and partitions** — named streams split for parallel processing
- **Offset-based consumption** — consumers own their position; no server-side state
- **Key-based routing** — hash a key to always land on the same partition (per-key ordering)
- **High-water mark** — committed offset boundary; consumers only see durable messages

## Project structure

```
rafka/
├── src/                    # Rust library + broker binary
│   ├── main.rs             # Broker entrypoint (cargo run)
│   ├── log/
│   │   ├── mod.rs          # Log — manages a collection of segments
│   │   ├── segment.rs      # Segment — append-only file on disk
│   │   └── index.rs        # Index — O(1) offset → byte position lookup
│   ├── partition.rs        # Partition — owns a Log, tracks high-water mark
│   ├── topic.rs            # Topic — owns partitions, routes messages by key
│   ├── broker.rs           # Broker — owns all topics, dispatches requests
│   ├── protocol.rs         # Binary wire protocol (framing, encode/decode)
│   ├── server.rs           # Tokio TCP server — one task per connection
│   └── client.rs           # Rust async client
├── tests/
│   └── integration.rs      # End-to-end tests over a real TCP socket
└── rafka-node/             # Node.js native addon (napi-rs)
    ├── src/lib.rs          # #[napi] bindings
    ├── index.js            # Platform binary loader
    ├── index.d.ts          # TypeScript declarations
    └── package.json
```

## Architecture

### Storage layer

Each partition stores messages in a **log** — a directory of segment files:

```
data/orders/0/
├── 00000000000000000000.log    # message frames
├── 00000000000000000000.index  # offset → byte position map
├── 00000000000001048576.log    # new segment after rollover
└── 00000000000001048576.index
```

Every message on disk is a fixed-format frame:

```
[length: u32][offset: u64][payload: bytes...]
```

The paired `.index` file maps each offset to its byte position in the `.log` file, enabling O(1) seeks. When a segment reaches its size limit a new one is created; the zero-padded filename means alphabetical order equals offset order.

### Wire protocol

Client–broker communication uses a simple binary protocol over TCP:

```
[frame_length: u32][type_tag: u8][fields...]
```

Three request types: `CreateTopic`, `Produce`, `Fetch`. All integers are big-endian.

### Concurrency model

The broker is wrapped in `Arc<Mutex<Broker>>` and shared across Tokio tasks. The mutex is held only for request dispatch — reading and writing the TCP stream happens outside the lock, so one slow client cannot block others.

## Running the broker

```bash
cargo run                          # listens on 127.0.0.1:9092, data in ./data
cargo run -- 0.0.0.0:9092 /var/rafka-data
```

## Rust client

```rust
use rafka::client::Client;

let mut client = Client::connect("127.0.0.1:9092").await?;

client.create_topic("events", 3).await?;

let (partition_id, offset) = client
    .produce("events", Some(b"user-42"), b"click")
    .await?;

let messages = client.fetch("events", partition_id, offset, 100).await?;

// paginate
let mut cursor = 0u64;
loop {
    let batch = client.fetch("events", 0, cursor, 100).await?;
    if batch.is_empty() { break; }
    cursor = batch.last().unwrap().offset + 1;
}
```

## Node.js client

Build the native addon first:

```bash
cd rafka-node
yarn install
yarn build          # release build
# or
yarn build:debug    # faster, for development
```

Then use it:

```js
const { RafkaClient } = require('./rafka-node');

const client = await RafkaClient.connect('127.0.0.1:9092');

await client.createTopic('events', 3);

const { partitionId, offset } = await client.produce(
  'events',
  Buffer.from('user-42'), // routing key — null for round-robin
  Buffer.from(JSON.stringify({ type: 'click', page: '/home' })),
);

const messages = await client.fetch('events', partitionId, offset, 100);
for (const msg of messages) {
  console.log(msg.offset, JSON.parse(msg.payload.toString()));
}
```

Full TypeScript types are included (`index.d.ts`).

## Testing

```bash
cargo test          # all unit + integration tests (49 total)
cargo test -p rafka # library tests only
```

