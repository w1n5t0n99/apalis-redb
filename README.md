# apalis-redb

Background task processing for rust using `apalis` and [`redb`](https://github.com/cberner/redb)

## Features

- **Reliable, embedded task queue** using a single `redb` file, with every state change committed in an ACID transaction.
- **No polling required**: idle workers are woken as soon as tasks are pushed, and scheduled or retried tasks run on time.
- **Customizable codecs** for serializing/deserializing task arguments such as `json`, `msgpack` and `bincode`.
- **Heartbeat and orphaned tasks re-enqueueing** for consistent task processing.
- **Integration with `apalis` workers and middleware** such as `retry`, `long_running` and `parallelize`

## Usage

Add the latest versions from crates.io:

```toml
apalis = { version = "1" }
apalis-redb = { version = "0.1" }
```

### Example

```rust,no_run
use apalis::prelude::*;
use apalis_redb::RedbStorage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
}

async fn send_email(task: Email) -> Result<(), BoxDynError> {
    Ok(())
}

#[tokio::main]
async fn main() {
    let conn = apalis_redb::connect("tasks.redb").await.expect("Could not connect");

    let mut storage = RedbStorage::new(conn);

    let task = Email {
        to: "test@example.com".to_owned()
    };

    storage.push(task).await.unwrap();

    let worker = WorkerBuilder::new("tasty-pear")
        .backend(storage)
        .build(send_email);

    worker.run().await;
}
```

### Factory Example

This shows an example of multiple backends using the same database.
redb only allows a database file to be opened once per process, so share one
connection between all task types.

```rust,no_run
use apalis::prelude::*;
use apalis_redb::factory::RedbStorageFactory;
use tokio::time::Duration;
use std::collections::HashMap;
use futures::stream;

#[tokio::main]
async fn main() {
    let conn = apalis_redb::connect("tasks.redb").await.unwrap();

    let mut store = RedbStorageFactory::new(conn).await.unwrap();

    let mut map_store = store.create().unwrap();

    let mut int_store = store.create().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T>(
        _: T,
        task_id: TaskId,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        wrk.stop().unwrap();
        Ok(())
    }

    let int_worker = WorkerBuilder::new("rango-tango-2")
        .backend(int_store)
        .build(send_reminder);
    let map_worker = WorkerBuilder::new("rango-tango-1")
        .backend(map_store)
        .build(send_reminder);
    tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
}
```

### Sequential Workflow example

```rust,no_run
use apalis::prelude::*;
use apalis_redb::RedbStorage;
use apalis_workflow::SteppedFlow;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Data {
   value: u32,
}

async fn task1(task: u32) -> Result<Data, BoxDynError> {
   Ok(Data { value: task + 1 })
}

async fn task2(task: Data) -> Result<Data, BoxDynError> {
  Ok(Data { value: task.value * 2 })
}

async fn task3(task: Data) -> Result<(), BoxDynError> {
  println!("Final value: {}", task.value);
  Ok(())
}

#[tokio::main]
async fn main() {
  let conn = apalis_redb::connect("tasks.redb").await.expect("Could not connect");

  let storage = RedbStorage::new(conn);

  let work_flow = SteppedFlow::new("sample-workflow")
      .and_then(task1)
      .and_then(task2)
      .and_then(task3);

  let worker = WorkerBuilder::new("tasty-carrot")
      .backend(storage)
      .build(work_flow);

  worker.run().await;
}
```

### More examples

The [`examples`](https://github.com/w1n5t0n99/apalis-redb/tree/main/examples) folder has runnable
versions of these and more, such as DAG workflows, named queues, idempotency keys and vacuuming:

```bash
cargo run --example basic
```

## Durability

By default every commit is flushed to disk before it returns. That is safe, but each commit pays for
an fsync. `Config::durability` lets you trade some crash safety for throughput, per queue:

```rust
use apalis_redb::{Config, Durability};

let config = Config::default().durability(Durability::None);
```

| Level | Behaviour |
|---|---|
| `Durability::Immediate` (default) | Each commit is on disk before it returns. |
| `Durability::None` | Commits are not written to disk until a later `Immediate` commit. |

The setting applies to queue operations: push, fetch, results, locks, releases, re-enqueueing
orphaned tasks and vacuum. Worker registration, heartbeats and shutdown always commit with
`Immediate`, and an `Immediate` commit also writes every earlier commit to disk. As a result:

- While a worker is running, a crash loses at most about one `heartbeat_interval` (30s by default)
  of queue changes.
- A worker that stops cleanly leaves everything on disk.
- A process that only pushes tasks, with no worker running in the same process, never makes an
  `Immediate` commit. If it crashes, it can lose everything it pushed.

As a rough guide, 1000 single pushes took about 507ms with `Immediate` and about 44ms with `None`
(one release-build run on WSL2). To cut the per-commit cost while staying fully durable, push in
batches with `push_bulk` or `push_stream`, which commit once per batch.

## Crash recovery

Every task a worker claims is recorded against that worker in the database, along with the worker's
last heartbeat. Nothing is lost when the application crashes: the tasks a worker held go back on the
queue once the worker is known to be dead.

- **Clean shutdown:** a stopping worker returns the tasks it holds to the queue before it exits.
- **Restart with the same worker name:** on startup the worker re-enqueues the tasks its previous run
  held, as long as that run's last heartbeat is more than one `heartbeat_interval` (30s by default) old.
  Within that window the old record still looks alive and `worker.run()` fails with
  `Error::WorkerAlreadyExists`. Restart crashed processes after a delay of at least `heartbeat_interval`
  (for example `RestartSec=30` under systemd), or retry `run()` on that error.
- **Workers with new names:** a running worker re-enqueues the tasks of any worker that has missed its
  heartbeats for `heartbeat_interval × missed_heartbeats` (5 minutes by default).

Recovery is at-least-once: a task that was running when the process died runs again, so handlers
should be safe to repeat. With `Durability::None` a crash can also lose recent commits; see
[Durability](#durability).

## Differences from `apalis-redis`

- `RedbStorage<Args>` has no connection type parameter; it always uses [`Connection`], a cloneable handle
  to a [`redb::Database`]. Build one with `apalis_redb::connect(path)` or `Connection::new(database)`.
- Each Redis key (`{queue}:active`, `{queue}:meta`, ...) is a redb table with the same name, and the
  `Config` key methods return those table names.
- Pub/sub events are replaced by in-process wakeups (`Config::emit_events`), since redb is embedded.
  Every storage sharing a `Connection` wakes the others' workers.
- Retries honour each task's `max_attempts` (default `25`) rather than a fixed limit.
- redb has no key expiry, so idempotency keys with a TTL are also indexed by expiry time in a
  `{queue}:idempotency_expiry` table, and each push deletes up to 100 expired keys. A sub-second
  `idempotency_ttl` rounds up to one second, where apalis-redis rounds it down.
- Commit durability is configurable with `Config::durability`; see [Durability](#durability).
- redb transactions are blocking, so they run on the tokio blocking pool (`tokio-comp`, default) or the
  `blocking` pool (`smol-comp`).

## Observability

You can track your tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).

## License

Licensed under the MIT license.
