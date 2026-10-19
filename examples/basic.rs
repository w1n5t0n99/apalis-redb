use std::time::Duration;

use apalis::prelude::*;
use apalis_redb::{Config, RedbStorage};

#[tokio::main]
async fn main() {
    // Use a throwaway database so the example can be rerun from a clean state
    let dir = tempfile::tempdir().unwrap();
    let conn = apalis_redb::connect(dir.path().join("tasks.redb"))
        .await
        .unwrap();
    let config = Config::default()
        .queue("redb_basic_worker")
        .heartbeat_interval(Duration::from_secs(1))
        .lock_tasks(false)
        .batch_size(100);
    let mut backend = RedbStorage::new(conn).with_config(config);
    backend.push(42).await.unwrap();
    async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        wrk.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(task);
    worker.run().await.unwrap();
}
