use std::time::Duration;

use apalis::prelude::*;
use apalis_redb::RedbStorage;

#[tokio::main]
async fn main() {
    let dedupe_key = "7902f170-3d80-47ff-a83c-ebb523247d3c";
    let dir = tempfile::tempdir().unwrap();
    let conn = apalis_redb::connect(dir.path().join("tasks.redb"))
        .await
        .unwrap();
    let mut backend = RedbStorage::new(conn);

    let task_1 = TaskBuilder::new(42).idempotency_key(dedupe_key).build();

    let task_2 = TaskBuilder::new(43).idempotency_key(dedupe_key).build();

    backend.push_task(task_1).await.unwrap();
    // Dropped, since a task with the same idempotency key is already queued
    backend.push_task(task_2).await.unwrap();

    async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        assert_eq!(task, 42, "This should be the only task");
        // Give a duplicate time to show up before stopping
        tokio::time::sleep(Duration::from_secs(2)).await;
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
