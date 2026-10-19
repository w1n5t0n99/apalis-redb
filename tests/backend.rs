use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use apalis_codec::msgpack::MsgPackCodec;
use apalis_core::{
    backend::{
        Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers, Metrics, TaskSink, Vacuum,
        WaitForCompletion, ext::BackendExt, factory::BackendFactory,
    },
    error::BoxDynError,
    task::{builder::TaskBuilder, context::TaskContext, status::Status, task_id::TaskId},
    worker::{
        builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        ext::parallelize::ParallelizeExt,
    },
};
use apalis_redb::{Config, Connection, RedbStorage, factory::RedbStorageFactory};
use apalis_workflow::{SteppedFlow, WorkflowSink};
use futures::StreamExt;

const ITEMS: u32 = 10;

async fn setup() -> (tempfile::TempDir, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let conn = apalis_redb::connect(dir.path().join("tasks.redb"))
        .await
        .unwrap();
    (dir, conn)
}

/// Runs the worker future, failing the test if it does not finish in time
async fn within<F: Future>(secs: u64, fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(secs), fut)
        .await
        .expect("worker did not finish in time")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_worker() {
    let (_dir, conn) = setup().await;
    let config = Config::default().queue("basic:queue").batch_size(100);
    let mut backend = RedbStorage::new(conn).with_config(config);
    for i in 0..ITEMS {
        backend.push(i).await.unwrap();
    }

    async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
        println!("{task:?}, {ctx:?}");
        if task == ITEMS - 1 {
            wrk.stop().unwrap();
        }
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango-basic")
        .backend(backend)
        .on_event(|ctx, ev| println!("CTX {:?}, On Event = {:?}", ctx.name(), ev))
        .build(task);
    within(10, worker.run()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_worker_msgpack() {
    let (_dir, conn) = setup().await;
    let config = Config::default().queue("msgpack:queue").batch_size(100);
    let mut backend = RedbStorage::new(conn)
        .with_config(config)
        .with_codec(MsgPackCodec);

    for i in 0..ITEMS {
        let req = TaskBuilder::new(i).build();
        backend.push_task(req).await.unwrap();
    }

    async fn task(task: u32, wrk: WorkerContext) -> Result<String, BoxDynError> {
        if task == ITEMS - 1 {
            wrk.stop().unwrap();
            return Err("Worker stopped!")?;
        }
        Ok("Worker".to_owned())
    }

    let worker = WorkerBuilder::new("rango-tango-msgpack")
        .backend(backend)
        .parallelize(tokio::spawn)
        .build(task);
    within(10, worker.run()).await.unwrap();
}

#[tokio::test]
async fn shared_workers() {
    let (_dir, conn) = setup().await;
    let mut store = RedbStorageFactory::new(conn).await.unwrap();

    let mut string_store = store.create().unwrap();
    let mut int_store = store.create().unwrap();

    for i in 0..ITEMS {
        string_store.push(format!("ITEM: {i}")).await.unwrap();
        int_store.push(i).await.unwrap();
    }

    async fn task(job: u32, ctx: WorkerContext) -> Result<usize, BoxDynError> {
        if job == ITEMS - 1 {
            ctx.stop().unwrap();
        }
        Ok(job as usize)
    }

    let int_worker = WorkerBuilder::new("rango-tango-int")
        .backend(int_store)
        .build(task)
        .run();

    let string_worker = WorkerBuilder::new("rango-tango-string")
        .backend(string_store)
        .build(|req: String, ctx: WorkerContext| async move {
            if req.ends_with(&(ITEMS - 1).to_string()) {
                ctx.stop().unwrap();
            }
        })
        .run();
    within(10, futures::future::try_join(int_worker, string_worker))
        .await
        .unwrap();
}

#[tokio::test]
async fn workflow() {
    async fn task1(job: u32) -> Result<Vec<u32>, BoxDynError> {
        Ok((job..2).collect())
    }

    async fn task2(_: Vec<u32>) -> Result<usize, BoxDynError> {
        Ok(42)
    }

    async fn task3(_: usize, wrk: WorkerContext, _: TaskContext) -> Result<(), io::Error> {
        wrk.stop().unwrap();
        Ok(())
    }

    let work_flow = SteppedFlow::new("sample-workflow")
        .and_then(task1)
        .delay_for(Duration::from_millis(1000))
        .and_then(task2)
        .and_then(task3);

    let (_dir, conn) = setup().await;
    let config = Config::default().queue("workflow:sample");
    let mut backend = RedbStorage::new(conn).with_config(config);

    backend.push_start(0u32).await.unwrap();

    let worker = WorkerBuilder::new("rango-tango-workflow")
        .backend(backend)
        .build(work_flow);
    within(15, worker.run()).await.unwrap();
}

/// Tasks pushed after the worker went idle are picked up without waiting
/// for a heartbeat or poll interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakes_on_push() {
    let (_dir, conn) = setup().await;
    let config = Config::default()
        .queue("wake:queue")
        .heartbeat_interval(Duration::from_secs(60));
    let backend = RedbStorage::<u32>::new(conn).with_config(config);
    let mut producer = backend.clone();

    let worker = WorkerBuilder::new("wake-worker").backend(backend).build(
        |task: u32, wrk: WorkerContext| async move {
            if task == 2 {
                wrk.stop().unwrap();
            }
        },
    );
    let handle = tokio::spawn(worker.run());

    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        producer.push(i).await.unwrap();
    }
    within(5, handle).await.unwrap().unwrap();
}

/// A scheduled task runs once due, without waiting for a heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_task_runs_when_due() {
    let (_dir, conn) = setup().await;
    let config = Config::default()
        .queue("scheduled:queue")
        .heartbeat_interval(Duration::from_secs(60));
    let mut backend = RedbStorage::<u32>::new(conn).with_config(config);

    let start = Instant::now();
    backend
        .push_task(TaskBuilder::new(7).run_in_seconds(2).build())
        .await
        .unwrap();

    let worker = WorkerBuilder::new("scheduled-worker")
        .backend(backend)
        .build(|_: u32, wrk: WorkerContext| async move {
            wrk.stop().unwrap();
        });
    within(10, worker.run()).await.unwrap();
    assert!(start.elapsed() >= Duration::from_secs(1), "ran too early");
}

/// Failed tasks are retried until they succeed and the final result is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_and_results() {
    let (_dir, conn) = setup().await;
    let config = Config::default().queue("retry:queue");
    let mut backend = RedbStorage::<u32>::new(conn).with_config(config);
    let task = TaskBuilder::new(5).max_attempts(5).build();
    backend.push_task(task).await.unwrap();
    let task_id = backend
        .list_tasks(&Filter {
            status: None,
            page: 1,
            page_size: None,
        })
        .await
        .unwrap()[0]
        .task_id()
        .cloned()
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let mut checker = backend.clone();
    let worker = WorkerBuilder::new("retry-worker")
        .backend(backend)
        .data(c)
        .build(
            |task: u32, calls: apalis_core::task::data::Data<Arc<AtomicUsize>>| async move {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err::<u32, BoxDynError>("not yet".into());
                }
                Ok(task * 2)
            },
        );
    let handle = tokio::spawn(worker.run());

    let result = within(15, checker.wait_for_single(task_id.clone()).next())
        .await
        .unwrap()
        .unwrap();
    let result: apalis_core::backend::TaskResult<u32> = result;
    assert_eq!(result.status, Status::Done);
    assert_eq!(result.result, Ok(10));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    handle.abort();
}

#[tokio::test]
async fn idempotency_key_prevents_duplicates() {
    let (_dir, conn) = setup().await;
    let config = Config::default().queue("idem:queue");
    let mut backend = RedbStorage::<u32>::new(conn).with_config(config);
    for _ in 0..3 {
        backend
            .push_task(TaskBuilder::new(1).idempotency_key("only-once").build())
            .await
            .unwrap();
    }
    let tasks = backend
        .list_tasks(&Filter {
            status: None,
            page: 1,
            page_size: Some(10),
        })
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
}

/// Tasks survive closing and reopening the database.
#[tokio::test]
async fn tasks_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.redb");
    {
        let conn = apalis_redb::connect(&path).await.unwrap();
        let mut backend =
            RedbStorage::<u32>::new(conn).with_config(Config::default().queue("persist"));
        backend.push(1).await.unwrap();
        backend.push(2).await.unwrap();
    }
    let conn = apalis_redb::connect(&path).await.unwrap();
    let backend = RedbStorage::<u32>::new(conn).with_config(Config::default().queue("persist"));
    let seen = Arc::new(AtomicUsize::new(0));
    let s = seen.clone();
    let worker = WorkerBuilder::new("persist-worker").backend(backend).build(
        move |task: u32, wrk: WorkerContext| {
            let s = s.clone();
            async move {
                s.fetch_add(task as usize, Ordering::SeqCst);
                if task == 2 {
                    wrk.stop().unwrap();
                }
            }
        },
    );
    within(10, worker.run()).await.unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 3);
}

/// Tasks held by a worker that stopped heartbeating are handed to another worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_tasks_are_reenqueued() {
    let (_dir, conn) = setup().await;
    let config = Config::default()
        .queue("orphan:queue")
        .heartbeat_interval(Duration::from_secs(1))
        .missed_heartbeats(1);
    let mut backend = RedbStorage::<u32>::new(conn).with_config(config.clone());
    backend.push(1).await.unwrap();

    // A worker that grabs the task and then hangs forever
    let hung = WorkerBuilder::new("hung-worker")
        .backend(backend.clone())
        .build(|_: u32| futures::future::pending::<()>());
    let hung = tokio::spawn(hung.run());
    tokio::time::sleep(Duration::from_millis(500)).await;
    hung.abort();

    let rescuer = WorkerBuilder::new("rescue-worker").backend(backend).build(
        |_: u32, wrk: WorkerContext| async move {
            wrk.stop().unwrap();
        },
    );
    within(10, rescuer.run()).await.unwrap();
}

/// Simulates the whole application crashing: a worker claims a task and is
/// dropped mid-task without shutting down, then the database is closed.
async fn crash_while_running(path: &std::path::Path, config: &Config, name: &str) {
    let conn = apalis_redb::connect(path).await.unwrap();
    let mut backend = RedbStorage::<u32>::new(conn).with_config(config.clone());
    backend.push(1).await.unwrap();

    let started = Arc::new(tokio::sync::Notify::new());
    let s = started.clone();
    let worker = WorkerBuilder::new(name)
        .backend(backend)
        .build(move |_: u32| {
            s.notify_one();
            futures::future::pending::<()>()
        });
    let handle = tokio::spawn(worker.run());
    within(10, started.notified()).await;
    handle.abort();
    let _ = handle.await;
}

/// Reopens the database once the crashed run has released it
async fn reopen(path: &std::path::Path) -> Connection {
    within(10, async {
        loop {
            match apalis_redb::connect(path).await {
                Ok(conn) => return conn,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
}

/// After a crash, a worker restarted under the same name re-runs the task it
/// held, once its last heartbeat is older than `heartbeat_interval`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_crash_recovers_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.redb");
    // Long orphan threshold, so only the same-name check can recover the task
    let config = Config::default()
        .queue("crash:recover")
        .heartbeat_interval(Duration::from_secs(1))
        .missed_heartbeats(1000);
    crash_while_running(&path, &config, "crash-worker").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let conn = reopen(&path).await;
    let backend = RedbStorage::<u32>::new(conn).with_config(config);
    let worker = WorkerBuilder::new("crash-worker").backend(backend).build(
        |task: u32, wrk: WorkerContext| async move {
            assert_eq!(task, 1);
            wrk.stop().unwrap();
        },
    );
    within(10, worker.run()).await.unwrap();
}

/// Restarting under the same name before `heartbeat_interval` has passed fails,
/// because the crashed worker's record still looks alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_restart_after_crash_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.redb");
    let config = Config::default().queue("crash:immediate");
    crash_while_running(&path, &config, "crash-worker").await;

    let conn = reopen(&path).await;
    let backend = RedbStorage::<u32>::new(conn).with_config(config);
    let worker = WorkerBuilder::new("crash-worker")
        .backend(backend)
        .build(|_: u32| async {});
    let err = within(10, worker.run()).await.unwrap_err();
    assert!(err.to_string().contains("WorkerAlreadyExists"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspection_apis() {
    let (_dir, conn) = setup().await;
    let config = Config::default().queue("inspect:queue");
    let mut backend = RedbStorage::<u32>::new(conn).with_config(config);
    for i in 0..ITEMS {
        backend.push(i).await.unwrap();
    }
    let pending = backend
        .list_tasks(&Filter {
            status: Some(Status::Pending),
            page: 1,
            page_size: Some(4),
        })
        .await
        .unwrap();
    assert_eq!(pending.len(), 4);

    let inspector = backend.clone();
    let worker = WorkerBuilder::new("inspect-worker").backend(backend).build(
        |task: u32, wrk: WorkerContext| async move {
            if task == ITEMS - 1 {
                wrk.stop().unwrap();
            }
        },
    );
    within(10, worker.run()).await.unwrap();

    let done = inspector
        .list_all_tasks(&Filter {
            status: Some(Status::Done),
            page: 1,
            page_size: Some(100),
        })
        .await
        .unwrap();
    assert_eq!(done.len(), ITEMS as usize);

    let stats = inspector.fetch_by_queue().await.unwrap();
    let done_jobs = stats.iter().find(|s| s.title == "DONE_JOBS").unwrap();
    assert_eq!(done_jobs.value, ITEMS.to_string());
    assert!(!inspector.global().await.unwrap().is_empty());

    let queues = inspector.list_queues().await.unwrap();
    assert!(queues.iter().any(|q| q.name == "inspect:queue"));

    // The worker unregisters itself when it stops
    assert!(inspector.list_workers().await.unwrap().is_empty());
    assert!(inspector.list_all_workers().await.unwrap().is_empty());

    let mut inspector = inspector;
    assert_eq!(inspector.vacuum().await.unwrap(), ITEMS as usize);
    let stats = inspector.fetch_by_queue().await.unwrap();
    let done_jobs = stats.iter().find(|s| s.title == "DONE_JOBS").unwrap();
    assert_eq!(done_jobs.value, "0");
}

#[tokio::test]
async fn fetch_by_id() {
    use apalis_core::backend::FetchById;
    let (_dir, conn) = setup().await;
    let mut backend =
        RedbStorage::<u32>::new(conn).with_config(Config::default().queue("byid:queue"));
    let id = TaskId::from_ulid(ulid::Ulid::generate());
    backend
        .push_task(TaskBuilder::new(9).task_id(id.clone()).build())
        .await
        .unwrap();
    let task = backend.fetch_by_id(&id).await.unwrap().unwrap();
    assert_eq!(task.task_id(), Some(&id));
    assert_eq!(serde_json::from_slice::<u32>(&task.args).unwrap(), 9);
}

/// Non-durable queue operations are flushed by the worker's durable commits,
/// so a cleanly stopped worker leaves everything on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_durable_commits_flushed_on_shutdown() {
    use apalis_redb::Durability;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.redb");
    let config = Config::default()
        .queue("nondurable:queue")
        .durability(Durability::None);
    {
        let conn = apalis_redb::connect(&path).await.unwrap();
        let mut backend = RedbStorage::<u32>::new(conn).with_config(config.clone());
        for i in 0..ITEMS {
            backend.push(i).await.unwrap();
        }
        let worker = WorkerBuilder::new("nondurable-worker")
            .backend(backend)
            .build(|task: u32, wrk: WorkerContext| async move {
                if task == ITEMS - 1 {
                    wrk.stop().unwrap();
                }
            });
        within(10, worker.run()).await.unwrap();
    }
    let conn = apalis_redb::connect(&path).await.unwrap();
    let backend = RedbStorage::<u32>::new(conn).with_config(config);
    let done = backend
        .list_tasks(&Filter {
            status: Some(Status::Done),
            page: 1,
            page_size: Some(100),
        })
        .await
        .unwrap();
    assert_eq!(done.len(), ITEMS as usize);
}
