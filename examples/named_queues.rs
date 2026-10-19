use std::time::Duration;

use apalis::prelude::*;
use apalis_redb::{Config, RedbStorage};
use serde::{Deserialize, Serialize};

const NEWSLETTER_SIZE: usize = 20;

#[derive(Debug, Serialize, Deserialize)]
struct Email {
    id: usize,
    to: String,
    subject: String,
}

async fn send_urgent(email: Email, wrk: WorkerContext) -> Result<(), BoxDynError> {
    println!("[urgent] sent {:?} to {}", email.subject, email.to);
    wrk.stop()?;
    Ok(())
}

async fn send_bulk(email: Email, wrk: WorkerContext) -> Result<(), BoxDynError> {
    // Pretend the mail provider is slow for bulk sends
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("[bulk]   sent {:?} to {}", email.subject, email.to);
    if email.id == NEWSLETTER_SIZE - 1 {
        wrk.stop()?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    // One file, one connection, two independent queues for the same task type
    let conn = apalis_redb::connect(dir.path().join("tasks.redb"))
        .await
        .unwrap();

    let mut urgent = RedbStorage::<Email>::new(conn.clone())
        .with_config(Config::default().queue("emails:urgent"));

    let mut bulk = RedbStorage::<Email>::new(conn)
        .with_config(Config::default().queue("emails:bulk").batch_size(100));

    // The newsletter goes to the bulk queue...
    for id in 0..NEWSLETTER_SIZE {
        bulk.push(Email {
            id,
            to: format!("user{id}@example.com"),
            subject: "Newsletter".into(),
        })
        .await
        .unwrap();
    }
    // ...so the password reset doesn't wait behind it
    urgent
        .push(Email {
            id: 0,
            to: "alice@example.com".into(),
            subject: "Reset your password".into(),
        })
        .await
        .unwrap();

    let urgent_worker = WorkerBuilder::new("urgent-mailer")
        .backend(urgent)
        .concurrency(4)
        .build(send_urgent);

    let bulk_worker = WorkerBuilder::new("bulk-mailer")
        .backend(bulk)
        .concurrency(1) // keep under the mail provider's rate limit
        .build(send_bulk);

    tokio::try_join!(urgent_worker.run(), bulk_worker.run()).unwrap();
}
