use std::{fmt::Debug, time::Duration};

use apalis::prelude::*;
use apalis_redb::{Config, RedbStorage};
use apalis_workflow::SteppedFlow;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WelcomeEmail {
    user_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CampaignEmail {
    campaign_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CompleteCampaign {
    completion_id: usize,
}

async fn welcome(req: WelcomeEmail, _ctx: Data<()>) -> Result<CampaignEmail, BoxDynError> {
    Ok::<_, _>(CampaignEmail {
        campaign_id: req.user_id + 1,
    })
}

async fn campaign(req: CampaignEmail, _ctx: Data<()>) -> Result<CompleteCampaign, BoxDynError> {
    Ok::<_, _>(CompleteCampaign {
        completion_id: req.campaign_id + 1,
    })
}

async fn complete_campaign(
    _req: CompleteCampaign,
    worker: WorkerContext,
) -> Result<String, BoxDynError> {
    worker.stop()?;
    Ok::<_, _>("Completed job successfully".to_string())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    unsafe {
        std::env::set_var("RUST_LOG", "info");
    };
    tracing_subscriber::fmt::init();
    let dir = tempfile::tempdir()?;
    let conn = apalis_redb::connect(dir.path().join("tasks.redb")).await?;
    let config = Config::default().queue("simple-campaign");
    let mut backend = RedbStorage::new(conn).with_config(config);
    backend.push(WelcomeEmail { user_id: 1 }).await?;

    let workflow = SteppedFlow::new("simple-campaign-workflow")
        .and_then(welcome)
        // The delayed step is scheduled, and the worker wakes when it is due
        .delay_for(Duration::from_secs(1))
        .and_then(campaign)
        .and_then(complete_campaign);

    WorkerBuilder::new("simple-campaign-worker")
        .backend(backend)
        .data(())
        .enable_tracing()
        .concurrency(2)
        .on_event(|_c, e| info!("{e}"))
        .build(workflow)
        .run()
        .await?;
    Ok(())
}
