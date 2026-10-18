use apalis_core::backend::{Metrics, StatType, Statistic};
use redb::{ReadTransaction, ReadableTable, ReadableTableMetadata};

use crate::{
    Config, RedbStorage,
    connection::run_blocking,
    error::Error,
    queries::{current_timestamp, list_queue_names, open_read, open_read_multimap, read, tables},
};

const HOUR: u64 = 3600;
const DAY: u64 = 24 * HOUR;

/// Raw counters for one or more queues
#[derive(Debug, Default, Clone)]
pub(crate) struct QueueCounts {
    pub(crate) pending: u64,
    pub(crate) running: u64,
    pub(crate) done: u64,
    pub(crate) failed: u64,
    pub(crate) done_1h: u64,
    pub(crate) failed_1h: u64,
    pub(crate) done_24h: u64,
    pub(crate) failed_24h: u64,
    pub(crate) failed_7d: u64,
    pub(crate) most_recent: u64,
    /// Completed (done or failed) tasks per day, oldest first, for the past 7 days
    pub(crate) activity: [u64; 7],
}

impl QueueCounts {
    fn merge(&mut self, other: &QueueCounts) {
        self.pending += other.pending;
        self.running += other.running;
        self.done += other.done;
        self.failed += other.failed;
        self.done_1h += other.done_1h;
        self.failed_1h += other.failed_1h;
        self.done_24h += other.done_24h;
        self.failed_24h += other.failed_24h;
        self.failed_7d += other.failed_7d;
        self.most_recent = self.most_recent.max(other.most_recent);
        for (a, b) in self.activity.iter_mut().zip(other.activity) {
            *a += b;
        }
    }

    pub(crate) fn statistics(&self) -> Vec<Statistic> {
        let percent = |part: u64, whole: u64| {
            if whole == 0 {
                0.0
            } else {
                part as f64 / whole as f64 * 100.0
            }
        };
        let completed = self.done + self.failed;
        let total_24h = self.done_24h + self.failed_24h;
        let stat = |title: &str, stat_type: StatType, value: String, priority: u64| Statistic {
            title: title.to_owned(),
            stat_type,
            value,
            priority: Some(priority),
        };
        vec![
            stat(
                "RUNNING_JOBS",
                StatType::Number,
                self.running.to_string(),
                1,
            ),
            stat(
                "PENDING_JOBS",
                StatType::Number,
                self.pending.to_string(),
                2,
            ),
            stat("FAILED_JOBS", StatType::Number, self.failed.to_string(), 3),
            stat("DONE_JOBS", StatType::Number, self.done.to_string(), 4),
            stat(
                "TOTAL_JOBS",
                StatType::Number,
                (self.pending + self.running + completed).to_string(),
                5,
            ),
            stat(
                "SUCCESS_RATE",
                StatType::Percentage,
                format!("{:.2}", percent(self.done, completed)),
                6,
            ),
            stat(
                "FAILURE_RATE",
                StatType::Percentage,
                format!("{:.2}", percent(self.failed, completed)),
                7,
            ),
            stat(
                "MOST_RECENT_JOB",
                StatType::Timestamp,
                self.most_recent.to_string(),
                8,
            ),
            stat(
                "JOBS_PAST_HOUR",
                StatType::Number,
                (self.done_1h + self.failed_1h).to_string(),
                9,
            ),
            stat(
                "JOBS_PAST_24_HOURS",
                StatType::Number,
                total_24h.to_string(),
                10,
            ),
            stat(
                "FAILED_JOBS_PAST_7_DAYS",
                StatType::Number,
                self.failed_7d.to_string(),
                11,
            ),
            stat(
                "SUCCESS_RATE_PAST_24H",
                StatType::Percentage,
                format!("{:.2}", percent(self.done_24h, total_24h)),
                12,
            ),
            stat(
                "AVG_JOBS_PER_HOUR_PAST_24H",
                StatType::Decimal,
                format!("{:.2}", total_24h as f64 / 24.0),
                13,
            ),
        ]
    }
}

type Timeline = redb::ReadOnlyTable<(u64, &'static str), ()>;

fn count_since(table: &Timeline, since: u64) -> Result<u64, Error> {
    Ok(table.range((since, "")..)?.count() as u64)
}

pub(crate) fn queue_counts(txn: &ReadTransaction, config: &Config) -> Result<QueueCounts, Error> {
    let now = current_timestamp();
    let mut counts = QueueCounts::default();

    if let Some(active) = open_read(txn, tables::active(&config.active_jobs_list()))? {
        counts.pending += active.len()?;
    }
    if let Some(scheduled) = open_read(txn, tables::timeline(&config.scheduled_jobs_set()))? {
        counts.pending += scheduled.len()?;
    }
    if let Some(inflight) = open_read_multimap(txn, tables::inflight(&config.inflight_jobs_set()))?
    {
        counts.running = inflight.len()?;
    }

    let week_start = (now / DAY).saturating_sub(6) * DAY;
    let mut tally = |table: &Timeline, total: &mut u64, h1: &mut u64, h24: &mut u64| {
        *total = table.len()?;
        *h1 = count_since(table, now.saturating_sub(HOUR))?;
        *h24 = count_since(table, now.saturating_sub(DAY))?;
        if let Some((last, _)) = table.last()? {
            counts.most_recent = counts.most_recent.max(last.value().0);
        }
        for entry in table.range((week_start, "")..)? {
            let (key, _) = entry?;
            let day = ((key.value().0.saturating_sub(week_start)) / DAY) as usize;
            if let Some(slot) = counts.activity.get_mut(day) {
                *slot += 1;
            }
        }
        Ok::<_, Error>(())
    };

    let (mut done, mut done_1h, mut done_24h) = (0, 0, 0);
    if let Some(table) = open_read(txn, tables::timeline(&config.done_jobs_set()))? {
        tally(&table, &mut done, &mut done_1h, &mut done_24h)?;
    }
    let (mut failed, mut failed_1h, mut failed_24h, mut failed_7d) = (0, 0, 0, 0);
    if let Some(table) = open_read(txn, tables::timeline(&config.dead_jobs_set()))? {
        tally(&table, &mut failed, &mut failed_1h, &mut failed_24h)?;
        failed_7d = count_since(&table, now.saturating_sub(7 * DAY))?;
    }

    counts.done = done;
    counts.done_1h = done_1h;
    counts.done_24h = done_24h;
    counts.failed = failed;
    counts.failed_1h = failed_1h;
    counts.failed_24h = failed_24h;
    counts.failed_7d = failed_7d;
    Ok(counts)
}

impl<Args> Metrics for RedbStorage<Args>
where
    Args: 'static + Send + Sync,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Error>> + Send {
        let db = self.persist.conn.db();
        run_blocking(move || {
            read(&db, |txn| {
                let mut total = QueueCounts::default();
                for queue in list_queue_names(txn)? {
                    total.merge(&queue_counts(txn, &Config::default().queue(queue))?);
                }
                Ok(total.statistics())
            })
        })
    }

    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let db = self.persist.conn.db();
        let config = self.persist.config.clone();
        run_blocking(move || read(&db, |txn| Ok(queue_counts(txn, &config)?.statistics())))
    }
}
