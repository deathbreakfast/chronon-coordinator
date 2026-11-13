//! Object-safe coordinator surface for local and distributed Chronon backends.

use std::collections::HashSet;

use async_trait::async_trait;
use chronon_core::{Job, JobRevision, Result, Run, RunKey};
use valence::Valence;

/// Most rows the default [`ChrononCoordinatorBackend::list_runs_after`] reads before it stops.
///
/// The default body scans [`ChrononCoordinatorBackend::list_runs`] from offset 0 and gives up
/// after this many rows, so runs older than the newest `LIST_RUNS_AFTER_SCAN_CAP` are never
/// returned by it. Backends that override `list_runs_after` are not limited by this value.
pub const LIST_RUNS_AFTER_SCAN_CAP: usize = 10_000;

/// Rows requested per [`ChrononCoordinatorBackend::list_runs`] call in the default scan.
const LIST_RUNS_AFTER_CHUNK: usize = 500;

/// Job/runs/revisions **admin** for server integration, UI server functions, and macros.
///
/// Object-safe (`dyn`-compatible) so hosts can hold `Arc<dyn ChrononCoordinatorBackend>` without
/// knowing whether jobs are administered through an in-process Chronon runtime or across a
/// distributed / HTTP boundary. Fluent schedule construction is Chronon's
/// [`chronon_scheduler::JobBuilder`] (this crate wraps it with Valence via [`crate::JobBuilder`]).
///
/// **Distributed mode latency:** `upsert_job` / `pause_job` / `resume_job` persist immediately;
/// automatic cron firing for a new or updated job is picked up on the next coordinator tick.
/// `run_now` persists a `queued` run immediately; a worker claims it on the next worker poll.
///
/// # Examples
///
/// ```rust,ignore
/// use chronon_coordinator::ChrononCoordinatorBackend;
///
/// # async fn schedule_and_run(
/// #     backend: &dyn ChrononCoordinatorBackend,
/// #     valence: &valence::Valence,
/// #     job: chronon_coordinator::Job,
/// # ) -> chronon_coordinator::Result<()> {
/// backend.upsert_job_with_valence(valence, job.clone()).await?;
/// let run_id = backend.run_now(&job.job_id).await?;
/// let run = backend.get_run(&run_id).await?;
/// assert!(run.is_some());
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Methods returning [`Result`] propagate [`chronon_core::ChrononError`] from the backing
/// store or coordinator runtime (validation failures, persistence errors, missing jobs). Methods
/// returning `Option` use `None` for not-found rows without an error.
#[async_trait]
pub trait ChrononCoordinatorBackend: Send + Sync {
    /// Load all jobs from the backing store into memory (in-process backends only; a no-op for
    /// backends that are always backed by a live store, such as HTTP).
    async fn load_jobs_from_db(&self) -> Result<()>;

    /// Create or update a job row (by `job.job_id`).
    ///
    /// Caller-supplied [`Job::actor_json`] must be validated with
    /// [`crate::validate_external_job_actor_json`] before persistence. Prefer
    /// [`Self::upsert_job_with_valence`] when a live [`Valence`] is available.
    async fn upsert_job(&self, job: Job) -> Result<()>;

    /// Like [`Self::upsert_job`], but sets [`Job::actor_json`] from `valence` via
    /// [`crate::snapshot_job_actor_from_valence`] before persistence (trusted in-process path).
    async fn upsert_job_with_valence(&self, valence: &Valence, job: Job) -> Result<()>;

    /// Load one job by id.
    async fn get_job(&self, job_id: &str) -> Option<Job>;

    /// Load one job by its unique `job_name`.
    async fn get_job_by_name(&self, job_name: &str) -> Option<Job>;

    /// List every known job.
    async fn list_jobs(&self) -> Vec<Job>;

    /// List runs, optionally filtered by job id and/or status, with pagination.
    ///
    /// Offset pages can skip or repeat rows when runs are inserted between reads; use
    /// [`Self::list_runs_after`] for stable cursor paging.
    async fn list_runs(
        &self,
        job_id: Option<&str>,
        status: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>>;

    /// Keyset run listing: up to `limit` runs that sort strictly after `after`.
    ///
    /// Pass the [`RunKey`] of the last run on a page (`RunKey::from(&run)`) to read the next
    /// page; `None` starts at the newest run. The contract matches
    /// [`chronon_core::SchedulerStore::list_runs_after`]:
    ///
    /// - Order is `scheduled_for` descending, then `run_id` descending as the tie-break.
    /// - With `Some(key)` only runs whose `(scheduled_for, run_id)` is lower than
    ///   `(key.scheduled_for, key.run_id)` are returned; the run at `key` itself is excluded, and
    ///   it does not need to still exist.
    /// - `job_id` and `status` filter rows before the limit is applied. `status` is interpreted
    ///   the same way as in [`Self::list_runs`] (the default body passes it through unchanged;
    ///   the upstream runtime ignores unrecognized status strings).
    /// - Paging with the key of the last row of each page visits every matching run exactly
    ///   once. A run inserted during the scan is returned only if its key sorts after the
    ///   current `after` (older than the last row read); one with a newer key is not returned by
    ///   later pages and shows up when the caller starts again from `None`.
    ///
    /// # Default body
    ///
    /// The default exists so backends written before this method keep compiling. It pages
    /// through [`Self::list_runs`] in chunks of 500 from offset 0, keeps the rows after `after`,
    /// sorts them, drops repeated `run_id`s (offset pages can repeat rows when runs are inserted
    /// mid-scan), and truncates to `limit`. Every call is O(n) in the matching rows and stops
    /// after [`LIST_RUNS_AFTER_SCAN_CAP`] rows, so older runs past the cap are never returned.
    /// Backends over a Chronon runtime should override it and call
    /// `CoordinatorService::list_runs_after` (or the store's `list_runs_after`) directly.
    async fn list_runs_after(
        &self,
        job_id: Option<&str>,
        status: Option<&str>,
        after: Option<&RunKey>,
        limit: usize,
    ) -> Result<Vec<Run>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut matches: Vec<Run> = Vec::new();
        let mut offset: usize = 0;
        while offset < LIST_RUNS_AFTER_SCAN_CAP {
            let chunk = LIST_RUNS_AFTER_CHUNK.min(LIST_RUNS_AFTER_SCAN_CAP - offset);
            let page = self.list_runs(job_id, status, offset, chunk).await?;
            let fetched = page.len();
            matches.extend(page.into_iter().filter(|run| sorts_after(run, after)));
            offset += fetched;
            if fetched < chunk {
                break;
            }
        }
        if offset >= LIST_RUNS_AFTER_SCAN_CAP {
            log::warn!(
                target: "chronon_coordinator",
                "default list_runs_after stopped at the {LIST_RUNS_AFTER_SCAN_CAP}-row scan cap; \
                 older runs are not listed (override list_runs_after on this backend)"
            );
        }
        sort_newest_first(&mut matches);
        let mut seen: HashSet<String> = HashSet::new();
        matches.retain(|run| seen.insert(run.run_id.clone()));
        matches.truncate(limit);
        Ok(matches)
    }

    /// Load one run by id.
    async fn get_run(&self, run_id: &str) -> Result<Option<Run>>;

    /// Pause a job so it no longer fires automatically (manual `run_now` still works).
    async fn pause_job(&self, job_id: &str) -> Result<()>;

    /// Resume a previously paused job.
    async fn resume_job(&self, job_id: &str) -> Result<()>;

    /// List config revision history for a job, most recent first.
    async fn list_revisions(&self, job_id_or_name: &str) -> Result<Vec<JobRevision>>;

    /// Replace a job's config with `updated`, recording a revision.
    async fn update_job_config(&self, job_id: &str, updated: Job) -> Result<()>;

    /// Persists config like [`Self::update_job_config`], using `valence` for Model access and
    /// revision `changed_by_actor_json` (via [`crate::snapshot_actor_json`]).
    async fn update_job_config_with_valence(
        &self,
        valence: &Valence,
        job_id: &str,
        updated: Job,
    ) -> Result<()>;

    /// Trigger an immediate out-of-band run of `job_id`, using its stored parameters. Returns the
    /// new run id.
    async fn run_now(&self, job_id: &str) -> Result<String>;

    /// Like [`Self::run_now`], but overrides the job's stored parameters for this run only when
    /// `params_override` is `Some`.
    async fn run_now_with_params(
        &self,
        job_id: &str,
        params_override: Option<serde_json::Value>,
    ) -> Result<String>;

    /// Like [`Self::run_now_with_params`], but also snapshots `actor_override` onto the run when
    /// `Some` (user-triggered enqueue). When `None`, inherits the job's stored `actor_json`
    /// (default jobs stay System).
    async fn run_now_with_params_and_actor(
        &self,
        job_id: &str,
        params_override: Option<serde_json::Value>,
        actor_override: Option<serde_json::Value>,
    ) -> Result<String> {
        let _ = actor_override;
        self.run_now_with_params(job_id, params_override).await
    }
}

/// Whether `run` sorts strictly after `after` in the newest-first order (`None` admits all).
fn sorts_after(run: &Run, after: Option<&RunKey>) -> bool {
    after.is_none_or(|key| {
        (run.scheduled_for, run.run_id.as_str()) < (key.scheduled_for, key.run_id.as_str())
    })
}

/// Sort by `scheduled_for` descending, then `run_id` descending.
fn sort_newest_first(runs: &mut [Run]) {
    runs.sort_by(|a, b| {
        b.scheduled_for
            .cmp(&a.scheduled_for)
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
}
