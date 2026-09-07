//! Object-safe coordinator surface for local and distributed Chronon backends.

use async_trait::async_trait;
use chronon_core::{Job, JobRevision, Result, Run};
use valence::Valence;

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
    async fn list_runs(
        &self,
        job_id: Option<&str>,
        status: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>>;

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
