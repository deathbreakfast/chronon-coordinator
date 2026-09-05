//! Typed script scheduling and run-now helpers.

use chrono::{DateTime, Utc};
use chronon_core::{ChrononError, Job, MisfirePolicy, Result, RetryPolicy, ScriptHandle};
use serde::Serialize;
use valence::Valence;

use crate::coordinator_trait::ChrononCoordinatorBackend;
use crate::JobBuilder;

/// Resolve a typed job reference for a script name (used by generated script types).
///
/// Loads the job by `job_name` and checks that its stored `script_name` matches
/// `expected_script_name`, so a caller cannot accidentally run one script's job through another
/// script's typed parameter type.
///
/// # Examples
///
/// ```rust,ignore
/// # async fn run(backend: &dyn chronon_coordinator::ChrononCoordinatorBackend) -> chronon_coordinator::Result<()> {
/// use chronon_coordinator::typed_job_ref_for_script;
///
/// let job_ref = typed_job_ref_for_script::<()>(backend, "nightly-report", "send_report").await?;
/// job_ref.run_now().await?;
/// # Ok(())
/// # }
/// ```
pub async fn typed_job_ref_for_script<'a, P>(
    backend: &'a dyn ChrononCoordinatorBackend,
    job_name: &str,
    expected_script_name: &'static str,
) -> Result<TypedJobRef<'a, P>> {
    let job = backend
        .get_job_by_name(job_name)
        .await
        .ok_or_else(|| ChrononError::JobNotFound(job_name.to_string()))?;
    if job.script_name != expected_script_name {
        return Err(ChrononError::ScriptMismatch {
            expected: expected_script_name.to_string(),
            actual: job.script_name.clone(),
            job_name: job.job_name,
        });
    }
    Ok(TypedJobRef::new(backend, job))
}

/// Typed scheduler API bound to a backend and Valence context.
///
/// Wraps [`JobBuilder`] with a bound `backend`, so [`ScriptScheduler::add`] both builds and
/// persists the job in one call instead of requiring the caller to call
/// [`ChrononCoordinatorBackend::upsert_job`](crate::ChrononCoordinatorBackend::upsert_job)
/// separately.
///
/// # Examples
///
/// ```rust,ignore
/// # async fn schedule(
/// #     backend: &dyn chronon_coordinator::ChrononCoordinatorBackend,
/// #     handle: chronon_core::ScriptHandle<()>,
/// #     valence: valence::Valence,
/// # ) -> chronon_coordinator::Result<()> {
/// use chronon_coordinator::ScriptScheduler;
///
/// let job = ScriptScheduler::new(backend, handle, valence)
///     .name("nightly-report")
///     .cron("0 0 * * * *")?
///     .add()
///     .await?;
/// # let _ = job;
/// # Ok(())
/// # }
/// ```
pub struct ScriptScheduler<'a, P>
where
    P: Serialize,
{
    backend: &'a dyn ChrononCoordinatorBackend,
    builder: JobBuilder<P>,
}

impl<'a, P> ScriptScheduler<'a, P>
where
    P: Serialize,
{
    /// Create a scheduler API from a typed script handle.
    pub fn new(
        backend: &'a dyn ChrononCoordinatorBackend,
        handle: &ScriptHandle<P>,
        valence: Valence,
    ) -> Self {
        Self {
            backend,
            builder: JobBuilder::new(handle).with_valence(valence),
        }
    }

    /// Set the job name (unique per deployment). See [`JobBuilder::name`].
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.builder = self.builder.name(name);
        self
    }

    /// Set the cron schedule. See [`JobBuilder::cron`].
    pub fn cron(mut self, expr: &str) -> Result<Self> {
        self.builder = self.builder.cron(expr)?;
        Ok(self)
    }

    /// Set the timezone for cron evaluation. See [`JobBuilder::timezone`].
    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.builder = self.builder.timezone(tz);
        self
    }

    /// Schedule a one-time execution at the specified time. See [`JobBuilder::run_once_at`].
    pub fn run_once_at(mut self, at: DateTime<Utc>) -> Self {
        self.builder = self.builder.run_once_at(at);
        self
    }

    /// Set the job to manual-only (no automatic scheduling). See [`JobBuilder::manual`].
    pub fn manual(mut self) -> Self {
        self.builder = self.builder.manual();
        self
    }

    /// Set the typed script parameters. See [`JobBuilder::params`].
    pub fn params(mut self, params: P) -> Self {
        self.builder = self.builder.params(params);
        self
    }

    /// Set the execution pool (for distributed mode). See [`JobBuilder::pool`].
    pub fn pool(mut self, pool: impl Into<String>) -> Self {
        self.builder = self.builder.pool(pool);
        self
    }

    /// Set the target region (for distributed mode). See [`JobBuilder::region`].
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.builder = self.builder.region(region);
        self
    }

    /// Set maximum concurrent runs. See [`JobBuilder::concurrency`].
    pub fn concurrency(mut self, max: i32) -> Self {
        self.builder = self.builder.concurrency(max);
        self
    }

    /// Set execution timeout in milliseconds. See [`JobBuilder::timeout_ms`].
    pub fn timeout_ms(mut self, ms: i64) -> Self {
        self.builder = self.builder.timeout_ms(ms);
        self
    }

    /// Set the retry policy for failed runs. See [`JobBuilder::retry_policy`].
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.builder = self.builder.retry_policy(policy);
        self
    }

    /// Set the misfire policy for missed runs. See [`JobBuilder::misfire_policy`].
    pub fn misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.builder = self.builder.misfire_policy(policy);
        self
    }

    /// Disable the job (it won't run until enabled). See [`JobBuilder::disabled`].
    pub fn disabled(mut self) -> Self {
        self.builder = self.builder.disabled();
        self
    }

    /// Build the job and persist it via the bound backend's
    /// [`upsert_job_with_valence`](crate::ChrononCoordinatorBackend::upsert_job_with_valence).
    pub async fn add(self) -> Result<Job> {
        let valence = self
            .builder
            .valence()
            .ok_or_else(|| ChrononError::ParamError("valence context is required".to_string()))?
            .clone();
        let job = self.builder.build()?;
        self.backend
            .upsert_job_with_valence(&valence, job.clone())
            .await?;
        Ok(job)
    }
}

/// Typed view of an existing job that supports typed one-off parameter override.
///
/// Obtain one via [`typed_job_ref_for_script`], which also validates that the loaded job was
/// created against the expected script.
///
/// # Examples
///
/// ```rust,ignore
/// use chronon_coordinator::typed_job_ref_for_script;
///
/// let job_ref = typed_job_ref_for_script::<MyParams>(backend, &job, "my_script")?;
/// job_ref.params(MyParams { dry_run: true }).run_now().await?;
/// ```
#[derive(Clone)]
pub struct TypedJobRef<'a, P> {
    backend: &'a dyn ChrononCoordinatorBackend,
    job: Job,
    params_override: Option<P>,
    actor_override: Option<serde_json::Value>,
}

impl<'a, P> TypedJobRef<'a, P> {
    /// Wrap an already-loaded job with no parameter override.
    ///
    /// Prefer [`typed_job_ref_for_script`], which also checks the job's script name.
    pub fn new(backend: &'a dyn ChrononCoordinatorBackend, job: Job) -> Self {
        Self {
            backend,
            job,
            params_override: None,
            actor_override: None,
        }
    }

    /// The underlying job's stored config.
    pub const fn job(&self) -> &Job {
        &self.job
    }

    /// Set a one-off parameter override for the next [`Self::run_now`] call, leaving the job's
    /// stored parameters unchanged.
    pub fn params(mut self, params: P) -> Self {
        self.params_override = Some(params);
        self
    }

    /// Snapshot the live Valence actor onto the next run (user-triggered enqueue).
    ///
    /// # Errors
    ///
    /// Returns [`ChrononError::ParamError`] when the actor cannot be serialized.
    pub fn with_actor_from_valence(mut self, valence: &valence::Valence) -> Result<Self> {
        self.actor_override = Some(crate::snapshot_actor_json(valence)?);
        Ok(self)
    }
}

impl<P> TypedJobRef<'_, P>
where
    P: Serialize,
{
    /// Trigger an immediate out-of-band run, using [`Self::params`] if set or the job's stored
    /// parameters otherwise. Returns the new run id.
    pub async fn run_now(self) -> Result<String> {
        let params_override = match self.params_override {
            Some(params) => Some(serde_json::to_value(params)?),
            None => None,
        };
        self.backend
            .run_now_with_params_and_actor(&self.job.job_id, params_override, self.actor_override)
            .await
    }
}
