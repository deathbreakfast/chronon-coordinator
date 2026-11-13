//! # Chronon coordinator
//!
//! Coordinator API for hosts integrating [Chronon]. An object-safe
//! [`ChrononCoordinatorBackend`] trait abstracts job/run/revision **admin** (upsert, run-now,
//! list, pause) behind one `dyn` interface. Fluent **schedule construction** lives in Chronon
//! ([`chronon_scheduler::JobBuilder`]); this crate's [`JobBuilder`] wraps that API with Valence
//! actor snapshots. Typed [`ScriptScheduler`] / [`TypedJobRef`] helpers and default-job bootstrap
//! from link-time inventory sit on top. Swapping how jobs are persisted and run later is a
//! backend swap.
//!
//! [Chronon]: https://github.com/unified-field-dev/chronon
//!
//! This crate defines [`ChrononCoordinatorBackend`] and the typed helpers ([`JobBuilder`],
//! [`Scheduler`], default-job bootstrap). Runtime tick and worker loops live in upstream
//! `chronon-runtime`. Jobs are persisted and run through a host-supplied backend that implements
//! the trait.
//!
//! ## Features
//!
//! - **Script discovery** — List every script registered through
//!   `#[chronon_coordinator_macros::script]` inventory without a running backend or coordinator.
//!   [Get started](#discover-scripts)
//! - **Job scheduling** — Build cron, run-once, or manual jobs with Valence lineage via
//!   [`JobBuilder`] or persist in one chain with [`ScriptScheduler`].
//!   [Get started](#build-and-schedule-a-job)
//! - **Immediate run** — Trigger an immediate run through
//!   [`ChrononCoordinatorBackend::run_now`] or a typed [`TypedJobRef`].
//!   [Get started](#run-a-job-now)
//! - **Default job seeding** — Upsert default jobs from macro inventory on every host boot so
//!   schedules stay aligned with code. [Get started](#bootstrap-default-jobs-at-boot)
//! - **Coordinator backend** — [`ChrononCoordinatorBackend`] admin trait (upsert, list, pause,
//!   run-now) for portable product code ([`ChrononCoordinatorBackend`] API reference)
//! - **Run paging** — Page the run history newest first by cursor with
//!   [`ChrononCoordinatorBackend::list_runs_after`] and a [`RunKey`], stable under concurrent
//!   inserts (offset paging through `list_runs` can skip or repeat rows).
//!
//! *One coordinator trait — the same product code administers and inspects Chronon jobs whether
//! they run in this process or behind a split runtime. Schedule fluent API stays in Chronon.*
//!
//! # Getting started
//!
//! Most hosts hold a single `Arc<dyn ChrononCoordinatorBackend>` (built by a host-specific
//! adapter crate wrapping upstream `chronon-runtime` or a remote API) and use the helpers below
//! on top of it. Product code depends on the trait, not the concrete backend, so it stays
//! portable if the backend implementation changes later.
//!
//! ## Discover scripts
//!
//! [`Scheduler`] wraps the upstream [`ScriptRegistry`] and reads link-time inventory only. Host
//! UIs and admin server functions call this before any Chronon runtime starts.
//!
//! Prerequisites: scripts registered with `#[chronon_coordinator_macros::script]` in linked
//! crates.
//!
//! ```rust,no_run
//! use chronon_coordinator::Scheduler;
//!
//! let scheduler = Scheduler::from_inventory();
//! let names = scheduler.list_scripts();
//! assert!(!names.is_empty(), "linked inventory must expose at least one script");
//! ```
//!
//! Next: [Build and schedule a job](#build-and-schedule-a-job) once you hold a
//! [`ChrononCoordinatorBackend`].
//!
//! ## Build and schedule a job
//!
//! [`JobBuilder`] wraps upstream [`chronon_scheduler::JobBuilder`] with Valence snapshots.
//! [`ScriptScheduler`] binds a backend so `.add()` builds and persists in one async call.
//!
//! Prerequisites: a [`ChrononCoordinatorBackend`] handle, a [`ScriptHandle`] for the target
//! script, and a [`Valence`](valence::Valence) context.
//!
//! ```rust,ignore
//! # async fn wire_job_builder(
//! #     backend: &dyn chronon_coordinator::ChrononCoordinatorBackend,
//! #     handle: chronon_core::ScriptHandle<()>,
//! #     valence: valence::Valence,
//! # ) -> chronon_coordinator::Result<()> {
//! use chronon_coordinator::JobBuilder;
//!
//! let job = JobBuilder::new(&handle)
//!     .with_valence(valence.clone())
//!     .name("nightly-report")
//!     .cron("0 0 * * * *")?
//!     .build()?;
//! backend.upsert_job_with_valence(&valence, job).await?;
//! let stored = backend
//!     .get_job_by_name("nightly-report")
//!     .await
//!     .expect("upserted job must be readable");
//! assert_eq!(stored.job_name, "nightly-report");
//! # Ok(())
//! # }
//! ```
//!
//! One-chain variant with [`ScriptScheduler`]:
//!
//! ```rust,ignore
//! # async fn wire_script_scheduler(
//! #     backend: &dyn chronon_coordinator::ChrononCoordinatorBackend,
//! #     handle: chronon_core::ScriptHandle<()>,
//! #     valence: valence::Valence,
//! # ) -> chronon_coordinator::Result<()> {
//! use chronon_coordinator::ScriptScheduler;
//!
//! let job = ScriptScheduler::new(backend, handle, valence)
//!     .name("nightly-report")
//!     .cron("0 0 * * * *")?
//!     .add()
//!     .await?;
//! assert_eq!(job.job_name, "nightly-report");
//! # Ok(())
//! # }
//! ```
//!
//! Next: [Run a job now](#run-a-job-now) to fire a manual run without waiting for cron.
//!
//! ## Run a job now
//!
//! [`ChrononCoordinatorBackend::run_now`] enqueues a `queued` run immediately; workers claim it
//! on the next poll. Use [`TypedJobRef::run_now`] when you need typed parameter override on an
//! existing job resolved through [`typed_job_ref_for_script`].
//!
//! Prerequisites: a persisted job id or a job name resolved through [`typed_job_ref_for_script`].
//!
//! ```rust,ignore
//! # async fn run(backend: &dyn chronon_coordinator::ChrononCoordinatorBackend) -> chronon_coordinator::Result<()> {
//! let run_id = backend.run_now("job-id").await?;
//! assert!(!run_id.is_empty(), "run_now must return a run id");
//! # Ok(())
//! # }
//! ```
//!
//! Typed variant:
//!
//! ```rust,ignore
//! # async fn run_typed(backend: &dyn chronon_coordinator::ChrononCoordinatorBackend) -> chronon_coordinator::Result<()> {
//! use chronon_coordinator::typed_job_ref_for_script;
//!
//! let job_ref = typed_job_ref_for_script::<()>(backend, "nightly-report", "send_report").await?;
//! let run_id = job_ref.run_now().await?;
//! assert!(run_id.starts_with("run-"), "run id must be persisted");
//! # Ok(())
//! # }
//! ```
//!
//! Next: [Bootstrap default jobs at boot](#bootstrap-default-jobs-at-boot) so inventory-declared
//! schedules exist before workers start.
//!
//! ## Bootstrap default jobs at boot
//!
//! Call [`ensure_default_jobs_embedded`] once at host boot after the coordinator backend is wired
//! and before workers dequeue runs. The call upserts every job from
//! `#[chronon_coordinator_macros::script(..., default_job(...))]` inventory and repairs schedule
//! drift on restart.
//!
//! Prerequisites: a [`ChrononCoordinatorBackend`] handle and a Valence factory closure (or use
//! [`register_default_jobs_embedded`] for fire-and-forget boot code). Skip hand-managed jobs with
//! [`ensure_default_jobs_embedded_with_skip`].
//!
//! ```rust,ignore
//! # async fn boot(
//! #     backend: std::sync::Arc<dyn chronon_coordinator::ChrononCoordinatorBackend>,
//! #     build_valence: impl FnMut() -> anyhow::Result<valence::Valence>,
//! # ) -> anyhow::Result<()> {
//! use chronon_coordinator::{ensure_default_jobs_embedded, ChrononCoordinatorBackend};
//!
//! ensure_default_jobs_embedded(backend.clone(), build_valence).await?;
//! let jobs = backend.list_jobs().await?;
//! assert!(!jobs.is_empty(), "default jobs must be seeded from inventory");
//! # Ok(())
//! # }
//! ```
//!
//! Fire-and-forget boot wrapper (errors logged, not propagated):
//!
//! ```rust,ignore
//! # async fn boot_fire_and_forget(
//! #     backend: std::sync::Arc<dyn chronon_coordinator::ChrononCoordinatorBackend>,
//! #     factory: std::sync::Arc<dyn valence::ValenceFactory>,
//! # ) {
//! chronon_coordinator::register_default_jobs_embedded(backend, factory).await;
//! # }
//! ```
//!
//! Runnable example: `cargo run -p chronon-coordinator --example register_default_jobs_embedded`.
//!
//! Older import path: [`models`].
//!
//! Runnable examples: see `examples/README.md` in this crate.
//!
//! Jobs are persisted and run through a host-supplied [`ChrononCoordinatorBackend`] (for example
//! an in-process adapter over `chronon-runtime`, or a remote HTTP client).

mod coordinator_trait;
mod default_job;
mod job_builder;
mod scheduler;
mod typed;

pub use chronon_core::{
    ChrononError, Job, JobRevision, MisfirePolicy, Result, RetryPolicy, Run, RunKey, RunStatus,
    ScheduleKind, Script, ScriptHandle,
};
/// Upstream script registry re-export, shared by [`Scheduler`] and [`ScriptHandle`] discovery.
pub use chronon_executor::ScriptRegistry;
/// Parsed cron expression re-export, used by [`JobBuilder::cron`].
pub use chronon_scheduler::CronExpr;
pub use coordinator_trait::{ChrononCoordinatorBackend, LIST_RUNS_AFTER_SCAN_CAP};
pub use default_job::{
    default_job_schedule_equivalent, ensure_default_jobs_embedded,
    ensure_default_jobs_embedded_with_skip, merge_default_job_schedule_fields,
    DefaultJobDescriptor, DefaultJobEnsureFn, DefaultJobRegistry,
};
pub use job_builder::{
    snapshot_actor_json, snapshot_job_actor_from_valence, validate_external_job_actor_json,
    JobBuilder,
};
pub use quark::inventory;
pub use scheduler::Scheduler;
pub use typed::{typed_job_ref_for_script, ScriptScheduler, TypedJobRef};

/// Re-exports model types under [`models`] for the `chronon_coordinator::models::*` import path.
pub mod models {
    pub use chronon_core::{
        Job, JobRevision, MisfirePolicy, RetryPolicy, Run, RunKey, RunStatus, ScheduleKind, Script,
    };
}

/// Register default jobs discovered via link-time inventory using a host Valence factory.
///
/// Builds a fresh `Actor::System` [`Valence`](valence::Valence) context per job via `factory`,
/// then delegates to [`ensure_default_jobs_embedded`]. Errors are logged (target
/// `"chronon_default_jobs"`) rather than propagated, since this is typically called from
/// fire-and-forget boot code; use [`ensure_default_jobs_embedded`] directly if you need to
/// handle failures.
///
/// # Examples
///
/// ```rust,ignore
/// # async fn boot(
/// #     backend: std::sync::Arc<dyn chronon_coordinator::ChrononCoordinatorBackend>,
/// #     factory: std::sync::Arc<dyn valence::ValenceFactory>,
/// # ) {
/// chronon_coordinator::register_default_jobs_embedded(backend, factory).await;
/// # }
/// ```
pub async fn register_default_jobs_embedded(
    backend: std::sync::Arc<dyn ChrononCoordinatorBackend>,
    factory: std::sync::Arc<dyn valence::ValenceFactory>,
) {
    register_default_jobs_embedded_with_skip(backend, factory, &[]).await;
}

/// Like [`register_default_jobs_embedded`], but skips job names in `skip`.
///
/// Use this when a host manages some default jobs' schedule by hand (e.g. an admin UI has
/// already customized it) and does not want this bootstrap to overwrite it.
pub async fn register_default_jobs_embedded_with_skip(
    backend: std::sync::Arc<dyn ChrononCoordinatorBackend>,
    factory: std::sync::Arc<dyn valence::ValenceFactory>,
    skip: &[&str],
) {
    use valence::Actor;

    let actor = Actor::System {
        operation: "chronon_default_jobs".into(),
    };
    let actor_json = match serde_json::to_value(&actor) {
        Ok(json) => json,
        Err(err) => {
            log::error!(
                target: "chronon_default_jobs",
                "failed to serialize system actor for default jobs: {err}"
            );
            return;
        }
    };
    if let Err(err) = factory.build(&actor_json) {
        log::error!(
            target: "chronon_default_jobs",
            "valence build for default jobs: {err}"
        );
        return;
    }
    let factory = std::sync::Arc::clone(&factory);
    let build_valence = move || {
        factory.build(&actor_json).map_err(|err| {
            anyhow::anyhow!("valence build for default jobs after successful probe: {err}")
        })
    };
    match ensure_default_jobs_embedded_with_skip(backend, build_valence, skip).await {
        Ok(()) => log::info!(target: "chronon_default_jobs", "default Chronon jobs registered"),
        Err(e) => log::error!(
            target: "chronon_default_jobs",
            "default Chronon jobs failed: {e}"
        ),
    }
}
