//! Contract tests: the default `run_now_with_params_and_actor`, `Scheduler` registry handles, and
//! `ScriptScheduler` schedule setters.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use chronon_coordinator::{
    ChrononCoordinatorBackend, ChrononError, Job, JobRevision, Result, Run, ScheduleKind,
    Scheduler, ScriptHandle, ScriptScheduler,
};
use valence::{Actor, InMemoryBackend, Valence};

fn test_valence() -> Valence {
    Valence::builder()
        .add_backend("default", Arc::new(InMemoryBackend::new()))
        .with_actor(Actor::System {
            operation: "chronon_coordinator_scheduler_contract".into(),
        })
        .build()
        .expect("valence")
}

const fn demo_handle() -> ScriptHandle<()> {
    ScriptHandle::<()>::new("demo_script")
}

type Seen = (String, Option<serde_json::Value>);

/// Implements only the required methods, so `run_now_with_params_and_actor` uses the trait default.
#[derive(Default)]
struct DefaultActorBackend {
    seen: Mutex<Vec<Seen>>,
    jobs: Mutex<Vec<Job>>,
}

#[async_trait]
impl ChrononCoordinatorBackend for DefaultActorBackend {
    async fn load_jobs_from_db(&self) -> Result<()> {
        Ok(())
    }
    async fn upsert_job(&self, job: Job) -> Result<()> {
        self.jobs.lock().unwrap().push(job);
        Ok(())
    }
    async fn upsert_job_with_valence(&self, _valence: &Valence, job: Job) -> Result<()> {
        self.upsert_job(job).await
    }
    async fn get_job(&self, _job_id: &str) -> Option<Job> {
        None
    }
    async fn get_job_by_name(&self, _job_name: &str) -> Option<Job> {
        None
    }
    async fn list_jobs(&self) -> Vec<Job> {
        self.jobs.lock().unwrap().clone()
    }
    async fn list_runs(
        &self,
        _job_id: Option<&str>,
        _status: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<Run>> {
        Ok(Vec::new())
    }
    async fn get_run(&self, _run_id: &str) -> Result<Option<Run>> {
        Ok(None)
    }
    async fn pause_job(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }
    async fn resume_job(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }
    async fn list_revisions(&self, _job_id_or_name: &str) -> Result<Vec<JobRevision>> {
        Ok(Vec::new())
    }
    async fn update_job_config(&self, _job_id: &str, _updated: Job) -> Result<()> {
        Ok(())
    }
    async fn update_job_config_with_valence(
        &self,
        _valence: &Valence,
        _job_id: &str,
        _updated: Job,
    ) -> Result<()> {
        Ok(())
    }
    async fn run_now(&self, job_id: &str) -> Result<String> {
        self.run_now_with_params(job_id, None).await
    }
    async fn run_now_with_params(
        &self,
        job_id: &str,
        params_override: Option<serde_json::Value>,
    ) -> Result<String> {
        if job_id == "missing" {
            return Err(ChrononError::JobNotFound(job_id.to_string()));
        }
        self.seen
            .lock()
            .unwrap()
            .push((job_id.to_string(), params_override));
        Ok(format!("run-{job_id}"))
    }
}

#[tokio::test]
async fn default_actor_run_forwards_params_and_drops_actor() {
    let backend = DefaultActorBackend::default();
    let params = serde_json::json!({ "n": 3 });
    let actor = serde_json::json!({ "User": { "id": "u1" } });
    let run_id = backend
        .run_now_with_params_and_actor("job-1", Some(params.clone()), Some(actor))
        .await
        .expect("run");
    assert_eq!(run_id, "run-job-1");
    let seen = backend.seen.lock().unwrap();
    assert_eq!(seen.as_slice(), &[("job-1".to_string(), Some(params))]);
}

#[tokio::test]
async fn default_actor_run_without_overrides_forwards_none() {
    let backend = DefaultActorBackend::default();
    backend
        .run_now_with_params_and_actor("job-2", None, None)
        .await
        .expect("run");
    let seen = backend.seen.lock().unwrap();
    assert_eq!(seen.as_slice(), &[("job-2".to_string(), None)]);
}

#[tokio::test]
async fn default_actor_run_propagates_backend_error() {
    let backend = DefaultActorBackend::default();
    match backend
        .run_now_with_params_and_actor("missing", None, Some(serde_json::json!({})))
        .await
    {
        Ok(_) => panic!("expected JobNotFound"),
        Err(ChrononError::JobNotFound(id)) => assert_eq!(id, "missing"),
        Err(other) => panic!("expected JobNotFound, got {other:?}"),
    }
    assert!(backend.seen.lock().unwrap().is_empty());
}

#[test]
fn scheduler_registry_handles_share_one_registry() {
    let scheduler = Scheduler::from_inventory();
    let shared = scheduler.registry_arc();
    assert!(std::ptr::eq(scheduler.registry(), Arc::as_ptr(&shared)));
    assert_eq!(scheduler.list_scripts().len(), shared.list().len());
}

#[test]
fn scheduler_from_registry_reuses_the_given_registry() {
    let first = Scheduler::from_inventory();
    let registry = first.registry_arc();
    let second = Scheduler::from_registry(Arc::clone(&registry));
    assert!(Arc::ptr_eq(&second.registry_arc(), &registry));
    assert_eq!(second.list_scripts(), first.list_scripts());
}

#[test]
fn scheduler_script_builder_targets_the_handle_script() {
    let job = Scheduler::from_inventory()
        .script(&demo_handle())
        .with_valence(test_valence())
        .name("scheduler-script")
        .manual()
        .build()
        .expect("build");
    assert_eq!(job.script_name, "demo_script");
}

#[tokio::test]
async fn script_scheduler_persists_timezone_with_cron() {
    let backend = DefaultActorBackend::default();
    let job = ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("tz-demo")
        .cron("0 0 * * * *")
        .expect("cron")
        .timezone("America/New_York")
        .add()
        .await
        .expect("add");
    assert_eq!(job.schedule_kind, ScheduleKind::Cron);
    assert_eq!(job.timezone.as_deref(), Some("America/New_York"));
    assert_eq!(backend.list_jobs().await.len(), 1);
}

#[tokio::test]
async fn script_scheduler_invalid_timezone_fails_before_upsert() {
    let backend = DefaultActorBackend::default();
    let result = ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("bad-tz")
        .cron("0 0 * * * *")
        .expect("cron")
        .timezone("not-a-timezone")
        .add()
        .await;
    assert!(matches!(result, Err(ChrononError::InvalidTimezone(_))));
    assert!(backend.list_jobs().await.is_empty());
}

#[tokio::test]
async fn script_scheduler_run_once_at_persists_schedule() {
    let backend = DefaultActorBackend::default();
    let at = Utc.with_ymd_and_hms(2032, 3, 4, 5, 6, 7).unwrap();
    let job = ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("once-sched")
        .run_once_at(at)
        .add()
        .await
        .expect("add");
    assert_eq!(job.schedule_kind, ScheduleKind::RunOnce);
    assert_eq!(job.run_once_at, Some(at));
    assert_eq!(backend.list_jobs().await.len(), 1);
}

#[tokio::test]
async fn script_scheduler_missing_name_fails_before_upsert() {
    let backend = DefaultActorBackend::default();
    let result = ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .manual()
        .add()
        .await;
    assert!(matches!(result, Err(ChrononError::ParamError(_))));
    assert!(backend.list_jobs().await.is_empty());
}
