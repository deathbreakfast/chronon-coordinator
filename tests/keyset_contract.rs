//! Contract tests: the default `list_runs_after` keyset read built on top of `list_runs`.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use chronon_coordinator::{
    ChrononCoordinatorBackend, Job, JobRevision, Result, Run, RunKey, RunStatus,
    LIST_RUNS_AFTER_SCAN_CAP,
};
use valence::Valence;

const PAGE: usize = 100;
const STATUSES: [RunStatus; 4] = [
    RunStatus::Queued,
    RunStatus::Success,
    RunStatus::Failed,
    RunStatus::Running,
];

fn base_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
}

fn make_run(run_id: String, job_id: &str, status: RunStatus, at: DateTime<Utc>) -> Run {
    let mut run = Run::new("demo_script", at);
    run.run_id = run_id;
    run.job_id = Some(job_id.to_string());
    run.status = status;
    run
}

/// `count` runs over two jobs and four statuses; every four runs share one timestamp, and
/// run ids are a permutation of the index so id order differs from insertion order.
fn seed_runs(count: usize) -> Vec<Run> {
    (0..count)
        .map(|i| {
            let job = if i.is_multiple_of(3) {
                "job-b"
            } else {
                "job-a"
            };
            let status = STATUSES[(i / 3) % STATUSES.len()];
            let at = base_time() + Duration::seconds(i64::try_from(i / 4).unwrap());
            let id = format!("run-{:06}", (i * 7919) % count);
            make_run(id, job, status, at)
        })
        .collect()
}

fn sort_newest_first(runs: &mut [Run]) {
    runs.sort_by(|a, b| {
        b.scheduled_for
            .cmp(&a.scheduled_for)
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
}

fn status_name(status: RunStatus) -> String {
    serde_json::to_value(status)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

/// Parses a status filter the way the upstream runtime does: unknown strings mean no filter.
fn parse_status(status: Option<&str>) -> Option<RunStatus> {
    let wanted = status?.to_ascii_lowercase();
    [
        RunStatus::Queued,
        RunStatus::Claimed,
        RunStatus::Running,
        RunStatus::Success,
        RunStatus::Failed,
        RunStatus::Canceled,
        RunStatus::Timeout,
    ]
    .into_iter()
    .find(|s| status_name(*s) == wanted)
}

fn matches(run: &Run, job_id: Option<&str>, status: Option<RunStatus>) -> bool {
    job_id.is_none_or(|id| run.job_id.as_deref() == Some(id))
        && status.is_none_or(|s| run.status == s)
}

/// Implements only `list_runs` (offset over a newest-first Vec), so `list_runs_after` uses the
/// trait default. `inject` inserts runs right before the `list_runs` call with that index.
#[derive(Default)]
struct OffsetOnlyBackend {
    runs: Mutex<Vec<Run>>,
    calls: Mutex<Vec<(usize, usize)>>,
    inject: Mutex<Option<(usize, Vec<Run>)>>,
}

impl OffsetOnlyBackend {
    fn with_runs(mut runs: Vec<Run>) -> Self {
        sort_newest_first(&mut runs);
        Self {
            runs: Mutex::new(runs),
            ..Self::default()
        }
    }

    fn insert(&self, extra: Vec<Run>) {
        let mut runs = self.runs.lock().unwrap();
        runs.extend(extra);
        sort_newest_first(&mut runs);
    }

    fn inject_before_call(&self, call_index: usize, extra: Vec<Run>) {
        *self.inject.lock().unwrap() = Some((call_index, extra));
    }

    /// Every matching run, newest first: the oracle the default must reproduce.
    fn expected(&self, job_id: Option<&str>, status: Option<&str>) -> Vec<Run> {
        let status = parse_status(status);
        let mut runs: Vec<Run> = self
            .runs
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches(r, job_id, status))
            .cloned()
            .collect();
        sort_newest_first(&mut runs);
        runs
    }

    fn take_pending_inject(&self, call_index: usize) -> Option<Vec<Run>> {
        let mut inject = self.inject.lock().unwrap();
        match inject.as_ref() {
            Some((at, _)) if *at == call_index => inject.take().map(|(_, runs)| runs),
            _ => None,
        }
    }
}

#[async_trait]
impl ChrononCoordinatorBackend for OffsetOnlyBackend {
    async fn load_jobs_from_db(&self) -> Result<()> {
        Ok(())
    }
    async fn upsert_job(&self, _job: Job) -> Result<()> {
        Ok(())
    }
    async fn upsert_job_with_valence(&self, _valence: &Valence, _job: Job) -> Result<()> {
        Ok(())
    }
    async fn get_job(&self, _job_id: &str) -> Option<Job> {
        None
    }
    async fn get_job_by_name(&self, _job_name: &str) -> Option<Job> {
        None
    }
    async fn list_jobs(&self) -> Vec<Job> {
        Vec::new()
    }
    async fn list_runs(
        &self,
        job_id: Option<&str>,
        status: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        let call_index = {
            let mut calls = self.calls.lock().unwrap();
            calls.push((offset, limit));
            calls.len() - 1
        };
        if let Some(extra) = self.take_pending_inject(call_index) {
            self.insert(extra);
        }
        let status = parse_status(status);
        Ok(self
            .runs
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches(r, job_id, status))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
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
        Ok(format!("run-{job_id}"))
    }
    async fn run_now_with_params(
        &self,
        job_id: &str,
        _params_override: Option<serde_json::Value>,
    ) -> Result<String> {
        Ok(format!("run-{job_id}"))
    }
}

/// Pages with the key of the last row until an empty page, starting after `start`.
async fn page_all(
    backend: &OffsetOnlyBackend,
    job_id: Option<&str>,
    status: Option<&str>,
    start: Option<RunKey>,
    page: usize,
) -> Vec<Run> {
    let mut out = Vec::new();
    let mut after = start;
    for _ in 0..=(LIST_RUNS_AFTER_SCAN_CAP / page + 1) {
        let rows = backend
            .list_runs_after(job_id, status, after.as_ref(), page)
            .await
            .expect("list_runs_after");
        assert!(rows.len() <= page, "page larger than limit");
        if rows.is_empty() {
            return out;
        }
        after = rows.last().map(RunKey::from);
        out.extend(rows);
    }
    panic!("paging did not terminate");
}

fn run_ids(runs: &[Run]) -> Vec<String> {
    runs.iter().map(|r| r.run_id.clone()).collect()
}

fn assert_strictly_descending(runs: &[Run]) {
    for pair in runs.windows(2) {
        let (a, b) = (RunKey::from(&pair[0]), RunKey::from(&pair[1]));
        assert!(
            (a.scheduled_for, &a.run_id) > (b.scheduled_for, &b.run_id),
            "rows out of order: {a:?} then {b:?}"
        );
    }
}

async fn assert_pages_match_oracle(job_id: Option<&str>, status: Option<&str>) {
    let backend = OffsetOnlyBackend::with_runs(seed_runs(2_500));
    let expected = backend.expected(job_id, status);
    assert!(!expected.is_empty(), "filter must match some seeded runs");
    let got = page_all(&backend, job_id, status, None, PAGE).await;
    assert_strictly_descending(&got);
    assert_eq!(run_ids(&got), run_ids(&expected));
}

#[tokio::test]
async fn default_list_runs_after_pages_every_run_once_unfiltered() {
    assert_pages_match_oracle(None, None).await;
}

#[tokio::test]
async fn default_list_runs_after_pages_every_run_once_with_job_filter() {
    assert_pages_match_oracle(Some("job-b"), None).await;
}

#[tokio::test]
async fn default_list_runs_after_pages_every_run_once_with_status_filter() {
    assert_pages_match_oracle(None, Some("failed")).await;
    assert_pages_match_oracle(Some("job-a"), Some("success")).await;
}

#[tokio::test]
async fn default_list_runs_after_passes_unknown_status_through_like_list_runs() {
    let backend = OffsetOnlyBackend::with_runs(seed_runs(300));
    let via_offset = backend
        .list_runs(None, Some("not-a-status"), 0, 50)
        .await
        .unwrap();
    let via_keyset = backend
        .list_runs_after(None, Some("not-a-status"), None, 50)
        .await
        .unwrap();
    assert_eq!(run_ids(&via_keyset), run_ids(&via_offset));
    assert_eq!(via_keyset.len(), 50);
}

#[tokio::test]
async fn default_list_runs_after_excludes_cursor_row_and_honours_limit() {
    let backend = OffsetOnlyBackend::with_runs(seed_runs(300));
    let all = backend.expected(None, None);
    let cursor = RunKey::from(&all[10]);
    let rows = backend
        .list_runs_after(None, None, Some(&cursor), 5)
        .await
        .unwrap();
    assert_eq!(run_ids(&rows), run_ids(&all[11..16]));

    let missing = RunKey {
        scheduled_for: cursor.scheduled_for,
        run_id: format!("{}~", cursor.run_id),
    };
    let rows = backend
        .list_runs_after(None, None, Some(&missing), 1)
        .await
        .unwrap();
    assert_eq!(rows[0].run_id, cursor.run_id, "cursor row need not exist");

    let none = backend.list_runs_after(None, None, None, 0).await.unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn default_list_runs_after_between_pages_skips_newer_and_keeps_older_inserts() {
    let seeded = seed_runs(2_500);
    let backend = OffsetOnlyBackend::with_runs(seeded);
    let first = backend
        .list_runs_after(None, None, None, PAGE)
        .await
        .unwrap();
    let original = backend.expected(None, None);

    let newer = make_run(
        "run-newer".into(),
        "job-a",
        RunStatus::Queued,
        original[0].scheduled_for + Duration::hours(1),
    );
    let older = make_run(
        "run-older".into(),
        "job-a",
        RunStatus::Queued,
        base_time() - Duration::hours(1),
    );
    backend.insert(vec![newer, older]);

    let start = first.last().map(RunKey::from);
    let rest = page_all(&backend, None, None, start, PAGE).await;
    let mut got = first;
    got.extend(rest);
    assert_strictly_descending(&got);

    let mut expected = run_ids(&original);
    expected.push("run-older".into());
    assert_eq!(run_ids(&got), expected);

    let restart = backend.list_runs_after(None, None, None, 1).await.unwrap();
    assert_eq!(restart[0].run_id, "run-newer", "restart from None sees it");
}

#[tokio::test]
async fn default_list_runs_after_dedupes_rows_shifted_by_inserts_mid_scan() {
    let backend = OffsetOnlyBackend::with_runs(seed_runs(2_500));
    let original = backend.expected(None, None);
    let cursor = RunKey::from(&original[PAGE - 1]);
    // Sorts around offset 1500: below the cursor and past where the injection happens.
    let older = make_run(
        "run-mid-older".into(),
        "job-a",
        RunStatus::Queued,
        original[1_500].scheduled_for - Duration::seconds(1),
    );
    let newer = make_run(
        "run-mid-newer".into(),
        "job-a",
        RunStatus::Queued,
        original[0].scheduled_for + Duration::hours(1),
    );
    // Before the third internal list_runs call (offset 1000): the newer insert shifts every
    // later offset page by one, so the scan reads a row it already saw.
    backend.inject_before_call(2, vec![older, newer]);

    let rows = backend
        .list_runs_after(None, None, Some(&cursor), 5_000)
        .await
        .unwrap();
    let ids: HashSet<String> = run_ids(&rows).into_iter().collect();
    assert_eq!(ids.len(), rows.len(), "no run returned twice");
    assert_strictly_descending(&rows);
    assert!(ids.contains("run-mid-older"));
    assert!(!ids.contains("run-mid-newer"));

    let expected: Vec<Run> = backend
        .expected(None, None)
        .into_iter()
        .filter(|r| (r.scheduled_for, &r.run_id) < (cursor.scheduled_for, &cursor.run_id))
        .collect();
    assert_eq!(run_ids(&rows), run_ids(&expected));
}

#[tokio::test]
async fn default_list_runs_after_stops_scanning_at_cap() {
    let total: usize = LIST_RUNS_AFTER_SCAN_CAP + 2_000;
    let backend = OffsetOnlyBackend::with_runs(seed_runs(total));
    let expected = backend.expected(None, None);

    let got = page_all(&backend, None, None, None, 500).await;
    assert_strictly_descending(&got);
    assert_eq!(
        run_ids(&got),
        run_ids(&expected[..LIST_RUNS_AFTER_SCAN_CAP]),
        "only the newest LIST_RUNS_AFTER_SCAN_CAP runs are reachable through the default"
    );

    let calls = backend.calls.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|(offset, limit)| offset + limit <= LIST_RUNS_AFTER_SCAN_CAP),
        "default never reads past the scan cap"
    );
}
