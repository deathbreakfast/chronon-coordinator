# chronon-coordinator verification

Chronon coordinator (backend trait, default-job bootstrap, typed job builders, scheduler handle).
Re-run after code or doc changes. Covered by unit + integration tests below.

## Environment

Match [`.github/workflows/ci.yml`](../.github/workflows/ci.yml):

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-chronon-coordinator
export RUSTFLAGS="-D warnings"
```

Toolchain: stable (same as CI).

## PR CI parity

Required PR job `quality`:

| CI step | Local command |
|--------|----------------|
| Formatting | `cargo fmt --all -- --check` |
| Clippy | `cargo clippy --all-targets --all-features -- -D warnings` |
| Test | `cargo test --all-features` |
| cargo doc | `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features` |

## Unit + integration (CI)

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

### TEST_MAP

| Behavior | Level | Happy | Sad | Notes |
|----------|-------|-------|-----|-------|
| `default_job_schedule_equivalent` / `merge_default_job_schedule_fields` | unit | cron merge preserves `job_id`; manual match; run-once equal | cron/tz/kind/run-once drift | `default_job::tests` |
| `ensure_default_jobs_embedded` (empty inventory) | unit | no-op `Ok` without calling `build_valence` | — | library binary has no `submit!` |
| `Scheduler::from_inventory` / `list_scripts` | integ | call succeeds (names may be empty) | — | inventory-only handle |
| `JobBuilder` cron / manual / run-once | integ + unit | schedule fields + `next_run_at` | invalid cron → `InvalidCron`; missing name/valence → `ParamError` | `tests/integration_test.rs`; unit ParamError |
| `snapshot_actor_json` | integ | serializes valence actor | — | lineage helper |
| `validate_external_job_actor_json` | unit | rejects forged System actor on external upsert | allows Service actor | `job_builder.rs` |
| `snapshot_job_actor_from_valence` | unit | matches `JobBuilder::build` actor snapshot | — | `job_builder.rs` |
| `ScriptScheduler::add` | integ | upserts via `upsert_job_with_valence` | invalid cron before upsert; upsert fail → `Internal`; missing valence → `ParamError` | MemBackend / FailUpsert |
| `typed_job_ref_for_script` / `TypedJobRef::run_now` | integ | resolve + run; params override forwarded | missing job → `JobNotFound`; script mismatch → `ScriptMismatch` | typed param override recorded |
| `ensure_default_jobs_embedded(+skip)` | integ | upserts inventory probe job | upsert deny → `anyhow` with job name; skip omits probe | `tests/default_jobs_contract.rs` |
| `register_default_jobs_embedded(+skip)` | integ | factory + ensure upserts probe; skip omits | factory / ensure / rebuild-after-probe failure logged, no panic, no upsert | fire-and-forget boot helper |

## Notes

- Tests may `unwrap`/`expect`; production paths map failures to typed
  [`ChrononError`](https://docs.rs/chronon-core) / `anyhow` (no ordinary-path unwrap).
  `register_default_jobs_embedded*` intentionally logs and swallows errors for
  fire-and-forget boot.
- Sad-path assertions check typed variants or message content — (stronger than `is_err()` alone).
