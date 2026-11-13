# chronon-coordinator

[![CI](https://github.com/unified-field-dev/chronon-coordinator/actions/workflows/ci.yml/badge.svg)](https://github.com/unified-field-dev/chronon-coordinator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[GitHub](https://github.com/unified-field-dev/chronon-coordinator) · `cargo doc -p chronon-coordinator --open`

Object-safe Chronon job/run admin for hosts: backend trait, Valence-aware job builder (schedule
fluent API from Chronon), typed helpers, and a script-discovery handle over the script registry.

```toml
chronon-coordinator = { git = "https://github.com/unified-field-dev/chronon-coordinator" }
```

```rust,ignore
use chronon_coordinator::{ChrononCoordinatorBackend, JobBuilder, Scheduler};

let scheduler = Scheduler::from_inventory();
let job = JobBuilder::new(/* script handle */)
    .with_valence(/* … */)
    .name("nightly-report")
    .build()?;

let backend: std::sync::Arc<dyn ChrononCoordinatorBackend> = /* … */;
backend.upsert_job_with_valence(&valence, job).await?;
```

## About

- `ChrononCoordinatorBackend` — object-safe job/run/revision **admin** trait (persist, run-now, list; `list_runs_after` pages runs by `RunKey` cursor)
- `JobBuilder` — Valence wrapper over Chronon's preferred `chronon_scheduler::JobBuilder` (schedules)
- `Scheduler` — discovery over the script registry
- `ensure_default_jobs_embedded` / `register_default_jobs_embedded` — inventory bootstrap

## Examples

Canonical teaching path and run commands: [examples/README.md](examples/README.md).

## Verify

See [docs/VERIFICATION.md](docs/VERIFICATION.md) for the test map and gates. Quick check:

```bash
export CARGO_BUILD_JOBS=1
cargo test
```

## License

MIT. See [LICENSE](LICENSE), [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
