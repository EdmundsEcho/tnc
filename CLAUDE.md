# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in
this repository. The outer `../CLAUDE.md` (at the Lucivia platform root) is
also relevant for cross-project context.

## Overview

This is the **TnC analysis workspace** — a Cargo workspace containing the
reusable Rust libraries that implement the Test-and-Control analysis pipeline,
a synthetic-data CLI that exercises them, and a React dashboard that reads
the pipeline's output.

The libraries in here are **consumed by services** (`etl-rs`, eventually
replacing `tnc-py`). Services stay *outside* this workspace and depend on
`tnc-analysis` via `{ path = "../tnc/tnc-analysis" }` (or later git/registry).

## Layout

```
tnc/
  Cargo.toml              ← Cargo workspace (resolver = "2")
  Cargo.lock              ← shared lockfile
  target/                 ← shared build dir (gitignored)

  tnc-analysis/           ← lib: the pipeline itself
  tnc-data-gen/           ← bin: thin CLI + synthetic data generator
  etl-unit-gen/           ← lib: synthetic subjects+measurements generator
  etl-unit/               ← lib: older "unit" semantic model (edition 2024)
  meta-tracing/           ← lib: dep of etl-unit
  propensity-score/       ← lib: logistic regression (+ binning strategies)

  tnc-charts/             ← React app, NOT a Cargo workspace member
```

## Dependency graph

```
tnc-data-gen ──► tnc-analysis ──► etl-unit-gen
                              └─► propensity-score

etl-unit ─────► meta-tracing

(etl-unit is standalone; not yet used by the pipeline)
```

External consumers:

```
etl-rs (service, lives at ../../etl-rs)
  └─► propensity-score    (optional feature)
  └─► tnc-analysis        (future)
```

## Core domain concepts

- **Universe** — subjects whose `(signature_qualities × merged
  writing-profile-deciles)` signature appears in the reached cohort. If no
  cardiac surgeons were reached, that specialty isn't in the universe.
- **Target list** — campaign's planned reach, persisted as `target_list.csv`.
  Standard data input. Reached ⊆ target list.
- **Reached** = `campaign_reach_date IS NOT NULL` (derived column). This is
  the propensity target and the test/control split.
- **L-anchor** — derived from observed `earliest_reach_month` in the data,
  never from the campaign config. `L12M` = the 12 months ending just before
  the campaign actually began.
- **Deciles** are equal-*volume* (not equal-count). `D0` = non-writers;
  `D1..D10` each hold ~1/10 of total positive Rx. D10 is a small set of
  whales, D1 is a long tail. Deciles are exposed per-measurement × window
  (brx/hrx/grx × L03M/L06M/L12M).
- **Decile grouping** (configurable, per-source) collapses arbitrary decile
  subsets into one merged bucket. Default applies globally; overrides
  per `(measurement, window)`.
- **Pipeline phases** (type-state `Analysis<S>`):
  ```
  Raw ──► Windowed ──► Scored ──► Matched ──► Estimated
  ```
  Validation gates at `Windowed → Scored` (inputs) and `Matched → Estimated`
  (pre-period DiD placebo). Each transition consumes the prior state.

## Build & run

### The full pipeline (synthetic data → analysis → artifacts)

```bash
cd tnc-data-gen

# Default seed (fixed in etl-schema.toml — reproducible)
cargo run --release -- etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output

# Override the seed for per-run variation
cargo run --release -- --seed 137 etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
cargo run --release -- --seed random etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
```

Outputs go to `tnc-data-gen/output/` — `subjects.csv`, `windows.csv`,
`matches.csv`, `timeseries.csv`, `summary.json`, `ancova.json`,
`config.json`, `cfg-parsed.json`, `target_list.csv`, plus copies of the
three TOML configs.

### Sync to the dashboard

```bash
cp output/*.csv output/*.json output/*.toml ../tnc-charts/public/data/
```

### Dashboard

```bash
cd tnc-charts
npm install                   # first time
npm run dev                   # Vite dev server; hot-reloads on data changes
npm run build                 # production build to dist/
```

**Use `npm`, not `yarn`** — this workspace doesn't use yarn. (The sibling
`../../ui_v2` uses yarn per the outer CLAUDE.md; this one does not.)

### Workspace-wide Rust commands

```bash
cargo check                   # all members
cargo check -p tnc-analysis   # just the library
cargo check -p tnc-data-gen   # just the CLI
cargo test --workspace        # all tests
cargo build --release -p tnc-data-gen
```

**Prefer `cargo check` to `cargo build` during iteration** — it's meaningfully
faster.

## Configuration files

Three TOMLs, loaded together by `tnc-analysis::config::load(...)`:

- **`etl-schema.toml`** — schema + generator spec (subject count, time range,
  seed). Shape consumed by `etl-unit-gen::generate`.
- **`campaign-cfg.toml`** — real calendar time: `study_start_date`, reach
  window, min_pre / max_post / test_period, target/reach fractions, lift
  injection spec. Describes the campaign mechanics.
- **`tnc-analysis-cfg.toml`** — derived field definitions, eligibility,
  decile grouping, universe, propensity spec, matching gates + scoring,
  ANCOVA spec, input/match validation thresholds, autotune stub.

Shipping copies of all three into `output/` is deliberate — the dashboard's
ConfigViewer renders them alongside the data.

## Testing the analysis side independently

`tnc-analysis` is a library. A service (or a test) constructs inputs and
walks the phases:

```rust
use tnc_analysis::{Analysis, Config, Treatment};

let cfg: Config = tnc_analysis::config::load(schema, campaign, analysis)?;
let data: etl_unit_gen::GeneratedData = ...;    // or built from real inputs
let treatment: Treatment = ...;                  // planned + actual reach dates

let result = Analysis::new(cfg, data, treatment)
    .compute_windows()?
    .fit_propensity(&y, &cat_predictors, &num_predictors)?
    .run_matching()?
    .run_ancova()?;

let ancova = result.ancova();
```

`tnc-data-gen::synth` fabricates `Treatment` for synthetic runs; real-data
services populate it from their own sources.

## Sticky conventions (don't accidentally undo these)

- **Every Rx aggregate report must show variance** (SD / SE / ±1σ bands)
  alongside the mean — not just the point estimate.
- **Rx deciles are equal-volume**, with `D0` = non-writers (zero).
- **`hc_match`** is gone. The target list lives only in `target_list.csv`.
  Propensity target = `reached`.
- **`allow_zero_zero`** on volume gates is gone. D0 is just a decile.
- **`ancova.treatment`** is implicit from matched-pair side (test=1, control=0)
  — don't add the field back.
- **Seed determinism** is by design. `--seed <N|random>` is the only
  supported way to re-roll.
- **Service boundaries**: this workspace is libraries. Don't add a web
  framework or an HTTP handler here. Services live at `../../etl-rs/`,
  `../../tnc-py/`, etc.

## Deployment

This repository itself doesn't deploy — it produces libraries. Services
that depend on `tnc-analysis` have their own Dockerfiles and k8s manifests
under `../../deployment/k8s-prod/` and `../../deployment/k8s-dev/`.

## Notes

- Polars version mismatch: `etl-unit` uses `0.51`, the rest use `0.46`. Cargo
  resolves both; unifying via `[workspace.dependencies]` is a possible cleanup
  but not blocking.
- Editions are mixed (`etl-unit` and `meta-tracing` on 2024, the rest on 2021).
  Intentional; workspace members can differ.
- The React app uses `react-chartjs-2` + Chart.js 4; custom components live
  in `tnc-charts/src/charts/`.
- Memory for this project (cross-session context) is at
  `~/.claude/projects/-home-users-ecape-Programming-Local-etl/memory/` —
  `project_tnc_analysis_refactor.md` is the current summary.
