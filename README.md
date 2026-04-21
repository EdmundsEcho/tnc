# TnC Analysis

A Cargo workspace for the Test-and-Control analysis pipeline, a CLI that
drives it on synthetic data, and a React dashboard that visualizes the
results.

```
Raw subjects + measurements
        │
        ▼
  UNIVERSE (signature filter)
        │
        ▼
  MICRO-POOL (decile + propensity gates)
        │
        ▼
  MATCHED PAIRS (1:1 test↔control)
        │
        ▼
  ANCOVA (recovered lift per outcome window)
```

## Quick start

There's a nushell wrapper at `./run.nu` with shortcuts for the common tasks:

```bash
./run.nu                         # show available subcommands
./run.nu fresh                   # generate + run analysis + sync to dashboard
./run.nu fresh --seed random     # same, with a new RNG draw
./run.nu dev                     # start the dashboard (after `npm install` once)
```

That's the 30-second version. Everything below is the long form — what each
step does, what knobs to tune, what artifacts to inspect.

### Raw commands (if you prefer them)

```bash
# 1. Generate data + run the analysis (produces artifacts in output/)
cd tnc-data-gen
cargo run --release -- etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output

# 2. Copy artifacts to the dashboard
cp output/*.csv output/*.json output/*.toml ../tnc-charts/public/data/

# 3. Run the dashboard (first time: npm install)
cd ../tnc-charts
npm install           # once
npm run dev           # open the URL Vite prints
```

That gives you a fully-reproducible baseline run using the seed pinned in
`etl-schema.toml`.

---

## 1. Configure the pipeline

Three TOML files drive everything. They live in `tnc-data-gen/`:

| File | What it describes |
|---|---|
| `etl-schema.toml` | Synthetic-data shape — subject count, time horizon, RNG seed, field definitions |
| `campaign-cfg.toml` | Real-calendar mechanics — study start date, reach window, min_pre / max_post / test_period, planned vs. actual reach fractions, lift injection |
| `tnc-analysis-cfg.toml` | Analysis rules — derived fields, decile grouping, universe signature, propensity predictors, matching gates, ANCOVA spec, validation thresholds |

### etl-schema.toml (what data gets generated)

```toml
[gen]
subject_count = 10000         # universe size
seed          = 42            # override with --seed at the CLI

[gen.time_range]
first_month = 1
last_month  = 24              # 24 months of observations
```

### campaign-cfg.toml (campaign mechanics)

```toml
[campaign]
study_start_date   = "2024-01-01"
total_span_months  = 24
min_pre_months     = 12                  # every reached subject has ≥ 12mo pre
max_post_months    = 6
test_period_months = 1                   # lag window, excluded from analysis

target_list_fraction        = 0.20       # 20% of universe is on the target list
reached_fraction_of_targets = 0.85       # 85% of targets actually get reached

[campaign.reach_window]
start_date      = "2025-02-01"
duration_months = 3

[lift]                                   # synthetic-only signal we can recover
applies_to  = "brx"
ramp_months = 3
max_pct     = 0.15                       # +15% steady-state lift
```

The reach window must fit inside
`[study_start + min_pre, study_end − max_post − test_period]` — the loader
validates this on startup.

### tnc-analysis-cfg.toml (how the analysis works)

Four knobs you'll touch most:

**Universe signature** — subjects whose (qualities × merged-decile) signature
appears in the reached cohort:

```toml
[universe]
signature_qualities = ["specialty", "state"]
signature_writing_profiles = [
  { measurement = "brx", window = "L12M" },
  { measurement = "hrx", window = "L12M" },
  { measurement = "grx", window = "L12M" },
]
```

**Decile grouping** — collapse sparse decile buckets before comparing:

```toml
[decile_grouping]
default = { groups = [] }                # no merges by default

[[decile_grouping.overrides]]            # brx whales (D9+D10) are sparse
source = { measurement = "brx", window = "L12M" }
groups = [[9, 10]]
```

**Matching gates** — how tight the pair has to be on pre-period Rx volume:

```toml
[[matching.volume_gates]]
source = { measurement = "brx", window = "L12M" }
within_deciles = 0                       # 0 = same merged bucket; 1 = neighbor
```

**Validation thresholds** — pipeline halts if any gate fails:

```toml
[input_validation]
min_reached                 = 50         # campaign must touch ≥ N in-universe
min_eligible_control_pool   = 200
min_test_to_control_ratio   = 0.05

[match_validation]
# Pre-period DiD placebo: tests and controls should look identical before
# the campaign started. Tolerance in raw units per measurement.
max_did_pre = [
  { measurement = "brx", window = "PRE06M", max =  20.0 },
  { measurement = "hrx", window = "PRE06M", max =  60.0 },
  { measurement = "grx", window = "PRE06M", max = 200.0 },
]
```

See `tnc/CLAUDE.md` for the design rationale behind each of these.

---

## 2. Generate data + run the analysis

```bash
cd tnc-data-gen
cargo run --release -- etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
```

The CLI will walk all five pipeline phases and print progress. A successful
run ends with an ANCOVA table:

```
outcome               N     β_treat         se        t    test_m  control_m    lift%
─────────────────────────────────────────────────────────────────────────────────────
brx_POST02M         460       3.609      0.627     5.76    31.592    24.909    +26.8%
brx_POST04M         460       8.694      0.920     9.45    64.102    49.326    +30.0%
brx_POST06M         460      12.278      1.204    10.20    96.817    75.357    +28.5%

INJECTED LIFT:  +15%   (plateau at 3 months post-reach)
```

### Overriding the seed

The default seed in `etl-schema.toml` is fixed for reproducibility. Change it
per run without editing the file:

```bash
./run.nu gen --seed 137           # via the wrapper
./run.nu gen --seed random

# Or raw
cd tnc-data-gen
cargo run --release -- --seed 137 etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
cargo run --release -- --seed random etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
```

### What gets written

`tnc-data-gen/output/`:

| File | Contents |
|---|---|
| `subjects.csv` | One row per subject — qualities, `campaign_reach_date`, `reached`, `in_universe`, `propensity_score`, `propensity_decile` |
| `windows.csv` | Wide frame — one row per subject, columns for every `{measurement}_{window}` and `{measurement}_{window}_decile` |
| `matches.csv` | `test_npi, control_npi` — the 1:1 matched pairs |
| `timeseries.csv` | Per-month mean/SD/count per measurement per group (test/control/universe) — for the dashboard's longitudinal view |
| `target_list.csv` | Target list with planned reach dates (standard input artifact) |
| `summary.json` | Pipeline counts + waterfall + DiD placebo report |
| `ancova.json` | ANCOVA results per outcome window |
| `config.json` | Compact summary for the dashboard |
| `cfg-parsed.json` | Full parsed contents of all three TOMLs |
| `etl-schema.toml` + `campaign-cfg.toml` + `tnc-analysis-cfg.toml` | Verbatim copies of the configs this run used |

---

## 3. Display the dashboard

```bash
# First time setup
cd tnc-charts
npm install

# Development (auto-reloads on data changes)
npm run dev

# Production build (outputs to tnc-charts/dist/)
npm run build
```

The dashboard expects its data at `tnc-charts/public/data/`. The generator
writes to `tnc-data-gen/output/`, so after every run:

```bash
./run.nu sync
# or raw:
cp tnc-data-gen/output/*.csv tnc-data-gen/output/*.json tnc-data-gen/output/*.toml tnc-charts/public/data/
```

Then just refresh the browser — Vite's dev server hot-reloads on file
changes, no restart needed.

### Dashboard views

| View | What it shows |
|---|---|
| **Overview** | Pipeline flow + headline numbers |
| **Config** | The three TOML files, structured and annotated |
| **Universe** | Specialty/state distribution + subjects-per-decile for brx/hrx/grx |
| **Assignment** | Population funnel by brx_L12M decile (Writing Universe → Campaign Universe → Reach + Control Pool) |
| **Propensity** | Reached vs. not-reached distribution by propensity bucket + decile table |
| **Matching** | Subject funnel waterfall, match rate by specialty/decile, pre-period DiD placebo table |
| **Comparison** | Longitudinal test-vs-control line chart with ±1σ bands, pre/post gap stat cards, DiD panel |
| **Results** | ANCOVA regression summary per outcome window |

---

## 4. Troubleshooting

**"validation failed: pre-period DiD on X exceeds max Y"** — matching is
leaving a pre-period gap larger than your tolerance. Either tighten gates
(`within_deciles = 0` across more measurements in `tnc-analysis-cfg.toml`)
or raise the tolerance in `[match_validation].max_did_pre`. The Matching
view's placebo table shows the exact gap per measurement.

**"reached-in-universe count N < min_reached M"** — your signature filter
is too strict (universe too small) or the synthetic config produced too few
reached subjects. Check `[universe]` signature fields vs. the specialties
the campaign actually reached.

**Match rate is 13%, I expected 50%** — strict decile gates across three
measurements × three windows can be very restrictive. Relax by setting
some `within_deciles = 1`. Low match rate with high-quality matches is
preferred over high match rate with weak matches; the placebo validation
backs this up.

**Dashboard looks stale after a run** — did you copy the artifacts? The
sync step is manual:

```bash
cp tnc-data-gen/output/*.csv tnc-data-gen/output/*.json tnc-data-gen/output/*.toml tnc-charts/public/data/
```

---

## 5. Project structure

```
tnc-analysis/      lib   the pipeline (Analysis<S> type-state)
tnc-data-gen/      bin   thin CLI + synthetic data generation
etl-unit-gen/      lib   synthetic subjects+measurements
etl-unit/          lib   older "unit" semantic model
meta-tracing/      lib   dep of etl-unit
propensity-score/  lib   logistic regression + binning

tnc-charts/              React dashboard (Vite, Chart.js)
```

Services (`etl-rs`, `tnc-py`, etc.) live outside this workspace and depend
on `tnc-analysis` via `{ path = "../tnc/tnc-analysis" }`.

## License

Proprietary.
