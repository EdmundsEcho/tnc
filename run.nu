#!/usr/bin/env nu
# TnC workspace helpers. Run from `etl/tnc/`.
# See README.md for the long-form equivalent of each task.

def main [] {
  print "Usage: ./run.nu <subcommand> [options]"
  print ""
  print "Subcommands:"
  print "  gen [--seed <N|random>]     Generate synthetic data + run analysis"
  print "  sync                         Copy generator output to the dashboard"
  print "  fresh [--seed <N|random>]   gen + sync (most common workflow)"
  print "  dev                          Start dashboard dev server (Vite)"
  print "  build                        Build dashboard for production"
  print "  check                        cargo check --workspace"
  print ""
  print "Examples:"
  print "  ./run.nu fresh --seed random"
  print "  ./run.nu gen  --seed 137"
  print "  ./run.nu dev"
}

# ── gen ────────────────────────────────────────────────────────
def "main gen" [
  --seed (-s): string   # N, or the literal string "random"
] {
  cd tnc-data-gen
  if $seed == null {
    cargo run --release -- etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
  } else {
    cargo run --release -- --seed $seed etl-schema.toml campaign-cfg.toml tnc-analysis-cfg.toml output
  }
}

# ── sync ───────────────────────────────────────────────────────
def "main sync" [] {
  let src = "tnc-data-gen/output"
  let dst = "tnc-charts/public/data"
  if not ($src | path exists) {
    error make { msg: $"($src) doesn't exist — run `./run.nu gen` first" }
  }
  mkdir $dst
  for ext in [csv json toml] {
    let files = (glob $"($src)/*.($ext)")
    if (($files | length) > 0) {
      cp ...$files $dst
    }
  }
  print $"synced ($src) → ($dst)"
}

# ── fresh (gen + sync) ─────────────────────────────────────────
def "main fresh" [--seed (-s): string] {
  if $seed == null {
    main gen
  } else {
    main gen --seed $seed
  }
  main sync
}

# ── dev / build ────────────────────────────────────────────────
def "main dev" [] {
  cd tnc-charts
  npm run dev
}

def "main build" [] {
  cd tnc-charts
  npm run build
}

# ── check ──────────────────────────────────────────────────────
def "main check" [] {
  cargo check --workspace
}
