//! Distribution enum with TOML-friendly (de)serialization + sampling.

use rand::Rng;
use rand_distr::{Distribution as RandDistribution, Gamma, LogNormal, Normal, Poisson};
use serde::{Deserialize, Serialize};

/// A distribution over scalar values.
///
/// Parameterized so each variant carries all its shape knobs. Sampling is
/// handled by the [`sample`](Distribution::sample) method.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    Constant { value: f64 },
    Uniform { min: f64, max: f64 },
    Normal { mean: f64, std: f64 },
    LogNormal { mu: f64, sigma: f64 },
    Poisson { lambda: f64 },
    /// Negative binomial parameterized by mean + dispersion.
    /// Higher dispersion → more variance above the Poisson baseline.
    /// dispersion = 0 collapses to Poisson.
    NegativeBinomial { mean: f64, dispersion: f64 },
    Categorical(Vec<CategoricalEntry>),
    UniformChoice(Vec<String>),
    Bernoulli { p: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoricalEntry {
    pub value: String,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NoiseSpec {
    /// Coefficient of variation of multiplicative noise: observed = value * (1 + N(0, cv))
    #[serde(default)]
    pub cv: f64,
}

impl Distribution {
    /// Mean of the distribution (for distributions with a defined mean).
    pub fn mean(&self) -> f64 {
        match self {
            Distribution::Constant { value } => *value,
            Distribution::Uniform { min, max } => (min + max) / 2.0,
            Distribution::Normal { mean, .. } => *mean,
            Distribution::LogNormal { mu, sigma } => (mu + sigma * sigma / 2.0).exp(),
            Distribution::Poisson { lambda } => *lambda,
            Distribution::NegativeBinomial { mean, .. } => *mean,
            Distribution::Bernoulli { p } => *p,
            _ => 0.0,
        }
    }

    /// Sample a numeric value. Applies mean_scale (multiplies the distribution's mean).
    pub fn sample_numeric<R: Rng>(&self, rng: &mut R, mean_scale: f64) -> f64 {
        match self {
            Distribution::Constant { value } => *value * mean_scale,
            Distribution::Uniform { min, max } => {
                let x = rng.gen_range(*min..=*max);
                x * mean_scale
            }
            Distribution::Normal { mean, std } => {
                let d = Normal::new(mean * mean_scale, *std).unwrap();
                d.sample(rng)
            }
            Distribution::LogNormal { mu, sigma } => {
                let d = LogNormal::new(*mu, *sigma).unwrap();
                d.sample(rng) * mean_scale
            }
            Distribution::Poisson { lambda } => {
                let d = Poisson::new(lambda * mean_scale).unwrap_or(Poisson::new(1e-6).unwrap());
                d.sample(rng) as f64
            }
            Distribution::NegativeBinomial { mean, dispersion } => {
                // NegBinom(mean, dispersion) via Gamma-Poisson mixture.
                // shape = 1/dispersion, scale = mean * dispersion
                let m = mean * mean_scale;
                if *dispersion <= 0.0 {
                    let d = Poisson::new(m.max(1e-6)).unwrap();
                    return d.sample(rng) as f64;
                }
                let shape = 1.0 / dispersion;
                let scale = m * dispersion;
                let gamma = Gamma::new(shape, scale).unwrap();
                let rate = gamma.sample(rng).max(1e-6);
                let poisson = Poisson::new(rate).unwrap();
                poisson.sample(rng) as f64
            }
            Distribution::Bernoulli { p } => {
                if rng.gen::<f64>() < *p {
                    1.0
                } else {
                    0.0
                }
            }
            Distribution::Categorical(_) | Distribution::UniformChoice(_) => {
                // Not meaningful for numeric sampling — return 0
                0.0
            }
        }
    }

    /// Sample a categorical string value.
    pub fn sample_string<R: Rng>(&self, rng: &mut R) -> String {
        match self {
            Distribution::Categorical(values) => {
                let total: f64 = values.iter().map(|e| e.weight).sum();
                let mut r = rng.gen::<f64>() * total;
                for entry in values {
                    r -= entry.weight;
                    if r <= 0.0 {
                        return entry.value.clone();
                    }
                }
                values.last().map(|e| e.value.clone()).unwrap_or_default()
            }
            Distribution::UniformChoice(values) => {
                let i = rng.gen_range(0..values.len());
                values[i].clone()
            }
            _ => panic!("sample_string called on a non-categorical distribution"),
        }
    }
}
