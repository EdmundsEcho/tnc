//! Treatment data — planned + actual reach dates + called_on flags per subject.
//!
//! In a synthetic run, `tnc-data-gen` *generates* a Treatment and passes it to
//! `Analysis::new`. In a real-data run, a service (e.g. `etl-rs`) reads the
//! same fields directly from its input sources (target-list CSV + campaign
//! execution log) and constructs the Treatment itself.

#[derive(Debug, Clone)]
pub struct Treatment {
    /// Month index (1-based) of each subject's PLANNED reach date if they are
    /// on the target list, else None. This is what the campaign team scheduled;
    /// it drives the `target_list.csv` artifact.
    pub planned_reach_date: Vec<Option<i64>>,
    /// Month index (1-based) of each subject's ACTUAL reach date, or None if
    /// not reached (either off-list or on-list but missed). `Some(_)` here is
    /// the derived "reached" flag.
    pub campaign_reach_date: Vec<Option<i64>>,
    /// 0 | 1 — received an in-person sales visit.
    pub called_on: Vec<i64>,
}

impl Treatment {
    /// Earliest reach date across all reached subjects (Phase-1 virtual
    /// anchor for controls, and the L-window anchor).
    pub fn earliest_reach_month(&self) -> Option<i64> {
        self.campaign_reach_date.iter().filter_map(|&m| m).min()
    }

    /// 0 / 1 per subject — derived "reached" flag (propensity target).
    pub fn reached_flags(&self) -> Vec<i64> {
        self.campaign_reach_date
            .iter()
            .map(|f| if f.is_some() { 1 } else { 0 })
            .collect()
    }
}
