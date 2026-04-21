import React from 'react';
import { PipelineFlow, StatCard } from '../charts';

export default function OverviewView({ data }) {
  const { summary, config, ancova } = data;

  const stages = [
    { label: 'Universe',          value: summary.universe,          accent: '#6b7280' },
    { label: 'Target List',       value: summary.target_list,       accent: '#3b82f6', relativeTo: 0 },
    { label: 'Reached',           value: summary.reached,           accent: '#8b5cf6', relativeTo: 1 },
    { label: 'Eligible Controls', value: summary.eligible_controls, accent: '#06b6d4', relativeTo: 0 },
    { label: 'Matched Pairs',     value: summary.matched_pairs,     accent: '#10b981', relativeTo: 2 },
  ];

  const longest = [...ancova].sort((a, b) => {
    // Prefer POST06M > POST04M > POST02M as the "headline" horizon
    const order = { POST06M: 3, POST04M: 2, POST02M: 1 };
    return (order[b.outcome_name.split('_')[1]] ?? 0) - (order[a.outcome_name.split('_')[1]] ?? 0);
  })[0];

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Pipeline Overview</h2>
      <PipelineFlow stages={stages} />

      <div style={{ marginTop: 32 }}>
        <h3 style={{ fontSize: 14, color: '#6b7280' }}>Configuration</h3>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <StatCard
            label="Injected Lift"
            value={`+${(config.injected_lift_pct * 100).toFixed(0)}%`}
            subtext={`plateau at ${config.ramp_months} mo`}
            accent="#f59e0b"
          />
          <StatCard
            label="Reach Window"
            value={`${config.reach_window.duration_months} mo`}
            subtext={`from ${config.reach_window.start_date}`}
            accent="#8b5cf6"
          />
          <StatCard
            label="Study Horizon"
            value={`${config.total_span_months} mo`}
            subtext={`from ${config.study_start_date}`}
            accent="#06b6d4"
          />
        </div>
      </div>

      {longest && (
        <div style={{ marginTop: 32 }}>
          <h3 style={{ fontSize: 14, color: '#6b7280' }}>Headline Result — {longest.outcome_name}</h3>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            <StatCard
              label="Recovered Lift"
              value={`${longest.lift_pct >= 0 ? '+' : ''}${longest.lift_pct.toFixed(1)}%`}
              subtext={`t = ${longest.t_stat.toFixed(2)}`}
              accent="#10b981"
            />
            <StatCard
              label="Test Mean"
              value={longest.test_mean.toFixed(2)}
              subtext={`n = ${longest.n_rows / 2}`}
              accent="#3b82f6"
            />
            <StatCard
              label="Control Mean"
              value={longest.control_mean.toFixed(2)}
              subtext="matched 1:1"
              accent="#6b7280"
            />
            <StatCard
              label="Treatment β"
              value={longest.beta_treatment.toFixed(2)}
              subtext={`SE ${longest.se_treatment.toFixed(3)}`}
              accent="#f59e0b"
            />
          </div>
        </div>
      )}
    </div>
  );
}
