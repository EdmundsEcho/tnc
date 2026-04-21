import React from 'react';
import { GroupedBarChart, StatCard } from '../charts';

export default function ResultsView({ data }) {
  const { ancova, config } = data;

  const injected = config.injected_lift_pct * 100;

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>ANCOVA Results</h2>
      <p style={{ color: '#6b7280', marginTop: 0 }}>
        Recovered lift estimates at each post-campaign horizon. OLS regression of outcome
        on treatment indicator with one-hot quality covariates and pre-period baseline windows.
      </p>

      <div style={{ display: 'flex', gap: 12, marginBottom: 24 }}>
        <StatCard
          label="Injected Lift"
          value={`+${injected.toFixed(0)}%`}
          subtext={`ramp ${config.ramp_months} mo`}
          accent="#f59e0b"
        />
      </div>

      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, padding: 16, marginBottom: 24 }}>
        <h3 style={{ fontSize: 14, color: '#374151', marginTop: 0 }}>Recovered Lift by Horizon</h3>
        <GroupedBarChart
          labels={ancova.map(r => r.outcome_name)}
          datasets={[
            {
              label: 'Recovered lift %',
              data: ancova.map(r => r.lift_pct),
              color: 'rgba(16, 185, 129, 0.85)',
            },
            {
              label: 'Injected lift %',
              data: ancova.map(() => injected),
              color: 'rgba(245, 158, 11, 0.35)',
            },
          ]}
          xAxisLabel="Outcome window"
          yAxisLabel="Lift (%)"
          height={280}
        />
      </div>

      <h3 style={{ fontSize: 14, color: '#374151' }}>Regression summary</h3>
      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, overflow: 'hidden' }}>
        <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: 'monospace', fontSize: 13 }}>
          <thead>
            <tr style={{ background: '#f9fafb' }}>
              <th style={th}>Outcome</th>
              <th style={th}>N</th>
              <th style={th}>β (treatment)</th>
              <th style={th}>SE</th>
              <th style={th}>t</th>
              <th style={th}>Test mean ± SD</th>
              <th style={th}>Control mean ± SD</th>
              <th style={th}>Lift %</th>
            </tr>
          </thead>
          <tbody>
            {ancova.map((r, i) => (
              <tr key={r.outcome_name} style={{ background: i % 2 ? '#fff' : '#fafafa' }}>
                <td style={td}>{r.outcome_name}</td>
                <td style={tdNum}>{r.n_rows}</td>
                <td style={tdNum}>{r.beta_treatment.toFixed(3)}</td>
                <td style={tdNum}>{r.se_treatment.toFixed(3)}</td>
                <td style={{ ...tdNum, fontWeight: 700 }}>{r.t_stat.toFixed(2)}</td>
                <td style={tdNum}>
                  {r.test_mean.toFixed(2)}
                  {r.test_sd != null && (
                    <span style={{ color: '#6b7280', marginLeft: 4 }}>
                      ± {r.test_sd.toFixed(2)}
                    </span>
                  )}
                </td>
                <td style={tdNum}>
                  {r.control_mean.toFixed(2)}
                  {r.control_sd != null && (
                    <span style={{ color: '#6b7280', marginLeft: 4 }}>
                      ± {r.control_sd.toFixed(2)}
                    </span>
                  )}
                </td>
                <td style={{ ...tdNum, color: r.lift_pct > 0 ? '#10b981' : '#ef4444', fontWeight: 700 }}>
                  {r.lift_pct >= 0 ? '+' : ''}{r.lift_pct.toFixed(1)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 16, fontSize: 12, color: '#6b7280' }}>
        <strong>Interpretation:</strong> The recovered lift grows with the outcome window
        (POST02M &lt; POST04M &lt; POST06M) because longer windows capture more of the post-ramp plateau.
        Recovered {'<'} injected because the cumulative window averages across the 3-month ramp.
      </div>
    </div>
  );
}

const th = { padding: '10px 16px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e5e7eb' };
const td = { padding: '10px 16px', borderBottom: '1px solid #f3f4f6' };
const tdNum = { ...td, textAlign: 'right' };
