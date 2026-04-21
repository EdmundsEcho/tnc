import React from 'react';
import { StatCard, GroupedBarChart } from '../charts';
import { mergeNote } from '../util/deciles';

export default function MatchingView({ data }) {
  const { summary, matches, subjects, windows, decileGrouping } = data;
  const hrxMerge = mergeNote(decileGrouping, 'hrx', 'L12M');

  const idx = new Map(windows.map(r => [r.npi, r]));

  const testIds = new Set(matches.map(m => m.test_npi));
  const controlIds = new Set(matches.map(m => m.control_npi));

  const testSubjects = subjects.filter(s => testIds.has(s.npi));
  const controlSubjects = subjects.filter(s => controlIds.has(s.npi));

  const matchRate = summary.matched_pairs /
    (summary.matched_pairs + summary.unmatched_tests) * 100;

  // Match rate per specialty: of the reached subjects in each specialty
  // (those who entered matching), what fraction got paired with a control?
  //   numerator   = matched tests in that specialty (test_ids ∩ specialty)
  //   denominator = in-universe AND reached subjects in that specialty
  //                 (same population that matching.rs attempts to pair)
  const attemptedTests = subjects.filter(
    s => Number(s.in_universe) === 1 && Number(s.reached) === 1,
  );
  const attemptedSpecs = [...new Set(attemptedTests.map(s => s.specialty))].sort();
  const specMatchRate = attemptedSpecs.map(sp => {
    const attempted = attemptedTests.filter(s => s.specialty === sp).length;
    const matched = testSubjects.filter(s => s.specialty === sp).length;
    return {
      specialty: sp,
      attempted,
      matched,
      rate: attempted > 0 ? (matched / attempted) * 100 : 0,
    };
  });

  // Match rate per hrx_L12M decile (equal-volume: D0 = non-writer, D10 = whale).
  //   numerator   = matched tests whose hrx_L12M decile = D
  //   denominator = in-universe reached subjects whose hrx_L12M decile = D
  const decileLabels = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10'];
  const countByDecile = (rows) => {
    const counts = new Array(11).fill(0);
    for (const s of rows) {
      const d = Number(idx.get(s.npi)?.hrx_L12M_decile);
      if (d >= 0 && d <= 10) counts[d] += 1;
    }
    return counts;
  };
  const attemptedByDecile = countByDecile(attemptedTests);
  const matchedByDecile   = countByDecile(testSubjects);
  const hrxDecileMatchRate = attemptedByDecile.map((att, i) =>
    att > 0 ? (matchedByDecile[i] / att) * 100 : 0,
  );

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Matching</h2>
      <p style={{ color: '#6b7280', marginTop: 0 }}>
        Pool → micro-pool → 1:1 control group. Matching is categorical (state, spec_rollup,
        sf_p1) + volume gates on hrx_L12M and grx_L12M, then scored on best-match per window.
      </p>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 24 }}>
        <StatCard label="Control pool"        value={summary.eligible_controls} accent="#06b6d4" />
        <StatCard label="Mean micro-pool"     value={summary.mean_micropool_size.toFixed(1)} accent="#8b5cf6"
          subtext="controls per test" />
        <StatCard label="Matched pairs"       value={summary.matched_pairs} accent="#10b981" />
        <StatCard label="Unmatched tests"     value={summary.unmatched_tests} accent="#f59e0b" />
        <StatCard label="Match rate"          value={`${matchRate.toFixed(1)}%`} accent="#10b981" />
      </div>

      {summary.waterfall && (
        <>
          <h3 style={{ fontSize: 14, color: '#374151', marginTop: 24 }}>
            Subject funnel
          </h3>
          <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
            Each stage narrows the population: <strong>All subjects</strong> are the whole universe,
            <strong> Universe</strong> applies the reached-cohort signature filter (qualities ×
            writing-profile deciles), <strong>Eligible for matching</strong> adds the
            brx/hrx volume eligibility cuts, and <strong>Matched</strong> counts the 1:1
            test+control pairs (matched_pairs × 2).
          </p>
          <WaterfallChart waterfall={summary.waterfall} />
        </>
      )}

      {summary.did_report && summary.did_report.length > 0 && (
        <>
          <h3 style={{ fontSize: 14, color: '#374151', marginTop: 24 }}>
            Match-quality placebo (pre-period DiD)
          </h3>
          <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
            If matching worked, test and control groups should look <em>identical</em>
            before the campaign starts. For each configured pre-period window, we compare
            the group-mean difference against a tolerance. A row marked red means tests
            and controls already diverged — the lift you'll see in ANCOVA is mixing
            real campaign effect with pre-existing imbalance.
          </p>
          <DidReport rows={summary.did_report} />
        </>
      )}

      <h3 style={{ fontSize: 14, color: '#374151', marginTop: 24 }}>
        Match rate per brx_L12M decile
      </h3>
      <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
        Deciles are by equal Rx volume (D0 = non-writers, D10 = top-volume writers —
        each of D1..D10 holds ~1/10 of total branded Rx). A low match rate in the
        top deciles typically means the tail of high-volume prescribers is sparse.
      </p>
      <DecileMatchRateTable summary={summary} />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginTop: 32 }}>
        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>Match rate by specialty</h3>
          <GroupedBarChart
            labels={specMatchRate.map(r => r.specialty)}
            datasets={[
              { label: '% matched',
                data: specMatchRate.map(r => Number(r.rate.toFixed(1))),
                color: 'rgba(16, 185, 129, 0.8)' },
            ]}
            xAxisLabel="Specialty"
            yAxisLabel="% of reached subjects with a matched control"
            height={300}
          />
          <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 4 }}>
            Denominator = in-universe reached subjects attempted for matching;
            numerator = those who found a control.
          </div>
        </div>

        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>
            Match rate by hrx_L12M decile
          </h3>
          <GroupedBarChart
            labels={decileLabels}
            datasets={[
              { label: '% matched',
                data: hrxDecileMatchRate.map(r => Number(r.toFixed(1))),
                color: 'rgba(16, 185, 129, 0.8)' },
            ]}
            xAxisLabel="hrx_L12M decile (D0 = non-writer, D10 = whale)"
            yAxisLabel="% of reached subjects with a matched control"
            height={300}
          />
          <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 4 }}>
            Low rates in the extreme deciles usually mean D0 (non-writers) and D10 (whales)
            are too sparse to find an exact-bucket partner.
          </div>
          {hrxMerge && (
            <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 2 }}>
              <strong>Decile grouping:</strong> {hrxMerge}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function DecileMatchRateTable({ summary }) {
  const attempts = summary.test_attempts_by_decile || new Array(11).fill(0);
  const matches  = summary.test_matches_by_decile  || new Array(11).fill(0);
  const totalA = attempts.reduce((a, b) => a + b, 0);
  const totalM = matches.reduce((a, b) => a + b, 0);
  // D0 = non-writers, D1..D10 = equal-volume positive-writer deciles
  const headers = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10','Total'];
  const rowFmt = (vals, total) => [...vals, total];

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, overflow: 'hidden' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: 'system-ui', fontSize: 13 }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            <th style={th}>Metric</th>
            {headers.map(h => <th key={h} style={{ ...th, textAlign: 'right' }}>{h}</th>)}
          </tr>
        </thead>
        <tbody>
          <tr>
            <td style={{ ...td, borderLeft: '3px solid #3b82f6' }}>Tests attempted</td>
            {rowFmt(attempts, totalA).map((v, i) => (
              <td key={i} style={tdNum}>{v}</td>
            ))}
          </tr>
          <tr>
            <td style={{ ...td, borderLeft: '3px solid #10b981' }}>Tests matched</td>
            {rowFmt(matches, totalM).map((v, i) => (
              <td key={i} style={tdNum}>{v}</td>
            ))}
          </tr>
          <tr style={{ fontWeight: 600 }}>
            <td style={{ ...td, borderLeft: '3px solid #111827' }}>Match rate</td>
            {attempts.map((a, i) => {
              const m = matches[i];
              const pct = a > 0 ? (m / a * 100).toFixed(0) + '%' : '—';
              return (
                <td key={i} style={{ ...tdNum, color: a > 0 && m / a < 0.5 ? '#b91c1c' : '#111827' }}>
                  {pct}
                </td>
              );
            })}
            <td style={tdNum}>
              {totalA > 0 ? (totalM / totalA * 100).toFixed(0) + '%' : '—'}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

function DidReport({ rows }) {
  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, overflow: 'hidden' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: 'system-ui', fontSize: 13 }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            <th style={th}>Window</th>
            <th style={{ ...th, textAlign: 'right' }}>Test mean</th>
            <th style={{ ...th, textAlign: 'right' }}>Control mean</th>
            <th style={{ ...th, textAlign: 'right' }}>|DiD|</th>
            <th style={{ ...th, textAlign: 'right' }}>Tolerance</th>
            <th style={{ ...th, textAlign: 'right' }}>Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={{ background: i % 2 ? '#fff' : '#fafafa' }}>
              <td style={{ ...td, borderLeft: `3px solid ${r.passed ? '#10b981' : '#ef4444'}` }}>
                <code>{r.measurement}_{r.window}</code>
              </td>
              <td style={tdNum}>{r.test_mean.toFixed(2)}</td>
              <td style={tdNum}>{r.control_mean.toFixed(2)}</td>
              <td style={{ ...tdNum, fontWeight: 700, color: r.passed ? '#047857' : '#b91c1c' }}>
                {r.did.toFixed(2)}
              </td>
              <td style={tdNum}>{r.max.toFixed(2)}</td>
              <td style={{ ...tdNum, color: r.passed ? '#047857' : '#b91c1c', fontWeight: 600 }}>
                {r.passed ? '✓ within' : '✗ exceeds'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function WaterfallChart({ waterfall }) {
  const stages = [
    { label: 'All subjects',          value: waterfall.all_subjects,          color: '#6b7280' },
    { label: 'Universe',              value: waterfall.universe,              color: '#3b82f6' },
    { label: 'Eligible for matching', value: waterfall.eligible_for_matching, color: '#8b5cf6' },
    { label: 'Matched',               value: waterfall.matched,               color: '#10b981' },
  ];
  const max = stages[0].value || 1;
  const fmt = (n) => n.toLocaleString();

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, padding: 16 }}>
      {stages.map((s, i) => {
        const pctOfMax = (s.value / max) * 100;
        const drop = i > 0 ? stages[i - 1].value - s.value : null;
        const dropPct = i > 0 && stages[i - 1].value > 0
          ? (drop / stages[i - 1].value) * 100
          : null;
        return (
          <div key={s.label} style={{ display: 'grid', gridTemplateColumns: '180px 1fr 140px', alignItems: 'center', gap: 12, marginBottom: 8 }}>
            <div style={{ fontSize: 13, color: '#374151', fontWeight: 500 }}>
              {s.label}
            </div>
            <div style={{ position: 'relative', background: '#f3f4f6', height: 28, borderRadius: 4, overflow: 'hidden' }}>
              <div style={{
                position: 'absolute', left: 0, top: 0, bottom: 0,
                width: `${pctOfMax}%`,
                background: s.color,
                transition: 'width 200ms',
              }} />
              <div style={{
                position: 'relative', lineHeight: '28px', paddingLeft: 10,
                fontSize: 12, fontFamily: 'monospace', color: pctOfMax > 15 ? '#fff' : '#111827',
              }}>
                {fmt(s.value)}
              </div>
            </div>
            <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'right', fontFamily: 'monospace' }}>
              {drop != null
                ? `▼ ${fmt(drop)}  (${dropPct.toFixed(1)}%)`
                : `${fmt(s.value)} total`}
            </div>
          </div>
        );
      })}
    </div>
  );
}

const th = { padding: '8px 12px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e5e7eb', fontSize: 12, color: '#374151' };
const td = { padding: '8px 12px', borderBottom: '1px solid #f3f4f6' };
const tdNum = { ...td, textAlign: 'right', fontFamily: 'monospace' };
