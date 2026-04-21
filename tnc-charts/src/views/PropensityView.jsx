import React from 'react';
import { GroupedBarChart, StatCard } from '../charts';

/**
 * Propensity score view.
 *
 * The propensity model is a logistic regression fit of `reached` (1 if the
 * subject has a campaign_reach_date, else 0) on all qualities + baseline
 * volumes. If reach is correlated with observable characteristics, the
 * reached cohort's propensity scores will concentrate in high buckets and
 * the not-reached cohort's in low buckets — that correlation is the
 * prerequisite for propensity-score matching.
 */
export default function PropensityView({ data }) {
  const { subjects, targetList } = data;

  const reached = subjects
    .filter(s => Number(s.reached) === 1)
    .map(s => Number(s.propensity_score) || 0);

  const notReached = subjects
    .filter(s => Number(s.reached) !== 1)
    .map(s => Number(s.propensity_score) || 0);

  // 5 equal-width buckets over the [0, 1] propensity range.
  const BUCKETS = 5;
  const bucketLabels = new Array(BUCKETS).fill(0).map((_, i) => {
    const lo = (i / BUCKETS).toFixed(1);
    const hi = ((i + 1) / BUCKETS).toFixed(1);
    return `${lo}–${hi}`;
  });
  const bucketize = (scores) => {
    const counts = new Array(BUCKETS).fill(0);
    for (const v of scores) {
      // Clamp scores to [0, 1); the final bucket captures p = 1 exactly.
      const idx = Math.min(BUCKETS - 1, Math.max(0, Math.floor(v * BUCKETS)));
      counts[idx] += 1;
    }
    return counts;
  };
  const reachedBuckets = bucketize(reached);
  const notReachedBuckets = bucketize(notReached);

  // Diagnostic: target-list members who were never reached. By design they
  // share qualities with reached targets, so their propensity scores should
  // sit near the reached distribution.
  const targetNpis = new Set((targetList || []).map(r => r.npi));
  const onListNotReached = subjects
    .filter(s => targetNpis.has(s.npi) && Number(s.reached) !== 1)
    .map(s => Number(s.propensity_score) || 0);

  // Propensity decile table: reached vs not-reached per decile
  const bins = {
    reached: new Array(10).fill(0),
    notReached: new Array(10).fill(0),
  };
  for (const s of subjects) {
    const d = Number(s.propensity_decile);
    if (!(d >= 1 && d <= 10)) continue;
    const idx = d - 1;
    if (Number(s.reached) === 1) bins.reached[idx] += 1;
    else bins.notReached[idx] += 1;
  }

  const meanReached = mean(reached);
  const meanNotReached = mean(notReached);

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Propensity Score</h2>
      <p style={{ color: '#6b7280', marginTop: 0 }}>
        Logistic regression of <code>reached</code> ∈ {'{'}0, 1{'}'} (1 if the subject has a
        campaign_reach_date) on all subject qualities (one-hot) and baseline volumes
        (hrx_L12M, grx_L12M). The resulting score is each subject's
        {' '}<strong>predicted probability of being reached</strong> given their observable
        characteristics — the prerequisite for propensity-score matching.
      </p>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 24 }}>
        <StatCard label="Mean — reached"     value={meanReached.toFixed(3)}    accent="#3b82f6" />
        <StatCard label="Mean — not reached" value={meanNotReached.toFixed(3)} accent="#6b7280" />
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>
            Reached vs not-reached by propensity bucket
          </h3>
          <GroupedBarChart
            labels={bucketLabels}
            datasets={[
              { label: 'Reached',     data: reachedBuckets,    color: 'rgba(59, 130, 246, 0.8)' },
              { label: 'Not reached', data: notReachedBuckets, color: 'rgba(107, 114, 128, 0.6)' },
            ]}
            xAxisLabel="propensity score bucket"
            yAxisLabel="subject count"
            height={300}
          />
          <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 4 }}>
            If the model works, reached subjects concentrate in the high buckets (0.6–1.0)
            and not-reached in the low buckets (0.0–0.4).
          </div>
        </div>

        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>Propensity decile table</h3>
          <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 13, background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, overflow: 'hidden' }}>
            <thead>
              <tr style={{ background: '#f9fafb' }}>
                <th style={th}>Segment</th>
                {['D1','D2','D3','D4','D5','D6','D7','D8','D9','D10'].map(d => (
                  <th key={d} style={{ ...th, textAlign: 'right' }}>{d}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style={{ ...td, borderLeft: '3px solid #3b82f6' }}>Reached</td>
                {bins.reached.map((v, i) => (<td key={i} style={tdNum}>{v}</td>))}
              </tr>
              <tr>
                <td style={{ ...td, borderLeft: '3px solid #9ca3af' }}>Not reached</td>
                {bins.notReached.map((v, i) => (<td key={i} style={tdNum}>{v}</td>))}
              </tr>
            </tbody>
          </table>
          <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 8 }}>
            Reached subjects should concentrate in the high propensity deciles (D8–D10).
          </div>
        </div>
      </div>

      <div style={{ marginTop: 32, fontSize: 12, color: '#6b7280' }}>
        <strong>Diagnostic: target-list members who weren't reached</strong>
        {' '}({onListNotReached.length} subjects) — by design these sit at
        {' '}roughly the same propensity level as reached targets, since they share
        {' '}observable characteristics. Mean score:
        {' '}<strong>{onListNotReached.length ? mean(onListNotReached).toFixed(3) : '—'}</strong>.
      </div>
    </div>
  );
}

function mean(arr) {
  if (arr.length === 0) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

const th = { padding: '8px 10px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e5e7eb', fontSize: 12, color: '#374151' };
const td = { padding: '8px 10px', borderBottom: '1px solid #f3f4f6' };
const tdNum = { ...td, textAlign: 'right', fontFamily: 'monospace' };
