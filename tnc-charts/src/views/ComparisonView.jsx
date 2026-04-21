import React, { useState } from 'react';
import { LineChart, StatCard } from '../charts';

export default function ComparisonView({ data }) {
  const { timeseries, config, summary } = data;
  const matchedPairs = summary?.matched_pairs ?? 0;

  const measurements = [...new Set(timeseries.map(r => r.measurement))].sort();
  const [selected, setSelected] = useState(measurements[0] || 'brx');

  const rows = timeseries.filter(r => r.measurement === selected);
  const groups = [...new Set(rows.map(r => r.group))];
  const months = [...new Set(rows.map(r => r.month))].sort((a, b) => a - b);

  // Include ±1σ bands for test + control (universe is noisier and would crowd the chart).
  const datasets = groups.map(g => {
    const series = months.map(m => {
      const row = rows.find(r => r.group === g && r.month === m);
      return row
        ? { mean: Number(row.mean_value) || 0, sd: Number(row.sd_value) || 0 }
        : { mean: 0, sd: 0 };
    });
    const showBand = g === 'test' || g === 'control';
    return {
      label: g,
      data: series.map(p => p.mean),
      sd: showBand ? series.map(p => p.sd) : undefined,
      color: g === 'test' ? 'rgb(59, 130, 246)'
           : g === 'control' ? 'rgb(16, 185, 129)'
           : 'rgba(107, 114, 128, 0.6)',
    };
  });

  // Phase-1 anchor: the earliest actual reach month (controls pivot on it).
  const cs = config.earliest_reach_month ?? config.reach_window?.first_month ?? 0;
  const tp = config.test_period_months ?? 0;
  const campaignStartIdx = months.indexOf(cs);

  // ── Difference-in-Differences ─────────────────────────────────
  // Pre  = N months ending the month BEFORE the Phase-1 anchor.
  // Test = [cs, cs + test_period - 1]  (lag; excluded).
  // Post = N months starting at cs + test_period.
  const windowLen = 6;

  const preStart  = cs - windowLen;
  const preEnd    = cs - 1;
  const testStart = cs;
  const testEnd   = cs + tp - 1;
  const postStart = cs + tp;
  const postEnd   = postStart + windowLen - 1;

  const meanInRange = (group, lo, hi) => {
    const vs = rows
      .filter(r => r.group === group && r.month >= lo && r.month <= hi)
      .map(r => Number(r.mean_value));
    if (vs.length === 0) return NaN;
    return vs.reduce((a, b) => a + b, 0) / vs.length;
  };

  const tPre  = meanInRange('test',    preStart,  preEnd);
  const tPost = meanInRange('test',    postStart, postEnd);
  const cPre  = meanInRange('control', preStart,  preEnd);
  const cPost = meanInRange('control', postStart, postEnd);
  const deltaT = tPost - tPre;
  const deltaC = cPost - cPre;
  const did = deltaT - deltaC;
  const didPct = cPre > 0 ? (did / cPre) * 100 : NaN;

  // Surface the visual story: how much of the "gap" existed BEFORE the
  // campaign (matching imbalance) vs. AFTER (campaign effect + residual).
  const preGap   = Number.isFinite(tPre)  ? tPre - cPre   : NaN;
  const postGap  = Number.isFinite(tPost) ? tPost - cPost : NaN;
  const preGapPct  = cPre > 0 && Number.isFinite(preGap)  ? (preGap  / cPre)  * 100 : NaN;
  const postGapPct = cPre > 0 && Number.isFinite(postGap) ? (postGap / cPre) * 100 : NaN;

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Test vs Control Comparison</h2>
      <p style={{ color: '#6b7280', marginTop: 0 }}>
        Average measurement value per month, by group. Subjects are time-aligned
        on a Phase-1 anchor — the earliest actual reach date (month {cs}) — so
        the horizontal axis is real study months. The lift appears as divergence
        between the test and control lines after month {cs + tp}
        {tp > 0 && ` (with a ${tp}-month test/lag window excluded from analysis)`}.
      </p>

      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        {measurements.map(m => (
          <button
            key={m}
            onClick={() => setSelected(m)}
            style={{
              padding: '6px 14px',
              border: '1px solid #d1d5db',
              background: selected === m ? '#3b82f6' : '#fff',
              color: selected === m ? '#fff' : '#374151',
              borderRadius: 4,
              cursor: 'pointer',
              fontWeight: selected === m ? 600 : 400,
            }}
          >
            {m}
          </button>
        ))}
      </div>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <StatCard
          label="Matched pairs"
          value={matchedPairs}
          subtext={`${matchedPairs.toLocaleString()} tests + ${matchedPairs.toLocaleString()} controls`}
          accent="#3b82f6"
        />
        <StatCard
          label="Pre-period gap"
          value={Number.isFinite(preGap) ? preGap.toFixed(1) : '—'}
          subtext={Number.isFinite(preGapPct)
            ? `${preGapPct >= 0 ? '+' : ''}${preGapPct.toFixed(1)}% vs control · matching imbalance`
            : 'matching imbalance'}
          accent={Math.abs(preGap) < 1 ? '#10b981' : '#f59e0b'}
        />
        <StatCard
          label="Post-period gap"
          value={Number.isFinite(postGap) ? postGap.toFixed(1) : '—'}
          subtext={Number.isFinite(postGapPct)
            ? `${postGapPct >= 0 ? '+' : ''}${postGapPct.toFixed(1)}% vs control · effect + residual`
            : 'effect + residual'}
          accent="#8b5cf6"
        />
        <StatCard
          label="DiD (post − pre)"
          value={Number.isFinite(did) ? did.toFixed(1) : '—'}
          subtext={Number.isFinite(didPct)
            ? `${didPct >= 0 ? '+' : ''}${didPct.toFixed(1)}% recovered lift`
            : 'recovered lift'}
          accent="#10b981"
        />
      </div>

      <PeriodBar preStart={preStart} preEnd={preEnd}
                 testStart={testStart} testEnd={testEnd}
                 postStart={postStart} postEnd={postEnd} />

      <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, padding: 16 }}>
        <LineChart
          labels={months}
          datasets={datasets}
          title={`${selected} — mean by month & group   (N = ${matchedPairs.toLocaleString()} matched pairs)`}
          xAxisLabel={`month (Phase-1 anchor at ${cs}; test period ${testStart}–${testEnd})`}
          yAxisLabel="mean units"
          campaignStartIndex={campaignStartIdx}
          height={400}
        />
      </div>

      <DiDPanel
        windowLen={windowLen}
        tPre={tPre} tPost={tPost} cPre={cPre} cPost={cPost}
        deltaT={deltaT} deltaC={deltaC} did={did} didPct={didPct}
        preStart={preStart} preEnd={preEnd}
        postStart={postStart} postEnd={postEnd}
        measurement={selected}
      />

      <div style={{ marginTop: 24, fontSize: 12, color: '#6b7280' }}>
        <strong>Reading the chart:</strong> before the Phase-1 anchor (month {cs}), test
        and control lines should overlap — if they don't, that pre-period gap
        (shown above) is matching imbalance, not campaign effect. The {tp}-month
        shaded test period is excluded from analysis. After month {cs + tp},
        <em> part of the divergence is the real lift and part is the pre-period
        gap carried forward</em> — the DiD below subtracts the pre-period gap out,
        which is why the DiD number can diverge from what the eye sees in the raw
        line chart. Phase-2 will re-anchor each subject to their own reach date for
        tighter alignment.
      </div>
    </div>
  );
}

function PeriodBar({ preStart, preEnd, testStart, testEnd, postStart, postEnd }) {
  const bandStyle = (bg, border) => ({
    background: bg, border: `1px solid ${border}`,
    padding: '8px 12px', borderRadius: 4, flex: 1, textAlign: 'center',
    fontFamily: 'system-ui', fontSize: 12,
  });
  return (
    <div style={{ display: 'flex', gap: 6, marginBottom: 12 }}>
      <div style={bandStyle('#eff6ff', '#bfdbfe')}>
        <div style={{ fontWeight: 600, color: '#1d4ed8' }}>Pre period</div>
        <div style={{ color: '#6b7280' }}>months {preStart}–{preEnd}</div>
      </div>
      <div style={bandStyle('#fef3c7', '#fde68a')}>
        <div style={{ fontWeight: 600, color: '#b45309' }}>Test (lag) period — excluded</div>
        <div style={{ color: '#6b7280' }}>
          {testEnd >= testStart ? `months ${testStart}–${testEnd}` : 'none'}
        </div>
      </div>
      <div style={bandStyle('#ecfdf5', '#a7f3d0')}>
        <div style={{ fontWeight: 600, color: '#047857' }}>Post period</div>
        <div style={{ color: '#6b7280' }}>months {postStart}–{postEnd}</div>
      </div>
    </div>
  );
}

function DiDPanel({
  windowLen, tPre, tPost, cPre, cPost, deltaT, deltaC, did, didPct,
  preStart, preEnd, postStart, postEnd, measurement,
}) {
  const fmt = v => (Number.isFinite(v) ? v.toFixed(2) : '—');

  return (
    <div style={{
      marginTop: 24, background: '#fff',
      border: '1px solid #e5e7eb', borderRadius: 6, padding: 16,
    }}>
      <h3 style={{ fontSize: 14, color: '#374151', margin: '0 0 4px 0' }}>
        Difference-in-Differences ({measurement})
      </h3>
      <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
        Comparing the mean change from pre (months {preStart}–{preEnd}) to post
        (months {postStart}–{postEnd}) — {windowLen} months each, test-period months
        excluded.
      </p>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: 'system-ui', fontSize: 13 }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            <th style={th}>Group</th>
            <th style={{ ...th, textAlign: 'right' }}>Pre mean</th>
            <th style={{ ...th, textAlign: 'right' }}>Post mean</th>
            <th style={{ ...th, textAlign: 'right' }}>Δ (post − pre)</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td style={{ ...td, borderLeft: '3px solid #3b82f6' }}>Test</td>
            <td style={tdNum}>{fmt(tPre)}</td>
            <td style={tdNum}>{fmt(tPost)}</td>
            <td style={{ ...tdNum, color: '#1d4ed8' }}>{fmt(deltaT)}</td>
          </tr>
          <tr>
            <td style={{ ...td, borderLeft: '3px solid #10b981' }}>Control</td>
            <td style={tdNum}>{fmt(cPre)}</td>
            <td style={tdNum}>{fmt(cPost)}</td>
            <td style={{ ...tdNum, color: '#047857' }}>{fmt(deltaC)}</td>
          </tr>
          <tr style={{ fontWeight: 600, background: '#f9fafb' }}>
            <td style={{ ...td, borderLeft: '3px solid #111827' }}>DiD (test − control)</td>
            <td style={tdNum}></td>
            <td style={tdNum}></td>
            <td style={{ ...tdNum, color: '#111827' }}>
              {fmt(did)}
              {Number.isFinite(didPct) && (
                <span style={{ marginLeft: 8, color: '#6b7280', fontSize: 11 }}>
                  ({didPct >= 0 ? '+' : ''}{didPct.toFixed(1)}% vs control pre)
                </span>
              )}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

const th = { padding: '8px 12px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e5e7eb', fontSize: 12, color: '#374151' };
const td = { padding: '8px 12px', borderBottom: '1px solid #f3f4f6' };
const tdNum = { ...td, textAlign: 'right', fontFamily: 'monospace' };
