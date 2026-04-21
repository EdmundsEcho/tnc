import React from 'react';
import { HorizontalBarChart } from '../charts';
import { countBy, sortedCounts } from '../data/loader';
import { mergeNote } from '../util/deciles';

export default function UniverseView({ data }) {
  const { subjects, windows, decileGrouping } = data;

  const bySpec = sortedCounts(countBy(subjects, r => r.specialty));
  const byState = sortedCounts(countBy(subjects, r => r.state));

  // Subject counts in each decile (D0..D10) for each Rx measurement's L12M
  // volume. Deciles are equal-*volume* (see add_decile_column in windows.rs),
  // so subject counts naturally skew — D10 is a small set of whales, D1 is
  // a long tail, and D0 is non-writers.
  const decileLabels = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10'];
  const countByDecile = (col) => {
    const counts = new Array(11).fill(0);
    for (const w of windows) {
      const d = Number(w[col]);
      if (d >= 0 && d <= 10) counts[d] += 1;
    }
    return counts;
  };
  const brxDeciles = countByDecile('brx_L12M_decile');
  const hrxDeciles = countByDecile('hrx_L12M_decile');
  const grxDeciles = countByDecile('grx_L12M_decile');

  // Decile distribution table.
  // Decile is on brx_L12M (branded Rx usage). Rows are a 2×2 cross-tab of
  // (called_on ∈ {yes, no}) × (reached ∈ {yes, no}) — reached = the subject
  // has a campaign_reach_date (the actual campaign touch).
  const winByNpi = new Map(windows.map(r => [r.npi, r]));
  // 11 buckets: index 0 = D0 (non-writers), 1..10 = D1..D10 equal-volume deciles
  const counts = {
    calledYes_reachedYes: new Array(11).fill(0),
    calledYes_reachedNo:  new Array(11).fill(0),
    calledNo_reachedYes:  new Array(11).fill(0),
    calledNo_reachedNo:   new Array(11).fill(0),
    totalByDec:           new Array(11).fill(0),
  };
  for (const s of subjects) {
    const w = winByNpi.get(s.npi);
    if (!w) continue;
    const d = Number(w.brx_L12M_decile);
    if (!(d >= 0 && d <= 10)) continue;
    const idx = d;
    counts.totalByDec[idx] += 1;
    const called = Number(s.called_on) === 1;
    const reached = Number(s.reached) === 1;
    if (called && reached) counts.calledYes_reachedYes[idx] += 1;
    else if (called && !reached) counts.calledYes_reachedNo[idx] += 1;
    else if (!called && reached) counts.calledNo_reachedYes[idx] += 1;
    else counts.calledNo_reachedNo[idx] += 1;
  }

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Universe of Subjects</h2>
      <p style={{ color: '#6b7280', marginTop: 0 }}>
        Distribution of qualities and baseline measurement volumes across the full subject pool.
      </p>

      <h3 style={{ fontSize: 14, color: '#374151', marginTop: 24 }}>
        Decile distribution (brx_L12M — branded Rx usage, equal-volume)
      </h3>
      <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
        <strong>D0</strong> = non-writers (brx_L12M = 0). <strong>D1..D10</strong> each hold
        ~1/10 of total branded Rx — so D10 is a small set of high-volume whales and D1 is a
        long tail of low-volume writers. Rows are a 2×2 cross-tab of <strong>called on</strong>
        (in-person sales visit) and <strong>reached</strong> (the subject has a
        campaign_reach_date — an actual campaign touch).
      </p>
      <DecileTable counts={counts} />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginTop: 32 }}>
        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>Specialty</h3>
          <HorizontalBarChart
            labels={bySpec.map(([k]) => k)}
            datasets={[{ label: 'Subjects', data: bySpec.map(([, v]) => v) }]}
            height={280}
          />
        </div>
        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>State</h3>
          <HorizontalBarChart
            labels={byState.map(([k]) => k)}
            datasets={[{ label: 'Subjects', data: byState.map(([, v]) => v) }]}
            height={280}
          />
        </div>

      </div>

      <div style={{ marginTop: 32 }}>
        <h3 style={{ fontSize: 14, color: '#374151' }}>
          Subjects per L12M decile (brx / hrx / grx)
        </h3>
        <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
          Equal-<em>volume</em> deciles: D0 is non-writers (value = 0), D1..D10 each hold
          roughly 1/10 of total Rx volume for that measurement — so subject counts
          naturally grow from D10 (whales) to D1 (long tail). Each series shows how
          the universe splits when bucketed by that measurement's L12M volume.
        </p>
        <HorizontalBarChart
          labels={decileLabels}
          datasets={[
            { label: 'brx_L12M', data: brxDeciles, color: 'rgba(59, 130, 246, 0.75)' },
            { label: 'hrx_L12M', data: hrxDeciles, color: 'rgba(16, 185, 129, 0.75)' },
            { label: 'grx_L12M', data: grxDeciles, color: 'rgba(245, 158, 11, 0.75)' },
          ]}
          xAxisLabel="Subject count"
          height={380}
        />
        <MergeNotes grouping={decileGrouping} sources={[
          { measurement: 'brx', window: 'L12M' },
          { measurement: 'hrx', window: 'L12M' },
          { measurement: 'grx', window: 'L12M' },
        ]} />
      </div>
    </div>
  );
}

function MergeNotes({ grouping, sources }) {
  const notes = sources
    .map(s => mergeNote(grouping, s.measurement, s.window))
    .filter(Boolean);
  if (!notes.length) return null;
  return (
    <div style={{ fontSize: 11, color: '#6b7280', marginTop: 6 }}>
      <strong>Decile grouping:</strong> {notes.join(' · ')}
    </div>
  );
}

function DecileTable({ counts }) {
  const headers = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10','Total'];
  const totalAll = counts.totalByDec.reduce((a, b) => a + b, 0);
  const rows = [
    { label: 'Called on  ·  Reached',         values: counts.calledYes_reachedYes, accent: '#3b82f6' },
    { label: 'Called on  ·  Not reached',     values: counts.calledYes_reachedNo,  accent: '#60a5fa' },
    { label: 'Not called on  ·  Reached',     values: counts.calledNo_reachedYes,  accent: '#8b5cf6' },
    { label: 'Not called on  ·  Not reached', values: counts.calledNo_reachedNo,   accent: '#9ca3af' },
    { label: 'Total',                         values: counts.totalByDec,           accent: '#111827', bold: true },
  ];

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, overflow: 'hidden' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontFamily: 'system-ui', fontSize: 13 }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            <th style={th}>Segment</th>
            {headers.map(h => <th key={h} style={{ ...th, textAlign: 'right' }}>{h}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map(row => {
            const rowTotal = row.values.reduce((a, b) => a + b, 0);
            return (
              <tr key={row.label} style={{ fontWeight: row.bold ? 600 : 400 }}>
                <td style={{ ...td, borderLeft: `3px solid ${row.accent}` }}>{row.label}</td>
                {row.values.map((v, i) => {
                  const total = counts.totalByDec[i];
                  const pct = total > 0 ? (v / total * 100).toFixed(0) : '';
                  return (
                    <td key={i} style={{ ...tdNum, color: v === 0 ? '#9ca3af' : '#111827' }}>
                      {v}
                      {total > 0 && !row.bold && (
                        <div style={{ fontSize: 10, color: '#6b7280' }}>{pct}%</div>
                      )}
                    </td>
                  );
                })}
                <td style={{ ...tdNum, fontWeight: 600 }}>
                  {rowTotal}
                  {totalAll > 0 && !row.bold && (
                    <div style={{ fontSize: 10, color: '#6b7280' }}>
                      {(rowTotal / totalAll * 100).toFixed(0)}%
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const th = { padding: '8px 12px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e5e7eb', fontSize: 12, color: '#374151' };
const td = { padding: '8px 12px', borderBottom: '1px solid #f3f4f6' };
const tdNum = { ...td, textAlign: 'right', fontFamily: 'monospace' };
