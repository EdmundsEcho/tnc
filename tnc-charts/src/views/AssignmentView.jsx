import React from 'react';
import { GroupedBarChart, StatCard } from '../charts';
import { mergeNote } from '../util/deciles';

export default function AssignmentView({ data }) {
  const { subjects, windows, matches, targetList, decileGrouping } = data;
  const hrxMerge = mergeNote(decileGrouping, 'hrx', 'L12M');

  const idx = new Map(windows.map(r => [r.npi, r]));
  const targetNpis = new Set((targetList || []).map(r => r.npi));
  // matches is unused by the funnel table (control pool is derived from
  // universe membership, not matched pairs); keep the binding for forward use.
  void matches;

  // Split subjects by assignment status
  const target = subjects.filter(s => targetNpis.has(s.npi));
  const reached = subjects.filter(s => Number(s.reached) === 1);
  const notReached = target.filter(s => Number(s.reached) !== 1);
  const notTarget = subjects.filter(s => !targetNpis.has(s.npi));

  // Specialty breakdown by assignment
  const specialties = [...new Set(subjects.map(s => s.specialty))].sort();
  const bySpec = specialties.map(sp => ({
    specialty: sp,
    target: target.filter(s => s.specialty === sp).length,
    notTarget: notTarget.filter(s => s.specialty === sp).length,
  }));

  // hrx_L12M DECILE distribution: reached vs not-reached.
  // Deciles are equal-volume (D0 = non-writers, D10 = whales).
  const decileLabels = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10'];
  const countByDecile = (rows) => {
    const counts = new Array(11).fill(0);
    for (const s of rows) {
      const d = Number(idx.get(s.npi)?.hrx_L12M_decile);
      if (d >= 0 && d <= 10) counts[d] += 1;
    }
    return counts;
  };
  const hrxReachedDeciles = countByDecile(reached);
  const hrxNotReachedDeciles = countByDecile(
    subjects.filter(s => Number(s.reached) !== 1),
  );

  // ─── Population funnel by brx_L12M decile ─────────────────────
  //   Writing Universe   — any subject with any positive Rx (brx + hrx + grx > 0)
  //   Campaign Universe  — signature-filtered universe (subjects.in_universe = 1)
  //   Campaign Reach     — in_universe AND reached
  //   Control Pool       — in_universe AND NOT reached
  //
  // Campaign Reach + Control Pool partition the Campaign Universe.
  const assignmentRows = new Array(11).fill(0).map(() => ({
    writing: 0, campaign: 0, reach: 0, controlPool: 0,
  }));
  for (const s of subjects) {
    const w = idx.get(s.npi);
    if (!w) continue;
    const d = Number(w.brx_L12M_decile);
    if (!(d >= 0 && d <= 10)) continue;

    const brx = Number(w.brx_L12M) || 0;
    const hrx = Number(w.hrx_L12M) || 0;
    const grx = Number(w.grx_L12M) || 0;
    const inWriting = (brx + hrx + grx) > 0;
    const inUniverse = Number(s.in_universe) === 1;
    const isReached = Number(s.reached) === 1;

    if (inWriting)  assignmentRows[d].writing    += 1;
    if (inUniverse) assignmentRows[d].campaign   += 1;
    if (inUniverse && isReached)  assignmentRows[d].reach       += 1;
    if (inUniverse && !isReached) assignmentRows[d].controlPool += 1;
  }

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Test / Control Assignment</h2>
      <p style={{ color: '#6b7280', marginTop: 0 }}>
        How the universe is partitioned into the target list, reached subjects, and the rest.
      </p>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 24 }}>
        <StatCard label="Universe"      value={subjects.length} accent="#6b7280" />
        <StatCard label="Target List"   value={target.length}    accent="#3b82f6"
          subtext={`${(100 * target.length / subjects.length).toFixed(1)}%`} />
        <StatCard label="Reached"       value={reached.length}   accent="#8b5cf6"
          subtext={`${(100 * reached.length / target.length).toFixed(1)}% of target`} />
        <StatCard label="On list, not reached" value={notReached.length} accent="#f59e0b" />
        <StatCard label="Not on list" value={notTarget.length} accent="#10b981"
          subtext="also eligible controls" />
      </div>

      <h3 style={{ fontSize: 14, color: '#374151', marginTop: 24 }}>
        Population funnel by brx_L12M decile
      </h3>
      <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 12px 0' }}>
        Four populations, bucketed by each subject's branded-Rx decile over the 12 months
        before the campaign. The table narrows left → right: from all Rx-writing subjects
        down to the signature-filtered campaign universe. Within the campaign universe,
        <strong> Campaign Reach + Control Pool partition the population</strong> — reach
        is the subjects the campaign actually touched; the rest are candidate controls.
      </p>
      <AssignmentTable rows={assignmentRows} />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginTop: 24 }}>
        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>Specialty by assignment</h3>
          <GroupedBarChart
            labels={specialties}
            datasets={[
              { label: 'On target list',  data: bySpec.map(r => r.target),    color: 'rgba(59, 130, 246, 0.8)' },
              { label: 'Not on list',     data: bySpec.map(r => r.notTarget), color: 'rgba(107, 114, 128, 0.6)' },
            ]}
            xAxisLabel="Specialty"
            yAxisLabel="Subject count"
            height={300}
          />
        </div>

        <div>
          <h3 style={{ fontSize: 14, color: '#374151' }}>
            hrx_L12M decile: reached vs. not-reached
          </h3>
          <GroupedBarChart
            labels={decileLabels}
            datasets={[
              { label: 'Reached',     data: hrxReachedDeciles,    color: 'rgba(139, 92, 246, 0.8)' },
              { label: 'Not reached', data: hrxNotReachedDeciles, color: 'rgba(107, 114, 128, 0.6)' },
            ]}
            xAxisLabel="hrx_L12M decile (D0 = non-writer, D10 = whale)"
            yAxisLabel="Subject count"
            height={300}
          />
          {hrxMerge && (
            <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 4 }}>
              <strong>Decile grouping:</strong> {hrxMerge}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function AssignmentTable({ rows }) {
  const labels = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','D10'];
  const total = rows.reduce(
    (acc, r) => ({
      writing:     acc.writing     + r.writing,
      campaign:    acc.campaign    + r.campaign,
      reach:       acc.reach       + r.reach,
      controlPool: acc.controlPool + r.controlPool,
    }),
    { writing: 0, campaign: 0, reach: 0, controlPool: 0 },
  );
  const pct = (n, d) => (d > 0 ? `${(100 * n / d).toFixed(1)}%` : '—');
  const fmt = (n) => n.toLocaleString();
  const th = { padding: '8px 12px', textAlign: 'left', fontWeight: 600, borderBottom: '1px solid #e5e7eb', fontSize: 12, color: '#374151' };
  const td = { padding: '8px 12px', borderBottom: '1px solid #f3f4f6', fontFamily: 'monospace', fontSize: 13 };
  const tdNum = { ...td, textAlign: 'right' };

  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 6, overflow: 'hidden' }}>
      <table style={{ borderCollapse: 'collapse', width: '100%' }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            <th style={th}>L12M decile</th>
            <th style={{ ...th, textAlign: 'right' }}>Writing Universe</th>
            <th style={{ ...th, textAlign: 'right' }}>Campaign Universe</th>
            <th style={{ ...th, textAlign: 'right' }}>Campaign Reach</th>
            <th style={{ ...th, textAlign: 'right' }}>Control Pool</th>
            <th style={{ ...th, textAlign: 'right' }}>Control Pool % of Campaign Universe</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={labels[i]} style={{ background: i % 2 ? '#fff' : '#fafafa' }}>
              <td style={{ ...td, fontWeight: 600, color: '#111827' }}>{labels[i]}</td>
              <td style={tdNum}>{fmt(r.writing)}</td>
              <td style={tdNum}>{fmt(r.campaign)}</td>
              <td style={{ ...tdNum, color: '#1d4ed8' }}>{fmt(r.reach)}</td>
              <td style={{ ...tdNum, color: '#047857' }}>{fmt(r.controlPool)}</td>
              <td style={{ ...tdNum, color: '#6b7280' }}>{pct(r.controlPool, r.campaign)}</td>
            </tr>
          ))}
          <tr style={{ background: '#f3f4f6', fontWeight: 700 }}>
            <td style={{ ...td, fontWeight: 700 }}>Total</td>
            <td style={tdNum}>{fmt(total.writing)}</td>
            <td style={tdNum}>{fmt(total.campaign)}</td>
            <td style={{ ...tdNum, color: '#1d4ed8' }}>{fmt(total.reach)}</td>
            <td style={{ ...tdNum, color: '#047857' }}>{fmt(total.controlPool)}</td>
            <td style={{ ...tdNum, color: '#111827' }}>{pct(total.controlPool, total.campaign)}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}
