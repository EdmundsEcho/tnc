import React from 'react';

/**
 * Horizontal funnel showing the pipeline: Universe → Target List → Reached →
 * Eligible Controls → Matched Pairs.
 * Each stage box shows the count and a % relative to the previous stage.
 */
export default function PipelineFlow({ stages }) {
  // stages: [{ label, value, relativeTo? (index of previous for %), accent }]
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
      {stages.map((s, i) => {
        const pct = s.relativeTo != null && stages[s.relativeTo]?.value > 0
          ? (s.value / stages[s.relativeTo].value * 100).toFixed(1)
          : null;
        return (
          <React.Fragment key={s.label}>
            <div style={{
              background: '#fff',
              border: `2px solid ${s.accent || '#3b82f6'}`,
              borderRadius: 8,
              padding: '12px 18px',
              minWidth: 140,
              textAlign: 'center',
            }}>
              <div style={{ fontSize: 11, color: '#6b7280', textTransform: 'uppercase', letterSpacing: 0.5 }}>
                {s.label}
              </div>
              <div style={{ fontSize: 24, fontWeight: 700, color: '#111827' }}>
                {s.value.toLocaleString()}
              </div>
              {pct && (
                <div style={{ fontSize: 11, color: '#6b7280' }}>
                  {pct}% of {stages[s.relativeTo].label}
                </div>
              )}
            </div>
            {i < stages.length - 1 && (
              <div style={{ fontSize: 24, color: '#9ca3af' }}>→</div>
            )}
          </React.Fragment>
        );
      })}
    </div>
  );
}
