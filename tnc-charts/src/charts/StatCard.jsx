import React from 'react';

/**
 * Large number + label card — the building block for "pipeline counts".
 */
export default function StatCard({ label, value, subtext, accent = '#2563eb', width = 180 }) {
  const formatted = typeof value === 'number' ? value.toLocaleString() : value;
  return (
    <div style={{
      background: '#fff',
      border: '1px solid #e5e7eb',
      borderLeft: `4px solid ${accent}`,
      borderRadius: 6,
      padding: '14px 18px',
      width,
      boxShadow: '0 1px 2px rgba(0,0,0,0.04)',
    }}>
      <div style={{ fontSize: 11, color: '#6b7280', textTransform: 'uppercase', letterSpacing: 0.5 }}>
        {label}
      </div>
      <div style={{ fontSize: 28, fontWeight: 700, color: '#111827', margin: '4px 0' }}>
        {formatted}
      </div>
      {subtext && (
        <div style={{ fontSize: 12, color: '#6b7280' }}>{subtext}</div>
      )}
    </div>
  );
}
