/**
 * Heatmap component — renders a color-coded grid.
 *
 * Designed to be importable into ui_v2 later.
 * No framework dependencies beyond React.
 *
 * Props:
 *   data: 2D array of numbers [rows][cols]
 *   rowLabels: array of row labels
 *   colLabels: array of column labels
 *   title: chart title
 *   rowTitle: y-axis label
 *   colTitle: x-axis label
 *   colorScale: function (value, min, max) → CSS color string
 *   formatValue: function (value) → display string
 */
import React from 'react';

const DEFAULT_COLORS = {
  low: [255, 255, 255],   // white
  high: [33, 113, 181],   // blue
};

function defaultColorScale(value, min, max) {
  if (value === 0 || value == null) return '#f5f5f5';
  const t = max === min ? 0 : (value - min) / (max - min);
  const r = Math.round(DEFAULT_COLORS.low[0] + t * (DEFAULT_COLORS.high[0] - DEFAULT_COLORS.low[0]));
  const g = Math.round(DEFAULT_COLORS.low[1] + t * (DEFAULT_COLORS.high[1] - DEFAULT_COLORS.low[1]));
  const b = Math.round(DEFAULT_COLORS.low[2] + t * (DEFAULT_COLORS.high[2] - DEFAULT_COLORS.low[2]));
  return `rgb(${r},${g},${b})`;
}

function defaultFormat(v) {
  if (v == null) return '';
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k`;
  if (v >= 100) return Math.round(v).toString();
  if (v >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

export default function Heatmap({
  data,
  rowLabels,
  colLabels,
  title = '',
  rowTitle = '',
  colTitle = '',
  colorScale = defaultColorScale,
  formatValue = defaultFormat,
}) {
  if (!data || data.length === 0) return <div>No data</div>;

  // Compute min/max for color scaling
  const allValues = data.flat().filter((v) => v != null && v > 0);
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);

  const cellStyle = {
    padding: '4px 8px',
    textAlign: 'center',
    fontSize: '12px',
    fontFamily: 'monospace',
    minWidth: '50px',
    border: '1px solid #e0e0e0',
  };

  const headerStyle = {
    ...cellStyle,
    fontWeight: 'bold',
    background: '#f5f5f5',
    fontSize: '11px',
  };

  return (
    <div style={{ padding: '16px' }}>
      {title && <h3 style={{ margin: '0 0 8px 0', fontSize: '14px' }}>{title}</h3>}

      <table style={{ borderCollapse: 'collapse' }}>
        {/* Column title */}
        {colTitle && (
          <thead>
            <tr>
              <td />
              <td colSpan={colLabels.length} style={{ textAlign: 'center', fontSize: '12px', fontWeight: 'bold', paddingBottom: '4px' }}>
                {colTitle}
              </td>
            </tr>
          </thead>
        )}

        {/* Column headers */}
        <thead>
          <tr>
            <td style={headerStyle}>{rowTitle}</td>
            {colLabels.map((label) => (
              <td key={label} style={headerStyle}>{label}</td>
            ))}
          </tr>
        </thead>

        {/* Data rows */}
        <tbody>
          {data.map((row, rowIdx) => (
            <tr key={rowLabels[rowIdx]}>
              <td style={headerStyle}>{rowLabels[rowIdx]}</td>
              {row.map((value, colIdx) => {
                const bg = colorScale(value, min, max);
                const textColor = value > (min + max) / 2 ? 'white' : '#333';
                return (
                  <td
                    key={colIdx}
                    style={{
                      ...cellStyle,
                      backgroundColor: bg,
                      color: textColor,
                    }}
                  >
                    {formatValue(value)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
