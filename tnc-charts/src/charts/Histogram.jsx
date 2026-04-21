/**
 * Simple histogram — bins numeric values into fixed-count buckets.
 *
 * Props:
 *   data: array of numbers (or an array of { label, values })
 *   bins: number of bins (default 20)
 *   title, xAxisLabel, yAxisLabel, height
 *   color
 */
import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

export default function Histogram({
  data,
  bins = 20,
  title = '',
  xAxisLabel = 'Value',
  yAxisLabel = 'Count',
  height = 300,
  color = 'rgba(54, 162, 235, 0.7)',
}) {
  if (!data || data.length === 0) return <div>No data</div>;

  // Accept either a flat array or a multi-series array
  const isMultiSeries = Array.isArray(data[0]?.values);
  const series = isMultiSeries
    ? data.map((s, i) => ({ label: s.label, values: s.values, color: s.color }))
    : [{ label: '', values: data, color }];

  // Global min/max across all series
  const allValues = series.flatMap(s => s.values);
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);
  const step = (max - min) / bins || 1;

  // Produce bin counts per series
  const datasets = series.map((s, i) => {
    const counts = new Array(bins).fill(0);
    for (const v of s.values) {
      const idx = Math.min(bins - 1, Math.max(0, Math.floor((v - min) / step)));
      counts[idx] += 1;
    }
    return {
      label: s.label,
      data: counts,
      backgroundColor: s.color || color,
      borderWidth: 0,
      categoryPercentage: 1.0,
      barPercentage: 0.97,
    };
  });

  const labels = new Array(bins).fill(0).map((_, i) => {
    const lo = min + i * step;
    const hi = lo + step;
    return `${lo.toFixed(0)}–${hi.toFixed(0)}`;
  });

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: title ? { display: true, text: title, font: { size: 14 } } : { display: false },
      legend: { display: isMultiSeries },
    },
    scales: {
      x: {
        title: xAxisLabel ? { display: true, text: xAxisLabel } : { display: false },
        ticks: { maxRotation: 45, minRotation: 45 },
      },
      y: {
        title: yAxisLabel ? { display: true, text: yAxisLabel } : { display: false },
        beginAtZero: true,
      },
    },
  };

  return (
    <div style={{ height, padding: '8px' }}>
      <Bar data={{ labels, datasets }} options={options} />
    </div>
  );
}
