/**
 * Grouped (side-by-side) bar chart — for comparing two groups across categories.
 *
 * Props:
 *   labels:   x-axis categories
 *   datasets: array of { label, data, color } objects (2+ groups plotted side by side)
 *   title, xAxisLabel, yAxisLabel, height
 *   vertical: boolean (true = vertical bars, false = horizontal)
 *   errorBars: optional per-dataset array of { upper, lower } for error bars (drawn as lines)
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

const DEFAULT_COLORS = [
  'rgba(54, 162, 235, 0.8)',
  'rgba(255, 99, 132, 0.8)',
  'rgba(75, 192, 192, 0.8)',
  'rgba(255, 159, 64, 0.8)',
];

export default function GroupedBarChart({
  labels,
  datasets,
  title = '',
  xAxisLabel = '',
  yAxisLabel = '',
  height = 320,
  vertical = true,
}) {
  if (!labels || !datasets) return null;

  const data = {
    labels,
    datasets: datasets.map((ds, i) => ({
      label: ds.label,
      data: ds.data,
      backgroundColor: ds.color || DEFAULT_COLORS[i % DEFAULT_COLORS.length],
      borderWidth: 0,
      barPercentage: 0.85,
      categoryPercentage: 0.8,
    })),
  };

  const options = {
    indexAxis: vertical ? 'x' : 'y',
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: title ? { display: true, text: title, font: { size: 14 } } : { display: false },
      legend: { display: true, position: 'top' },
    },
    scales: {
      x: {
        title: xAxisLabel ? { display: true, text: xAxisLabel } : { display: false },
        beginAtZero: true,
      },
      y: {
        title: yAxisLabel ? { display: true, text: yAxisLabel } : { display: false },
        beginAtZero: true,
      },
    },
  };

  return (
    <div style={{ height, padding: '8px' }}>
      <Bar data={data} options={options} />
    </div>
  );
}
