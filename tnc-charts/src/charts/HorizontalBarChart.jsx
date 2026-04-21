/**
 * Horizontal bar chart — compares two groups side by side.
 *
 * Designed to be importable into ui_v2 later.
 * Uses Chart.js via react-chartjs-2.
 *
 * Props:
 *   labels: array of category labels (e.g., specialty names)
 *   datasets: array of { label, data, color } objects
 *   title: chart title
 *   xAxisLabel: x-axis label
 *   height: chart height in pixels
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

export default function HorizontalBarChart({
  labels,
  datasets,
  title = '',
  xAxisLabel = 'Count',
  height = 400,
}) {
  if (!labels || labels.length === 0) return <div>No data</div>;

  const data = {
    labels,
    datasets: datasets.map((ds, i) => ({
      label: ds.label,
      data: ds.data,
      backgroundColor: ds.color || defaultColors[i % defaultColors.length],
      borderWidth: 0,
      barPercentage: 0.8,
      categoryPercentage: 0.9,
    })),
  };

  const options = {
    indexAxis: 'y',
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: title ? { display: true, text: title, font: { size: 14 } } : { display: false },
      legend: { display: datasets.length > 1, position: 'top' },
    },
    scales: {
      x: {
        title: xAxisLabel ? { display: true, text: xAxisLabel } : { display: false },
        beginAtZero: true,
      },
      y: {
        ticks: { font: { size: 11 } },
      },
    },
  };

  return (
    <div style={{ height, padding: '16px' }}>
      <Bar data={data} options={options} />
    </div>
  );
}

const defaultColors = [
  'rgba(54, 162, 235, 0.7)',
  'rgba(255, 99, 132, 0.7)',
  'rgba(75, 192, 192, 0.7)',
  'rgba(255, 159, 64, 0.7)',
];
