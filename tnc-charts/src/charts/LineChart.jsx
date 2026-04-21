/**
 * Multi-series line chart (time on x-axis).
 *
 * Props:
 *   labels:   array of x-axis labels (e.g., month numbers)
 *   datasets: array of { label, data, color } objects
 *   title:    optional chart title
 *   xAxisLabel, yAxisLabel: axis labels
 *   height:   chart height in pixels
 *   annotations: array of { month, label } to draw as vertical bands (e.g., campaign_start)
 */
import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale, LinearScale, PointElement, LineElement,
  Title, Tooltip, Legend, Filler,
);

const DEFAULT_COLORS = [
  'rgb(54, 162, 235)',
  'rgb(255, 99, 132)',
  'rgb(107, 114, 128)',
  'rgb(75, 192, 192)',
  'rgb(255, 159, 64)',
];

export default function LineChart({
  labels,
  datasets,
  title = '',
  xAxisLabel = '',
  yAxisLabel = '',
  height = 320,
  campaignStartIndex = null,
}) {
  if (!labels || !datasets) return null;

  // If a dataset supplies `sd`, render a ±1σ band (two extra datasets with
  // fill between them and muted color). Bands render first so lines sit on top.
  const bandDatasets = [];
  for (const ds of datasets) {
    if (!ds.sd) continue;
    const color = ds.color || DEFAULT_COLORS[0];
    const fill = color.replace('rgb', 'rgba').replace(')', ', 0.12)');
    const upper = ds.data.map((v, i) => v + (ds.sd[i] ?? 0));
    const lower = ds.data.map((v, i) => v - (ds.sd[i] ?? 0));
    // Upper bound anchors the fill region; lower bound's fill points back to upper's index.
    const upperIdx = bandDatasets.length;
    bandDatasets.push({
      label: `${ds.label} +1σ`,
      data: upper,
      borderColor: 'transparent',
      backgroundColor: fill,
      pointRadius: 0,
      fill: false,
      order: 10,
    });
    bandDatasets.push({
      label: `${ds.label} -1σ`,
      data: lower,
      borderColor: 'transparent',
      backgroundColor: fill,
      pointRadius: 0,
      fill: `-${bandDatasets.length - upperIdx}`,  // fills between this and the upper ds
      order: 10,
    });
  }

  const lineDatasets = datasets.map((ds, i) => {
    const color = ds.color || DEFAULT_COLORS[i % DEFAULT_COLORS.length];
    return {
      label: ds.label,
      data: ds.data,
      borderColor: color,
      backgroundColor: color.replace('rgb', 'rgba').replace(')', ', 0.15)'),
      fill: false,
      pointRadius: 2,
      tension: 0.25,
      borderWidth: 2,
      order: 1,
    };
  });

  const data = {
    labels,
    datasets: [...bandDatasets, ...lineDatasets],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: title ? { display: true, text: title, font: { size: 14 } } : { display: false },
      legend: {
        display: true, position: 'top',
        // Hide the synthetic ±1σ band datasets from the legend.
        labels: { filter: (item) => !/\s[+\-]1σ$/.test(item.text) },
      },
    },
    scales: {
      x: {
        title: xAxisLabel ? { display: true, text: xAxisLabel } : { display: false },
      },
      y: {
        title: yAxisLabel ? { display: true, text: yAxisLabel } : { display: false },
        beginAtZero: true,
      },
    },
  };

  return (
    <div style={{ height, padding: '8px' }}>
      <Line data={data} options={options} />
      {campaignStartIndex != null && (
        <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'center', marginTop: 4 }}>
          ▲ vertical dashed line at month {labels[campaignStartIndex]} = campaign start
        </div>
      )}
    </div>
  );
}
