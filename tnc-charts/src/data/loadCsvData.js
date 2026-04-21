/**
 * Load CSV files and compute cross-tabulation for heatmaps.
 *
 * Computes a 10x10 matrix where:
 *   rows = deciles of one product (e.g., comp)
 *   cols = deciles of another product (e.g., product A)
 *   cells = count of subjects or average value
 */
import Papa from 'papaparse';

/**
 * Load a CSV file via fetch + papaparse.
 * @param {string} url - path to CSV
 * @returns {Promise<Array<Object>>} parsed rows
 */
export async function loadCsv(url) {
  const response = await fetch(url);
  const text = await response.text();
  const result = Papa.parse(text, { header: true, skipEmptyLines: true });
  return result.data;
}

/**
 * Compute total volume per subject from Rx rows.
 * @param {Array<Object>} rows - parsed CSV rows
 * @param {string} npiCol - NPI column name
 * @param {string} valueCol - value column name
 * @returns {Map<string, number>} npi → total volume
 */
export function aggregateBySubject(rows, npiCol = 'NPI Number', valueCol = 'Unit Count') {
  const totals = new Map();
  for (const row of rows) {
    const npi = row[npiCol];
    const val = parseFloat(row[valueCol]) || 0;
    totals.set(npi, (totals.get(npi) || 0) + val);
  }
  return totals;
}

/**
 * Assign deciles (1=lowest, 10=highest) based on total volume.
 * @param {Map<string, number>} volumes - npi → total volume
 * @returns {Map<string, number>} npi → decile (1-10)
 */
export function assignDeciles(volumes) {
  const sorted = [...volumes.entries()].sort((a, b) => a[1] - b[1]);
  const n = sorted.length;
  const deciles = new Map();

  sorted.forEach(([npi], idx) => {
    const decile = Math.min(10, Math.floor((idx / n) * 10) + 1);
    deciles.set(npi, decile);
  });

  return deciles;
}

/**
 * Build a 10x10 cross-tabulation of deciles.
 *
 * @param {Map<string, number>} rowDeciles - npi → decile for rows
 * @param {Map<string, number>} colDeciles - npi → decile for columns
 * @param {string} metric - 'count' (subject count) or 'avgVolume' (average)
 * @param {Map<string, number>} colVolumes - npi → volume (for avgVolume metric)
 * @returns {{ data: number[][], rowLabels: string[], colLabels: string[] }}
 */
export function buildCrossTab(rowDeciles, colDeciles, metric = 'count', colVolumes = null) {
  // Initialize 10x10 grid (row=row decile, col=col decile)
  const grid = Array.from({ length: 10 }, () => Array(10).fill(0));
  const counts = Array.from({ length: 10 }, () => Array(10).fill(0));

  // Only include subjects present in both
  const common = [...rowDeciles.keys()].filter((npi) => colDeciles.has(npi));

  for (const npi of common) {
    const rd = rowDeciles.get(npi) - 1; // 0-indexed
    const cd = colDeciles.get(npi) - 1;
    if (rd >= 0 && rd < 10 && cd >= 0 && cd < 10) {
      counts[rd][cd] += 1;
      if (metric === 'avgVolume' && colVolumes) {
        grid[rd][cd] += colVolumes.get(npi) || 0;
      }
    }
  }

  // Convert sums to averages if needed
  const data = grid.map((row, ri) =>
    row.map((val, ci) => {
      if (metric === 'count') return counts[ri][ci];
      if (metric === 'avgVolume' && counts[ri][ci] > 0) {
        return val / counts[ri][ci];
      }
      return 0;
    }),
  );

  const labels = ['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10'];

  return {
    data,
    rowLabels: labels,
    colLabels: labels,
  };
}
