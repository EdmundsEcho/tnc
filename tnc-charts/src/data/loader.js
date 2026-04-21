/**
 * Load all the dashboard data from public/data/ and expose a uniform shape.
 */
import Papa from 'papaparse';

const BASE = '/data';

async function loadCsv(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`fetch ${url} ${r.status}`);
  const text = await r.text();
  const parsed = Papa.parse(text, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: true,
  });
  return parsed.data;
}

async function loadJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`fetch ${url} ${r.status}`);
  return r.json();
}

export async function loadAll() {
  const [summary, config, cfgParsed, ancova, subjects, windows, matches, timeseries, targetList] =
    await Promise.all([
      loadJson(`${BASE}/summary.json`),
      loadJson(`${BASE}/config.json`),
      loadJson(`${BASE}/cfg-parsed.json`),
      loadJson(`${BASE}/ancova.json`),
      loadCsv(`${BASE}/subjects.csv`),
      loadCsv(`${BASE}/windows.csv`),
      loadCsv(`${BASE}/matches.csv`),
      loadCsv(`${BASE}/timeseries.csv`),
      loadCsv(`${BASE}/target_list.csv`),
    ]);
  // Hoist decile_grouping to the top level for convenience; views shouldn't
  // need to know the cfg-parsed.json shape.
  const decileGrouping = cfgParsed?.tnc_analysis?.decile_grouping || { default: { groups: [] }, overrides: [] };
  return {
    summary, config, cfgParsed, decileGrouping,
    ancova, subjects, windows, matches, timeseries, targetList,
  };
}

/** Utility: group rows by a key, returning a Map<key, rows[]> */
export function groupBy(rows, keyFn) {
  const m = new Map();
  for (const r of rows) {
    const k = keyFn(r);
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(r);
  }
  return m;
}

/** Utility: count rows by a key */
export function countBy(rows, keyFn) {
  const m = new Map();
  for (const r of rows) {
    const k = keyFn(r);
    m.set(k, (m.get(k) || 0) + 1);
  }
  return m;
}

/** Sort [label, count] pairs by count desc */
export function sortedCounts(map) {
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}
