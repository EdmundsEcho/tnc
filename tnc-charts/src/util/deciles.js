/**
 * Decile-grouping helpers — read the active decile_grouping config for a given
 * (measurement, window) and render a human-readable note like "D9+D10 merged".
 *
 * Source of truth is `cfg-parsed.json → tnc_analysis.decile_grouping`, with
 * overrides overriding the default per-source.
 */

export function groupsFor(decileGrouping, measurement, window) {
  if (!decileGrouping) return [];
  const match = (decileGrouping.overrides || []).find(
    o => o.source?.measurement === measurement && o.source?.window === window,
  );
  return match ? (match.groups || []) : (decileGrouping.default?.groups || []);
}

/** "brx L12M: D9+D10 merged" or "" if no merges are configured. */
export function mergeNote(decileGrouping, measurement, window) {
  const groups = groupsFor(decileGrouping, measurement, window);
  if (!groups.length) return '';
  const parts = groups
    .filter(g => g.length >= 2)
    .map(g => g.slice().sort((a, b) => a - b).map(d => `D${d}`).join('+'));
  if (!parts.length) return '';
  return `${measurement} ${window}: ${parts.join(', ')} merged`;
}
