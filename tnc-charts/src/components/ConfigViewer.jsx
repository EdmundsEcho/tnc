/**
 * ConfigViewer — structured, human-readable presentation of the three
 * configuration files (etl-schema, campaign-cfg, tnc-analysis-cfg).
 *
 * Loads the pre-parsed JSON from /data/cfg-parsed.json (emitted by the Rust
 * CLI) so no client-side TOML parsing is needed. A "Raw" tab renders the
 * original TOML file with syntax highlighting.
 */
import React, { useEffect, useState } from 'react';
import FileViewer from './FileViewer';

export default function ConfigViewer({ section, tomlUrl, title, onClose }) {
  const [cfg, setCfg] = useState(null);
  const [error, setError] = useState(null);
  const [mode, setMode] = useState('pretty');

  useEffect(() => {
    fetch('data/cfg-parsed.json')
      .then(r => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then(setCfg)
      .catch(e => setError(e.message));
  }, []);

  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose?.(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  if (mode === 'raw') {
    return (
      <FileViewer
        url={tomlUrl}
        title={title}
        onClose={onClose}
        headerExtras={
          <ModeTabs mode={mode} onChange={setMode} />
        }
      />
    );
  }

  const sectionData = cfg?.[section];

  return (
    <div style={styles.backdrop} onClick={onClose}>
      <div style={styles.modal} onClick={e => e.stopPropagation()}>
        <header style={styles.header}>
          <div>
            <div style={styles.title}>{title}</div>
            <div style={styles.subtitle}>{tomlUrl}</div>
          </div>
          <div style={styles.actions}>
            <ModeTabs mode={mode} onChange={setMode} />
            <a href={tomlUrl} download style={styles.downloadBtn}>Download</a>
            <button onClick={onClose} style={styles.closeBtn} aria-label="Close">×</button>
          </div>
        </header>
        <div style={styles.body}>
          {error && <div style={styles.error}>Failed to load: <code>{error}</code></div>}
          {!error && !cfg && <div style={styles.loading}>Loading…</div>}
          {!error && cfg && !sectionData && (
            <div style={styles.error}>No section “{section}” in cfg-parsed.json.</div>
          )}
          {!error && sectionData && (
            <div style={{ padding: 20 }}>
              {section === 'etl_schema'   && <EtlSchemaPretty   data={sectionData} />}
              {section === 'campaign'     && <CampaignPretty    data={sectionData} />}
              {section === 'tnc_analysis' && <TncAnalysisPretty data={sectionData} />}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function ModeTabs({ mode, onChange }) {
  const tab = (id, label) => (
    <button
      onClick={() => onChange(id)}
      style={{
        ...styles.tabBtn,
        ...(mode === id ? styles.tabBtnActive : {}),
      }}
    >
      {label}
    </button>
  );
  return (
    <div style={styles.tabs}>
      {tab('pretty', 'Pretty')}
      {tab('raw',    'Raw')}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// etl-schema.toml  →  schema (subject/time/qualities/measurements)
//                     + gen (subject_count, time_range, distributions)
// ─────────────────────────────────────────────────────────────
function EtlSchemaPretty({ data }) {
  const { schema, gen } = data;
  return (
    <>
      <Section title="Schema" subtitle="Structural definition of the subject universe">
        <KVGrid
          rows={[
            ['Subject key', <code>{schema.subject}</code>],
            ['Time key',    <code>{schema.time}</code>],
            ['Qualities',   schema.qualities.map(q => q.name).join(', ')],
            ['Measurements', schema.measurements.map(m =>
              `${m.name} (${m.kind}${m.components?.length ? ', by ' + m.components.join(',') : ''})`
            ).join('  ·  ')],
          ]}
        />
      </Section>

      <Section title="Generation" subtitle="Synthetic data parameters (not needed for real-data runs)">
        <KVGrid
          rows={[
            ['Subject count', gen.subject_count.toLocaleString()],
            ['Time range',    `${gen.time_range.first_month} .. ${gen.time_range.last_month}  (${gen.time_range.last_month - gen.time_range.first_month + 1} months)`],
            ['Seed',          gen.seed],
            ['Volume scale',  distLabel(gen.subject_volume_scale?.distribution)],
            ['Noise (cv)',    gen.noise?.cv],
          ]}
        />

        <h4 style={styles.subheader}>Quality distributions</h4>
        <div style={styles.cards}>
          {Object.entries(gen.qualities || {}).map(([name, q]) => (
            <QualityCard key={name} name={name} q={q} />
          ))}
        </div>

        <h4 style={styles.subheader}>Measurement distributions</h4>
        <div style={styles.cards}>
          {Object.entries(gen.measurements || {}).map(([name, m]) => (
            <MeasurementCard key={name} name={name} m={m} />
          ))}
        </div>
      </Section>
    </>
  );
}

function QualityCard({ name, q }) {
  const dist = q.distribution;
  if (q.derived_from) {
    return (
      <div style={styles.card}>
        <div style={styles.cardTitle}>{name}</div>
        <div style={styles.cardMeta}>derived from <code>{q.derived_from}</code></div>
        <Mapping map={q.mapping} />
      </div>
    );
  }
  if (dist?.categorical) {
    return (
      <div style={styles.card}>
        <div style={styles.cardTitle}>{name}</div>
        <div style={styles.cardMeta}>categorical</div>
        <CategoricalTable values={dist.categorical} />
      </div>
    );
  }
  return (
    <div style={styles.card}>
      <div style={styles.cardTitle}>{name}</div>
      <div style={styles.cardMeta}>{distLabel(dist)}</div>
    </div>
  );
}

function MeasurementCard({ name, m }) {
  const dist = m.distribution;
  return (
    <div style={styles.card}>
      <div style={styles.cardTitle}>{name}</div>
      <div style={styles.cardMeta}>{distLabel(dist)}</div>
      {m.components && Object.entries(m.components).map(([comp, c]) => (
        <div key={comp} style={{ marginTop: 10 }}>
          <div style={styles.cardSub}>component: {comp}</div>
          {c.distribution?.categorical && <CategoricalTable values={c.distribution.categorical} />}
        </div>
      ))}
    </div>
  );
}

function CategoricalTable({ values }) {
  return (
    <table style={styles.innerTable}>
      <tbody>
        {values.map((v, i) => (
          <tr key={i}>
            <td style={styles.innerTd}><code>{v.value}</code></td>
            <td style={{ ...styles.innerTd, textAlign: 'right', color: '#6b7280' }}>
              {(v.weight * 100).toFixed(0)}%
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Mapping({ map }) {
  if (!map) return null;
  return (
    <table style={styles.innerTable}>
      <tbody>
        {Object.entries(map).map(([k, v]) => (
          <tr key={k}>
            <td style={styles.innerTd}><code>{k}</code></td>
            <td style={styles.innerTd}>→ <code>{v}</code></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function distLabel(d) {
  if (!d) return '—';
  if (d.poisson) return `Poisson(λ=${d.poisson.lambda})`;
  if (d.negative_binomial) return `NegBin(μ=${d.negative_binomial.mean}, φ=${d.negative_binomial.dispersion})`;
  if (d.log_normal || (d.distribution === 'log_normal')) {
    const p = d.log_normal || d;
    return `LogNormal(μ=${p.mu}, σ=${p.sigma})`;
  }
  if (d.categorical) return `Categorical (${d.categorical.length} levels)`;
  if (d.distribution) return d.distribution;
  return JSON.stringify(d);
}

// ─────────────────────────────────────────────────────────────
// campaign-cfg.toml  →  campaign mechanics + synthetic lift
// ─────────────────────────────────────────────────────────────
function CampaignPretty({ data }) {
  const { campaign, lift } = data;
  const tp = campaign.test_period_months;
  const rw = campaign.reach_window || {};
  return (
    <>
      <Section title="Study horizon & campaign" subtitle="Real calendar time — everything is relative to this.">
        <Timeline
          studyStart={campaign.study_start_date}
          totalSpan={campaign.total_span_months}
          rwStart={rw.start_date}
          rwDuration={rw.duration_months}
          testPeriod={tp}
          minPre={campaign.min_pre_months}
          maxPost={campaign.max_post_months}
        />
        <KVGrid
          rows={[
            ['Study start date',    <code>{campaign.study_start_date}</code>],
            ['Total span',          `${campaign.total_span_months} months`],
            ['Min pre-period',      `${campaign.min_pre_months} months — every reached subject has ≥ this much before their reach date`],
            ['Max post-period',     `${campaign.max_post_months} months — observable post-reach`],
            ['Test / lag period',   `${tp} month(s) — excluded from both pre and post analysis windows`],
            ['Reach window',        <code>{rw.start_date}</code>],
            ['Reach duration',      `${rw.duration_months} month(s) — subject reach dates fall inside this window`],
            ['Target-list fraction',        `${(campaign.target_list_fraction * 100).toFixed(0)}% of the universe (listed in target_list.csv)`],
            ['Reached fraction of targets', `${(campaign.reached_fraction_of_targets * 100).toFixed(0)}% of the target list actually gets a campaign_reach_date`],
          ]}
        />
      </Section>

      <Section title="In-person visits (called_on)"
               subtitle="Probability a subject receives an in-person rep visit — correlated with reach status">
        <KVGrid
          rows={[
            ['p(called_on | reached)',     `${(campaign.called_on.p_when_reached * 100).toFixed(0)}%`],
            ['p(called_on | not reached)', `${(campaign.called_on.p_when_not_reached * 100).toFixed(0)}%`],
          ]}
        />
      </Section>

      <Section title="Lift (synthetic)"
               subtitle="Known-truth signal injected so the pipeline can be closed-loop validated. Ignored for real-data runs.">
        <KVGrid
          rows={[
            ['Applies to',  <code>{lift.applies_to}</code>],
            ['Ramp',        `${lift.ramp_months} month(s) — linear from 0% up to max`],
            ['Max lift',    `+${(lift.max_pct * 100).toFixed(0)}%`],
            ['After ramp',  lift.plateau_after_ramp ? 'Plateau (holds at max)' : 'Decays'],
          ]}
        />
      </Section>
    </>
  );
}

function Timeline({ studyStart, totalSpan, rwStart, rwDuration, testPeriod, minPre, maxPost }) {
  // Compact visual using real calendar labels. Showing the overall horizon
  // and the reach window inside it; per-subject PRE/TEST/POST are anchored
  // on each subject's campaign_reach_date, not drawn here.
  const band = (bg, border, label, detail, flex = 1) => (
    <div style={{
      background: bg, border: `1px solid ${border}`,
      padding: '10px 12px', borderRadius: 4, flex, minWidth: 90,
    }}>
      <div style={{ fontWeight: 600, fontSize: 12 }}>{label}</div>
      <div style={{ fontSize: 11, color: '#6b7280', marginTop: 2 }}>{detail}</div>
    </div>
  );
  const studyEnd = addMonths(studyStart, totalSpan - 1);
  const rwEnd = addMonths(rwStart, rwDuration - 1);
  return (
    <div style={{ margin: '8px 0 14px 0' }}>
      <div style={{ display: 'flex', gap: 6 }}>
        {band('#f3f4f6', '#e5e7eb',
          'Study horizon',
          `${ym(studyStart)} – ${ym(studyEnd)}  ·  ${totalSpan} months`,
          3)}
      </div>
      <div style={{ display: 'flex', gap: 6, marginTop: 6 }}>
        {band('#eff6ff', '#bfdbfe',
          'Min pre per subject',
          `≥ ${minPre} months before reach`)}
        {band('#fef3c7', '#fde68a',
          'Reach window',
          `${ym(rwStart)} – ${ym(rwEnd)}  ·  ${rwDuration} month(s)`)}
        {band('#fff7ed', '#fed7aa',
          'Test / lag',
          `${testPeriod} month(s) post-reach (excluded)`)}
        {band('#ecfdf5', '#a7f3d0',
          'Max post per subject',
          `≤ ${maxPost} months observable`)}
      </div>
    </div>
  );
}

function ym(dateStr) {
  if (!dateStr) return '—';
  const [y, m] = dateStr.split('-');
  return `${y}-${m}`;
}

function addMonths(dateStr, months) {
  if (!dateStr) return '';
  const [y, m] = dateStr.split('-').map(Number);
  const total = y * 12 + (m - 1) + months;
  const ny = Math.floor(total / 12);
  const nm = (total % 12) + 1;
  return `${String(ny).padStart(4, '0')}-${String(nm).padStart(2, '0')}`;
}

// ─────────────────────────────────────────────────────────────
// tnc-analysis-cfg.toml  →  derived fields, eligibility, propensity,
//                            matching, ANCOVA
// ─────────────────────────────────────────────────────────────
function TncAnalysisPretty({ data }) {
  const {
    derived_fields, eligibility, decile_grouping, universe,
    propensity, matching, ancova,
    input_validation, match_validation,
  } = data;

  // Group derived fields by anchor
  const byAnchor = {};
  Object.entries(derived_fields).forEach(([name, def]) => {
    (byAnchor[def.anchor] ||= []).push({ name, months: def.months });
  });
  const anchorOrder = ['pre', 'test_period', 'post', 'first', 'last'];
  const postFlag = (a) => a === 'post' || a === 'test_period';

  return (
    <>
      <Section title="Derived fields"
               subtitle="Windowed aggregates — each (measurement × window) pair materializes a per-subject scalar.">
        {anchorOrder.filter(a => byAnchor[a]).map(a => (
          <div key={a} style={{ marginBottom: 12 }}>
            <div style={styles.anchorLabel}>
              <code>{a}</code>
              {postFlag(a) && (
                <span style={styles.warnBadge}>
                  OUTCOME-ONLY — forbidden in matching / propensity
                </span>
              )}
            </div>
            <div style={styles.chipRow}>
              {byAnchor[a].map(w => (
                <span key={w.name} style={styles.chip}>
                  {w.name} <span style={{ color: '#6b7280' }}>· {w.months}mo</span>
                </span>
              ))}
            </div>
          </div>
        ))}
      </Section>

      <Section title="Universe"
               subtitle="Signature filter: a subject joins iff its (qualities × merged writing-profile deciles) signature is observed in the reached cohort.">
        <h4 style={styles.subheader}>Signature qualities (must match exactly)</h4>
        <div style={styles.chipRow}>
          {(universe?.signature_qualities || []).map(q =>
            <span key={q} style={styles.chipPurple}>{q}</span>
          )}
        </div>
        <h4 style={styles.subheader}>Writing profiles (merged decile must match)</h4>
        <div style={styles.chipRow}>
          {(universe?.signature_writing_profiles || []).map((wp, i) =>
            <span key={i} style={styles.chipBlue}>
              {wp.measurement}_{wp.window}
            </span>
          )}
        </div>
      </Section>

      <Section title="Eligibility"
               subtitle="Secondary volume filter applied on top of the universe (test OR control)">
        <KVGrid
          rows={[
            ['Max brx_L12M', `${eligibility.brx_L12M_max}   (new-drug launch uses 0; relaxed here for MVP)`],
            ['Min hrx_L12M', `${eligibility.hrx_L12M_min}   (must have some headroom prescribing)`],
          ]}
        />
      </Section>

      <Section title="Decile grouping"
               subtitle="Collapse arbitrary decile subsets into one bucket. Applied wherever deciles are compared (universe signatures, volume gates).">
        <KVGrid
          rows={[
            ['Default groups',
              (decile_grouping?.default?.groups || []).length
                ? (decile_grouping.default.groups || []).map((g, i) =>
                    <code key={i} style={{ marginRight: 8 }}>
                      [{g.map(d => `D${d}`).join(', ')}]
                    </code>)
                : <span style={{ color: '#6b7280' }}>none (all singletons)</span>],
          ]}
        />
        {(decile_grouping?.overrides || []).length > 0 && (
          <>
            <h4 style={styles.subheader}>Per-source overrides</h4>
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>Measurement</th>
                  <th style={styles.th}>Window</th>
                  <th style={styles.th}>Merged groups</th>
                </tr>
              </thead>
              <tbody>
                {decile_grouping.overrides.map((o, i) => (
                  <tr key={i}>
                    <td style={styles.td}><code>{o.source.measurement}</code></td>
                    <td style={styles.td}><code>{o.source.window}</code></td>
                    <td style={styles.td}>
                      {(o.groups || []).map((g, j) =>
                        <code key={j} style={{ marginRight: 8 }}>
                          [{g.map(d => `D${d}`).join(', ')}]
                        </code>)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </Section>

      <Section title="Propensity score"
               subtitle="Logistic regression — its decile is a Stage-1 filter for matching">
        <KVGrid
          rows={[
            ['Target',     <code>{propensity.target}</code>],
            ['Binning',    `${propensity.bin_count} ${propensity.binning} bins`],
          ]}
        />
        <h4 style={styles.subheader}>Predictor qualities (one-hot encoded)</h4>
        <div style={styles.chipRow}>
          {propensity.predictor_qualities.map(q =>
            <span key={q} style={styles.chipPurple}>{q}</span>
          )}
        </div>
        <h4 style={styles.subheader}>Predictor derived fields (pre-period only)</h4>
        <div style={styles.chipRow}>
          {propensity.predictor_derived.map(d =>
            <span key={d} style={styles.chipBlue}>{d}</span>
          )}
        </div>
      </Section>

      <Section title="Matching"
               subtitle="Two-stage: categorical gates (+ propensity decile) → volume gates → Rx-decile scoring">
        <h4 style={styles.subheader}>Stage 1 — micro-pool filter</h4>
        <KVGrid
          rows={[
            ['Categorical gates', matching.categorical_gates.map(c => <code key={c} style={{ marginRight: 6 }}>{c}</code>)],
            ['Propensity-decile match',
              matching.propensity_match
                ? <span style={styles.okBadge}>enabled — test pairs with a control in the same propensity decile</span>
                : <span style={{ color: '#6b7280' }}>disabled</span>],
          ]}
        />
        <h4 style={styles.subheader}>Volume gates (decile-based)</h4>
        <p style={{ fontSize: 12, color: '#6b7280', margin: '0 0 8px 0' }}>
          Test and control must share the same <em>merged</em> decile bucket
          (after decile_grouping is applied). <code>within_deciles</code> = 0
          means exact match; 1 allows a one-bucket neighbor.
        </p>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>Measurement</th>
              <th style={styles.th}>Window</th>
              <th style={{ ...styles.th, textAlign: 'right' }}>within_deciles</th>
            </tr>
          </thead>
          <tbody>
            {matching.volume_gates.map((g, i) => (
              <tr key={i}>
                <td style={styles.td}><code>{g.source.measurement}</code></td>
                <td style={styles.td}><code>{g.source.window}</code></td>
                <td style={{ ...styles.td, textAlign: 'right' }}>
                  {g.within_deciles === 0
                    ? <span style={styles.okBadge}>0 (exact)</span>
                    : `±${g.within_deciles}`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <h4 style={styles.subheader}>Stage 2 — scoring (higher points = tighter match)</h4>
        <KVGrid
          rows={[
            ['Rx types scored', matching.scoring.rx_types.map(r => <code key={r} style={{ marginRight: 6 }}>{r}</code>)],
          ]}
        />
        <table style={{ ...styles.table, maxWidth: 400 }}>
          <thead>
            <tr>
              <th style={styles.th}>Window</th>
              <th style={{ ...styles.th, textAlign: 'right' }}>Points</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(matching.scoring.window_points).map(([w, p]) => (
              <tr key={w}>
                <td style={styles.td}><code>{w}</code></td>
                <td style={{ ...styles.td, textAlign: 'right' }}>{p}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      <Section title="ANCOVA"
               subtitle="Final causal model: outcome ~ treatment + baseline + covariates">
        <KVGrid
          rows={[
            ['Treatment',   <span><code>reached</code> (implicit — matched pair side: test = 1, control = 0)</span>],
            ['Baseline',    <code>{ancova.baseline.measurement}_{ancova.baseline.window}</code>],
            ['Outcomes',    ancova.outcomes.map(o =>
              <code key={o.measurement + o.window} style={{ marginRight: 6 }}>
                {o.measurement}_{o.window}
              </code>
            )],
            ['Quality covariates (one-hot)',
              ancova.covariate_qualities.map(c =>
                <code key={c} style={{ marginRight: 6 }}>{c}</code>)],
            ['Window covariates (pre-period only)',
              ancova.covariate_windows.map(w =>
                <code key={w.measurement + w.window} style={{ marginRight: 6 }}>
                  {w.measurement}_{w.window}
                </code>)],
          ]}
        />
      </Section>

      <Section title="Input validation"
               subtitle="Pre-flight gates — any failure halts the pipeline before matching.">
        <KVGrid
          rows={[
            ['Min reached',                `${input_validation?.min_reached ?? 0}  (campaign must have touched ≥ N in-universe subjects)`],
            ['Min eligible control pool',  `${input_validation?.min_eligible_control_pool ?? 0}`],
            ['Min test:control ratio',     `${((input_validation?.min_test_to_control_ratio ?? 0) * 100).toFixed(1)}%`],
          ]}
        />
      </Section>

      <Section title="Match validation"
               subtitle="Placebo test: pre-period DiD per measurement must fall within the configured tolerance or the pipeline halts. See Matching view for the measured values.">
        {(match_validation?.max_did_pre || []).length === 0
          ? <div style={{ color: '#6b7280', fontSize: 13 }}>disabled (empty list)</div>
          : (
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>Measurement</th>
                  <th style={styles.th}>Window</th>
                  <th style={{ ...styles.th, textAlign: 'right' }}>Max |DiD|</th>
                </tr>
              </thead>
              <tbody>
                {match_validation.max_did_pre.map((t, i) => (
                  <tr key={i}>
                    <td style={styles.td}><code>{t.measurement}</code></td>
                    <td style={styles.td}><code>{t.window}</code></td>
                    <td style={{ ...styles.td, textAlign: 'right', fontFamily: 'monospace' }}>{t.max.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
      </Section>

      {matching.autotune && (
        <Section title="Autotune (reserved)"
                 subtitle="When enabled, the pipeline would start with the strictest gates and progressively relax each step until target match rate is hit. Not yet implemented.">
          <KVGrid
            rows={[
              ['Enabled',           matching.autotune.enabled
                ? <span style={styles.warnBadge}>TRUE — but autotune is not wired up; using explicit gate values</span>
                : <span style={{ color: '#6b7280' }}>false</span>],
              ['Target match rate', `${((matching.autotune.target_match_rate || 0) * 100).toFixed(0)}%`],
              ['Relaxation steps',  `${(matching.autotune.relaxation_schedule || []).length} (in schedule order)`],
            ]}
          />
        </Section>
      )}
    </>
  );
}

// ─────────────────────────────────────────────────────────────
// Building blocks
// ─────────────────────────────────────────────────────────────
function Section({ title, subtitle, children }) {
  return (
    <section style={{ marginBottom: 28 }}>
      <h3 style={{ fontSize: 15, fontWeight: 600, margin: '0 0 2px 0', color: '#111827' }}>
        {title}
      </h3>
      {subtitle && (
        <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 10 }}>{subtitle}</div>
      )}
      {children}
    </section>
  );
}

function KVGrid({ rows }) {
  return (
    <table style={styles.kvTable}>
      <tbody>
        {rows.map(([k, v], i) => (
          <tr key={i}>
            <td style={styles.kvLabel}>{k}</td>
            <td style={styles.kvValue}>{v}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const styles = {
  backdrop: {
    position: 'fixed', inset: 0, background: 'rgba(17, 24, 39, 0.55)',
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    zIndex: 50, padding: 24,
  },
  modal: {
    background: '#fff', borderRadius: 8,
    width: 'min(1040px, 100%)', maxHeight: '88vh',
    display: 'flex', flexDirection: 'column',
    boxShadow: '0 10px 32px rgba(0,0,0,0.2)',
    overflow: 'hidden',
  },
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    padding: '12px 20px', borderBottom: '1px solid #e5e7eb',
    background: '#f9fafb',
  },
  title: { fontFamily: 'system-ui', fontSize: 14, fontWeight: 600, color: '#111827' },
  subtitle: { fontFamily: 'system-ui', fontSize: 11, color: '#6b7280', marginTop: 2 },
  actions: { display: 'flex', gap: 8, alignItems: 'center' },
  tabs: {
    display: 'inline-flex', border: '1px solid #d1d5db', borderRadius: 4,
    overflow: 'hidden', background: '#fff',
  },
  tabBtn: {
    padding: '4px 10px', border: 'none', background: '#fff',
    color: '#6b7280', cursor: 'pointer', fontSize: 12,
    fontFamily: 'inherit',
  },
  tabBtnActive: { background: '#eff6ff', color: '#1d4ed8', fontWeight: 600 },
  downloadBtn: {
    fontSize: 12, padding: '4px 10px',
    border: '1px solid #d1d5db', borderRadius: 4,
    color: '#374151', textDecoration: 'none', background: '#fff',
  },
  closeBtn: {
    fontSize: 20, lineHeight: 1, width: 28, height: 28,
    border: 'none', background: 'transparent', cursor: 'pointer', color: '#6b7280',
  },
  body: {
    overflow: 'auto',
    fontFamily: 'system-ui, -apple-system, sans-serif',
    color: '#111827',
  },
  loading: { padding: 24, color: '#6b7280', textAlign: 'center' },
  error: {
    padding: 20, margin: 16, borderRadius: 4,
    background: '#fef2f2', color: '#b91c1c', fontSize: 13,
  },
  subheader: {
    fontSize: 13, fontWeight: 600, color: '#374151',
    margin: '16px 0 6px 0',
  },
  kvTable: {
    borderCollapse: 'collapse', width: '100%',
    fontSize: 13,
  },
  kvLabel: {
    padding: '6px 12px 6px 0', color: '#6b7280', width: '34%',
    verticalAlign: 'top', borderBottom: '1px solid #f3f4f6',
  },
  kvValue: {
    padding: '6px 0', color: '#111827',
    borderBottom: '1px solid #f3f4f6',
  },
  table: {
    borderCollapse: 'collapse', width: '100%', fontSize: 13,
    marginTop: 4, marginBottom: 4,
  },
  th: {
    padding: '6px 10px', textAlign: 'left',
    borderBottom: '1px solid #e5e7eb', fontWeight: 600,
    fontSize: 12, color: '#374151', background: '#f9fafb',
  },
  td: {
    padding: '6px 10px', borderBottom: '1px solid #f3f4f6',
  },
  innerTable: {
    borderCollapse: 'collapse', width: '100%', fontSize: 12,
    marginTop: 6,
  },
  innerTd: {
    padding: '3px 6px', borderBottom: '1px solid #f3f4f6',
  },
  cards: {
    display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
    gap: 10,
  },
  card: {
    border: '1px solid #e5e7eb', borderRadius: 4, padding: 10,
    background: '#fafafa',
  },
  cardTitle: { fontSize: 13, fontWeight: 600, color: '#111827' },
  cardSub:   { fontSize: 11, color: '#6b7280', fontWeight: 500 },
  cardMeta:  { fontSize: 11, color: '#6b7280', marginTop: 2 },
  anchorLabel: {
    fontSize: 12, color: '#374151', fontWeight: 600,
    display: 'flex', alignItems: 'center', gap: 8,
    marginBottom: 4,
  },
  chipRow: { display: 'flex', gap: 6, flexWrap: 'wrap' },
  chip: {
    fontSize: 12, padding: '3px 8px',
    border: '1px solid #e5e7eb', borderRadius: 3,
    background: '#fff', fontFamily: 'ui-monospace, monospace',
  },
  chipPurple: {
    fontSize: 12, padding: '3px 8px',
    border: '1px solid #ddd6fe', borderRadius: 3,
    background: '#f5f3ff', color: '#6d28d9',
    fontFamily: 'ui-monospace, monospace',
  },
  chipBlue: {
    fontSize: 12, padding: '3px 8px',
    border: '1px solid #bfdbfe', borderRadius: 3,
    background: '#eff6ff', color: '#1d4ed8',
    fontFamily: 'ui-monospace, monospace',
  },
  okBadge: {
    fontSize: 11, padding: '2px 8px',
    background: '#ecfdf5', color: '#047857',
    border: '1px solid #a7f3d0', borderRadius: 3,
  },
  warnBadge: {
    fontSize: 11, padding: '2px 8px',
    background: '#fef2f2', color: '#b91c1c',
    border: '1px solid #fecaca', borderRadius: 3,
    marginLeft: 6,
  },
};
