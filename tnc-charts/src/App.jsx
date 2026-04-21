/**
 * TnC Analysis Dashboard
 *
 * Multi-view dashboard for exploring the TnC pipeline: universe → assignment
 * → matching → comparison → results. Data is produced by the tnc-data-gen
 * CLI and dropped into public/data.
 */
import React, { useEffect, useState } from 'react';
import { loadAll } from './data/loader';
import OverviewView from './views/OverviewView';
import UniverseView from './views/UniverseView';
import AssignmentView from './views/AssignmentView';
import MatchingView from './views/MatchingView';
import ComparisonView from './views/ComparisonView';
import ResultsView from './views/ResultsView';
import PropensityView from './views/PropensityView';
import FileViewer from './components/FileViewer';
import ConfigViewer from './components/ConfigViewer';

// Mapping: config TOMLs open the structured ConfigViewer on a given section.
const CONFIG_FILES = {
  'data/etl-schema.toml':       { section: 'etl_schema',   title: 'etl-schema.toml' },
  'data/campaign-cfg.toml':     { section: 'campaign',     title: 'campaign-cfg.toml' },
  'data/tnc-analysis-cfg.toml': { section: 'tnc_analysis', title: 'tnc-analysis-cfg.toml' },
};

const VIEWS = [
  { id: 'overview',   label: 'Overview',                component: OverviewView   },
  { id: 'universe',   label: 'Universe',                component: UniverseView   },
  { id: 'assignment', label: 'Assignment',              component: AssignmentView },
  { id: 'propensity', label: 'Propensity',              component: PropensityView },
  { id: 'matching',   label: 'Matching',                component: MatchingView   },
  { id: 'comparison', label: 'Test vs Control',         component: ComparisonView },
  { id: 'results',    label: 'ANCOVA',                  component: ResultsView    },
];

export default function App() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [view, setView] = useState('overview');
  const [fileView, setFileView] = useState(null); // { url, title } or null

  useEffect(() => {
    loadAll()
      .then(setData)
      .catch(e => setError(e.message));
  }, []);

  if (error) {
    return (
      <div style={styles.error}>
        <h2>Failed to load dashboard data</h2>
        <p><code>{error}</code></p>
        <p>Did you run <code>cargo run --release -- config.toml public/data</code> in <code>tnc-data-gen/</code>?</p>
      </div>
    );
  }

  if (!data) {
    return <div style={styles.loading}>Loading pipeline data…</div>;
  }

  const ViewComponent = VIEWS.find(v => v.id === view)?.component || OverviewView;

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <div style={styles.brand}>TnC Analysis Dashboard</div>
        <nav style={styles.nav}>
          {VIEWS.map(v => (
            <button
              key={v.id}
              onClick={() => setView(v.id)}
              style={{
                ...styles.navButton,
                ...(view === v.id ? styles.navButtonActive : {}),
              }}
            >
              {v.label}
            </button>
          ))}
        </nav>
      </header>

      <main style={styles.main}>
        <ViewComponent data={data} />
      </main>

      <Footer onView={(url, title) => setFileView({ url, title })} />

      {fileView && (
        CONFIG_FILES[fileView.url]
          ? <ConfigViewer
              tomlUrl={fileView.url}
              section={CONFIG_FILES[fileView.url].section}
              title={CONFIG_FILES[fileView.url].title}
              onClose={() => setFileView(null)}
            />
          : <FileViewer
              url={fileView.url}
              title={fileView.title}
              onClose={() => setFileView(null)}
            />
      )}
    </div>
  );
}

function Footer({ onView }) {
  // Viewable formats open in the modal; non-viewable (CSV) download directly.
  const viewable = /\.(toml|json)$/i;

  const item = (href, label) => {
    const isViewable = viewable.test(href);
    if (isViewable) {
      return (
        <button
          key={href}
          onClick={() => onView(href, label)}
          style={styles.viewBtn}
          title={`View ${label}`}
        >
          <span style={{ marginRight: 4, opacity: 0.7 }}>⎚</span>{label}
        </button>
      );
    }
    return (
      <a
        key={href}
        href={href}
        download
        style={styles.downloadLink}
        title={`Download ${label}`}
      >
        <span style={{ marginRight: 4, opacity: 0.7 }}>⬇</span>{label}
      </a>
    );
  };

  return (
    <footer style={styles.footer}>
      <div style={styles.footerSection}>
        <div style={styles.footerLabel}>Data</div>
        <div style={styles.footerLinks}>
          {item('data/subjects.csv',    'subjects.csv')}
          {item('data/windows.csv',     'windows.csv')}
          {item('data/matches.csv',     'matches.csv')}
          {item('data/timeseries.csv',  'timeseries.csv')}
          {item('data/summary.json',    'summary.json')}
          {item('data/ancova.json',     'ancova.json')}
          {item('data/config.json',     'config.json')}
        </div>
      </div>
      <div style={styles.footerSection}>
        <div style={styles.footerLabel}>Configurations</div>
        <div style={styles.footerLinks}>
          {item('data/etl-schema.toml',        'etl-schema.toml')}
          {item('data/campaign-cfg.toml',      'campaign-cfg.toml')}
          {item('data/tnc-analysis-cfg.toml',  'tnc-analysis-cfg.toml')}
        </div>
      </div>
    </footer>
  );
}

const styles = {
  app: {
    minHeight: '100vh',
    background: '#f3f4f6',
    fontFamily: 'system-ui, -apple-system, sans-serif',
    color: '#111827',
  },
  header: {
    background: '#fff',
    borderBottom: '1px solid #e5e7eb',
    padding: '16px 24px',
    display: 'flex',
    alignItems: 'center',
    gap: 32,
    position: 'sticky',
    top: 0,
    zIndex: 10,
  },
  brand: {
    fontSize: 16,
    fontWeight: 700,
    color: '#111827',
  },
  nav: {
    display: 'flex',
    gap: 4,
  },
  navButton: {
    padding: '6px 14px',
    border: 'none',
    background: 'transparent',
    color: '#6b7280',
    cursor: 'pointer',
    borderRadius: 4,
    fontSize: 14,
  },
  navButtonActive: {
    background: '#eff6ff',
    color: '#1d4ed8',
    fontWeight: 600,
  },
  main: {
    maxWidth: 1280,
    margin: '0 auto',
  },
  loading: {
    padding: 48,
    textAlign: 'center',
    color: '#6b7280',
    fontSize: 14,
  },
  error: {
    padding: 48,
    color: '#b91c1c',
    fontFamily: 'system-ui',
    maxWidth: 720,
    margin: '40px auto',
  },
  footer: {
    maxWidth: 1280,
    margin: '32px auto 16px auto',
    padding: '16px 24px',
    background: '#fff',
    border: '1px solid #e5e7eb',
    borderRadius: 6,
    display: 'flex',
    gap: 32,
    flexWrap: 'wrap',
  },
  footerSection: {
    display: 'flex',
    flexDirection: 'column',
    gap: 8,
  },
  footerLabel: {
    fontSize: 11,
    fontWeight: 600,
    textTransform: 'uppercase',
    color: '#6b7280',
    letterSpacing: 0.5,
  },
  footerLinks: {
    display: 'flex',
    gap: 6,
    flexWrap: 'wrap',
  },
  viewBtn: {
    fontSize: 12,
    padding: '4px 8px',
    border: '1px solid #dbeafe',
    borderRadius: 4,
    background: '#eff6ff',
    color: '#1d4ed8',
    cursor: 'pointer',
    fontFamily: 'inherit',
  },
  downloadLink: {
    fontSize: 12,
    padding: '4px 8px',
    border: '1px solid #e5e7eb',
    borderRadius: 4,
    background: '#f9fafb',
    color: '#374151',
    textDecoration: 'none',
  },
};
